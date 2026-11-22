#!/usr/bin/env python3
"""Real Kafka/PostgreSQL delivery evidence. Never accepts external service URLs."""
import argparse
import fcntl
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import threading
import time
import uuid

import psutil
import psycopg2
from confluent_kafka import Consumer, Producer, TopicPartition, libversion
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / 'work' / 'delivery-lab'
ENV_FILE = STATE / 'lab.env'
COMPOSE = ['docker', 'compose', '-p', 'kafkaui-delivery-lab', '--env-file', str(ENV_FILE), '-f', str(ROOT/'tests/delivery_lab/compose.yaml')]
sys.path.insert(0, str(ROOT/'worker'))
import credentials
from destinations import get_destination


class Lab:
    def __init__(self):
        self.id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6]
        self.output = STATE/'results'/self.id
        self.output.mkdir(parents=True)
        self.brokers = '127.0.0.1:'+os.environ.get('LAB_KAFKA_PORT','19092')
        password = dict(line.split('=',1) for line in ENV_FILE.read_text().splitlines() if '=' in line)['LAB_DB_PASSWORD']
        self.db = dict(host='127.0.0.1',port=int(os.environ.get('LAB_POSTGRES_PORT','55432')),dbname='kafkaui_delivery_lab',user='lab',password=password,
                       sslmode='verify-full',sslrootcert=str(STATE/'certs/server.crt'),connect_timeout=2)
        self.credential_file=STATE/'credentials.json'
        self.credential_file.write_text(json.dumps(dict(host=self.db['host'],port=self.db['port'],database=self.db['dbname'],username=self.db['user'],password=password)))
        self.credential_file.chmod(0o600)
        self.profile=dict(id='lab',name='Isolated delivery lab',destinationType='postgresql',provider='kubernetes_secret',secretName='lab-only',caSecretName='lab-only',serviceAccount='lab-only')
        credentials.CREDENTIAL_FILE=self.credential_file
        credentials.CA_FILE=self.db['sslrootcert']
        os.environ['DESTINATION_PROFILE_JSON']=json.dumps(self.profile)
        self.admin=AdminClient({'bootstrap.servers':self.brokers,'socket.timeout.ms':3000})
        self.workers=[]
        self.evidence=[]
        self.context=self.hardware()
        sources=sorted([*ROOT.glob('worker/**/*.py'),*ROOT.glob('tests/delivery_lab/*.py'),ROOT/'tests/delivery_lab/requirements.txt',ROOT/'tests/delivery_lab/compose.yaml',ROOT/'scripts/delivery_lab.sh'])
        digest=hashlib.sha256()
        for path in sources:
            digest.update(str(path.relative_to(ROOT)).encode()+b'\0'+path.read_bytes())
        self.context['tested_source_sha256']=digest.hexdigest()
        self.context['python_environment']={distribution.metadata['Name']:distribution.version for distribution in importlib.metadata.distributions()}

    def hardware(self):
        model=platform.processor()
        if Path('/proc/cpuinfo').exists():
            model=next((line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')),model)
        info={'os':platform.platform(),'architecture':platform.machine(),'cpu_model':model,'visible_vcpus':psutil.cpu_count(),
              'memory_bytes':psutil.virtual_memory().total,'python':platform.python_version(),
              'packages':{name:importlib.metadata.version(name) for name in ('confluent-kafka','psycopg2-binary','psutil')},
              'librdkafka':libversion()[0],'limits':{'kafka_cpus':1,'kafka_memory_mib':512,'postgres_cpus':1,'postgres_memory_mib':256,'worker':'host process; no additional CPU/memory limit'},
              'batching':{'destination_records_per_transaction':1,'offset_commit_records':1,'auto_commit':False,'auto_offset_store':False,'producer_batch_num_messages':1,'producer_linger_ms':0,'producer_acks':'all'},
              'notes':['single-broker RF=1/minISR=1 lab, not broker-loss durability evidence','TLS verified for PostgreSQL; loopback plaintext Kafka','structured JSON event logging enabled','management HTTP callbacks disabled in this lab']}
        warnings=json.loads(subprocess.check_output(['docker','info','--format','{{json .Warnings}}'],text=True)) or []
        info['docker_warnings']=warnings
        info['container_cgroup_limits_enforced']=not any('without cgroups' in warning for warning in warnings)
        info['notes'].append('Container resources are host-visible process-tree CPU and summed RSS; RSS counts shared pages more than once. Docker stats is not used because rootless/no-cgroup daemons may report shared host usage.')
        for name in ('cpu.max','memory.max'):
            path=Path('/sys/fs/cgroup')/name
            if path.exists():info['cgroup_'+name]=path.read_text().strip()
        return info

    def connection(self):
        return psycopg2.connect(**self.db)

    def wait_ready(self, timeout=90):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            try:
                self.admin.list_topics(timeout=2)
                with self.connection() as conn:
                    with conn.cursor() as cursor:cursor.execute('SELECT version()');self.context['postgres_version']=cursor.fetchone()[0]
                return
            except Exception:time.sleep(1)
        raise RuntimeError('Local Kafka/PostgreSQL did not become ready; inspect Compose logs')

    def sql(self, query, params=(), fetch=True):
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query,params)
                return cursor.fetchall() if fetch and cursor.description else []

    def setup(self):
        self.wait_ready()
        self.sql('''CREATE TABLE IF NOT EXISTS public.delivery_effects (id BIGSERIAL PRIMARY KEY,target_table TEXT,source_cluster TEXT,source_topic TEXT,source_partition INT,source_offset BIGINT,business_id BIGINT,applied_at TIMESTAMPTZ DEFAULT clock_timestamp());
        CREATE OR REPLACE FUNCTION public.lab_record_effect() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO public.delivery_effects(target_table,source_cluster,source_topic,source_partition,source_offset,business_id) VALUES(TG_TABLE_NAME,NEW._kafka_cluster,NEW._kafka_topic,NEW._kafka_partition,NEW._kafka_offset,NEW.id); RETURN NEW; END$$;''',fetch=False)
        self.context['docker_version']=subprocess.check_output(['docker','version','--format','{{.Server.Version}}'],text=True).strip()
        self.context['image_ids']={}
        for service in ('kafka','postgres'):
            cid=subprocess.check_output(COMPOSE+['ps','-q',service],text=True).strip()
            image_id=subprocess.check_output(['docker','inspect','--format','{{.Image}}',cid],text=True).strip()
            digests=json.loads(subprocess.check_output(['docker','image','inspect','--format','{{json .RepoDigests}}',image_id],text=True))
            self.context['image_ids'][service]={'config_id':image_id,'repository_digests':digests}

    def case(self,name,partitions=1):
        token=uuid.uuid4().hex[:10]
        topic=f'lab-{name}-{token}';table='lab_'+token
        for future in self.admin.create_topics([NewTopic(topic,num_partitions=partitions,replication_factor=1)]).values():future.result(timeout=30)
        job=dict(id=token,name=name,cluster_id='isolated-delivery-lab',topic_name=topic,consumer_group='lab-'+token,destination_type='postgresql',
                 destination_config={'connectionId':'lab','tableName':table,'schema':'public'},start_position='earliest',include_historical=True,
                 fields=[dict(name='id',field_type='long',field_source='value',is_primary_key=True,is_required=True),dict(name='payload',field_type='string',field_source='value',is_primary_key=False,is_required=True)])
        dest=get_destination('postgresql',job['destination_config'],job['fields']);dest.ensure_table();dest.close()
        self.sql(f'CREATE TRIGGER audit_effect AFTER INSERT OR UPDATE ON public."{table}" FOR EACH ROW EXECUTE FUNCTION public.lab_record_effect()',fetch=False)
        return dict(name=name,topic=topic,table=table,job=job,partitions=partitions,workers=[])

    def worker(self,case,**options):
        index=len(self.workers)
        config=dict(job=case['job'],brokers=self.brokers,profile=self.profile,credentials_file=str(self.credential_file),ca_file=self.db['sslrootcert'],
                    events_file=str(self.output/f'worker-{index}.jsonl'),marker=str(self.output/(case['table']+'-crash.marker')),**options)
        path=self.output/f'worker-{index}.json';path.write_text(json.dumps(config))
        output=(self.output/f'worker-{index}.log').open('w')
        process=subprocess.Popen([sys.executable,str(ROOT/'tests/delivery_lab/worker_entry.py'),str(path)],stdout=output,stderr=subprocess.STDOUT)
        worker=dict(process=process,events=Path(config['events_file']),output=output,samples=[],ps=psutil.Process(process.pid),case=case)
        worker['ps'].cpu_percent(None)
        self.workers.append(worker);case['workers'].append(worker)
        return worker

    def events(self,worker):
        if not worker['events'].exists():return []
        result=[]
        for line in worker['events'].read_text().splitlines():
            try:result.append(json.loads(line))
            except ValueError:pass
        return result

    def all_events(self,case):
        return [e for w in case['workers'] for e in self.events(w)]

    def stop(self,worker):
        process=worker['process']
        if process.poll() is None:
            process.terminate()
            try:process.wait(timeout=15)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        worker['output'].close()

    def sample(self,worker):
        try:
            process=worker['ps'];cpu=process.cpu_times()
            sample=dict(monotonic=time.monotonic(),cpu_percent_one_core=process.cpu_percent(None),rss_bytes=process.memory_info().rss,cpu_seconds=cpu.user+cpu.system)
            worker['samples'].append(sample)
        except psutil.Error:pass

    def wait(self,function,timeout=60,message='condition not reached'):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            result=function()
            if result:return result
            time.sleep(.1)
        logs=[str(self.output/f'worker-{i}.log') for i,w in enumerate(self.workers) if w['process'].poll() not in (None,0,86)]
        raise AssertionError(message+('; failed worker logs: '+', '.join(logs) if logs else ''))

    def produce(self,case,count,*,start=0,payload_bytes=512,rate=0,raw=None,headers=None):
        producer=Producer({'bootstrap.servers':self.brokers,'acks':'all','enable.idempotence':True,'linger.ms':0,'batch.num.messages':1,'compression.type':'none'})
        errors=[];lengths=[];began=time.monotonic()
        def delivered(error,message):
            if error:errors.append(str(error.code()))
        for n in range(count):
            if rate:
                delay=began+n/rate-time.monotonic()
                if delay>0:time.sleep(delay)
            body={'id':start+n,'payload':''}
            encoded=json.dumps(body,separators=(',',':')).encode()
            body['payload']='x'*max(0,payload_bytes-len(encoded))
            data=raw if raw is not None else json.dumps(body,separators=(',',':')).encode()
            lengths.append(len(data))
            producer.produce(case['topic'],partition=n%case['partitions'],key=str(start+n).encode(),value=data,
                             timestamp=time.time_ns()//1000000,headers=headers,on_delivery=delivered)
            producer.poll(0)
        pending=producer.flush(30)
        assert not pending and not errors, ('source production not acknowledged',errors)
        return lengths

    def offsets(self,case,group=None):
        consumer=Consumer({'bootstrap.servers':self.brokers,'group.id':group or case['job']['consumer_group'],'enable.auto.commit':False,'enable.auto.offset.store':False})
        try:
            parts=[TopicPartition(case['topic'],p) for p in range(case['partitions'])]
            commits=consumer.committed(parts,timeout=5)
            rows=[]
            for tp in commits:
                low,high=consumer.get_watermark_offsets(tp,timeout=3)
                rows.append(dict(partition=tp.partition,committed=tp.offset,low=low,high=high,lag=high-max(low,tp.offset if tp.offset>=0 else low),uncommitted=tp.offset<0))
            return rows
        finally:consumer.close()

    def counts(self,case):
        rows=self.sql(f'SELECT count(*),count(DISTINCT id) FROM public."{case["table"]}"')[0]
        effects=self.sql('SELECT count(*) FROM public.delivery_effects WHERE target_table=%s',(case['table'],))[0][0]
        ledger=self.sql('SELECT count(*) FROM public._kafkaui_delivery_ledger WHERE source_topic=%s',(case['topic'],))[0][0]
        return dict(rows=rows[0],unique_business_ids=rows[1],business_effects=effects,ledger_entries=ledger,duplicate_effects=effects-ledger)

    def finished(self,case,count,timeout=90):
        def ready():
            try:return self.counts(case)['ledger_entries']==count and sum(row['lag'] for row in self.offsets(case))==0
            except Exception:return False
        self.wait(ready,timeout=timeout,message=f'{case["name"]} did not drain')
        result=self.counts(case)
        assert result==dict(rows=count,unique_business_ids=count,business_effects=count,ledger_entries=count,duplicate_effects=0),result
        return result

    def paused(self,worker,timeout=45):
        return self.wait(lambda:next((e for e in self.events(worker) if e.get('event')=='record_paused'),None),timeout,'worker did not surface a paused record')

    def record(self,name,case,**values):
        entry=dict(scenario=name,topic=case['topic'],partitions=case['partitions'],consumer_group=case['job']['consumer_group'],**values)
        self.evidence.append(entry)
        print(json.dumps(entry),flush=True)

    def verify(self):
        for stage in ('before_write','after_write_before_commit'):
            case=self.case(stage);self.produce(case,1)
            worker=self.worker(case,failpoint=stage)
            self.wait(lambda:worker['process'].poll() is not None,timeout=35,message='crash failpoint not reached')
            assert worker['process'].returncode==86
            before=self.counts(case);offsets=self.offsets(case)
            assert all(row['committed']<=0 for row in offsets),offsets
            assert before['business_effects']==(1 if stage.startswith('after') else 0),before
            self.stop(worker);restarted=self.worker(case)
            final=self.finished(case,1);self.stop(restarted)
            self.record(stage,case,before_restart=before,offsets_before_restart=offsets,final=final,
                        replay_outcomes=[e['outcome'] for e in self.events(restarted) if e.get('event')=='record_processed'])
        case=self.case('invalid');self.produce(case,1,raw=b'{not-json')
        worker=self.worker(case);paused=self.paused(worker);offsets=self.offsets(case)
        assert paused['stage']=='validation' and all(row['committed']<=0 for row in offsets)
        assert self.counts(case)['business_effects']==0
        self.stop(worker);self.record('invalid_record',case,paused=paused,offsets=offsets,final=self.counts(case))
        case=self.case('dlq-unavailable');self.produce(case,1,raw=b'{not-json')
        worker=self.worker(case,policy='dead_letter',dlq_topic='missing-'+uuid.uuid4().hex)
        paused=self.paused(worker);offsets=self.offsets(case)
        assert paused['stage']=='dead_letter' and paused['attempts']==3 and all(row['committed']<=0 for row in offsets)
        self.stop(worker);self.record('dead_letter_unavailable',case,paused=paused,offsets=offsets,final=self.counts(case))
        case=self.case('dlq-ack');dlq='lab-dlq-'+uuid.uuid4().hex[:10]
        for f in self.admin.create_topics([NewTopic(dlq,num_partitions=1,replication_factor=1)]).values():f.result(timeout=30)
        raw=b'{bad-json';headers=[('same',b'one'),('same',b'two'),('null',None)]
        self.produce(case,1,raw=raw,headers=headers);worker=self.worker(case,policy='dead_letter',dlq_topic=dlq)
        self.wait(lambda:self.offsets(case)[0]['committed']==1,timeout=40,message='DLQ acknowledgment did not advance exactly one source offset')
        reader=Consumer({'bootstrap.servers':self.brokers,'group.id':'dlq-reader-'+uuid.uuid4().hex,'enable.auto.commit':False})
        reader.assign([TopicPartition(dlq,0,0)])
        message=self.wait(lambda:reader.poll(.5),timeout=15,message='no durable DLQ record found')
        assert not message.error();envelope=json.loads(message.value())
        import base64
        assert base64.b64decode(envelope['record']['value_base64'])==raw and len(envelope['record']['headers'])==3
        assert self.counts(case)['ledger_entries']==0
        reader.close();self.stop(worker);self.record('dead_letter_acknowledged',case,source_offset=1,dlq=dlq,event_id=envelope['event_id'],raw_payload_roundtrip=True)
        case=self.case('manual');self.produce(case,8);crashed=self.worker(case,failpoint='before_write')
        self.wait(lambda:crashed['process'].poll()==86,timeout=35);self.stop(crashed)
        from replay import source_is_stopped
        def source_stopped():
            try:return source_is_stopped(self.brokers,case['job']['consumer_group'])
            except TimeoutError:return False
        self.wait(source_stopped,timeout=30,message='source group did not stop before replay')
        replay=self.worker(case,replay={'partition':0,'start':0,'end_exclusive':4})
        # The child entry uses the production replay planner/safety check.
        self.wait(lambda:replay['process'].poll() is not None,timeout=40,message='manual replay did not stop at requested bound')
        assert replay['process'].returncode==0
        selected=self.sql(f'SELECT id FROM public."{case["table"]}" ORDER BY id')
        assert [r[0] for r in selected]==list(range(4)),selected
        assert self.offsets(case)[0]['committed']<=0,'replay changed the source consumer group'
        self.stop(replay);worker=self.worker(case);final=self.finished(case,8);self.stop(worker)
        self.record('manual_replay',case,recovered_selected_ids=list(range(4)),source_group_unchanged_during_replay=True,final=final)
        case=self.case('rebalance',partitions=3);self.produce(case,600)
        one=self.worker(case,write_delay_seconds=.02)
        self.wait(lambda:sum(e.get('event')=='record_processed' for e in self.events(one))>=10,timeout=30)
        two=self.worker(case,write_delay_seconds=.02)
        self.wait(lambda:any(e.get('event')=='assigned' and e.get('partitions') for e in self.events(two)),timeout=40)
        at_assignment=self.counts(case)['ledger_entries'];assert at_assignment<600,'rebalance happened after processing ended'
        final=self.finished(case,600);self.stop(one);self.stop(two)
        assert any(e.get('event')=='revoked' and e.get('partitions') for e in self.events(one))
        self.record('consumer_rebalance',case,records_at_second_assignment=at_assignment,final=final,
                    assignment_events=[e for e in self.all_events(case) if e.get('event') in ('assigned','revoked')])
        self.outage()

    def outage(self):
        case=self.case('outage');self.produce(case,5);worker=self.worker(case);self.finished(case,5)
        timeline=[]
        def mark(event,**values):timeline.append(dict(event=event,monotonic=time.monotonic(),**values))
        mark('processing',counts=self.counts(case))
        try:
            subprocess.run(COMPOSE+['stop','postgres'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            mark('destination_stopped');self.produce(case,10,start=5)
            paused=self.paused(worker);offsets=self.offsets(case)
            assert paused['stage']=='destination' and paused['attempts']==3
            assert offsets[0]['committed']==paused['offset'] and offsets[0]['lag']==10,offsets
            mark('paused',offsets=offsets,attempts=paused['attempts'])
        finally:
            subprocess.run(COMPOSE+['start','postgres'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        self.wait_ready();restored=time.monotonic();mark('destination_restored')
        assert worker['process'].poll() is None,'paused worker should be visibly alive'
        self.stop(worker);mark('operator_restarts_worker');worker=self.worker(case)
        final=self.finished(case,15);recovery=time.monotonic()-restored;mark('recovered',counts=final,lag=0)
        self.stop(worker)
        self.record('destination_unavailable_and_recovery',case,timeline=timeline,recovery_seconds=recovery,final=final)
        return self.evidence[-1]

    def container_samples(self):
        # A local Linux Docker daemon is required. No-cgroup rootless Docker stats
        # can report identical host-wide memory for every container.
        result=[]
        for service in ('kafka','postgres'):
            pid=int(subprocess.check_output(COMPOSE+['ps','-q',service],text=True).strip() and
                    subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}',f'kafkaui-delivery-lab-{service}-1'],text=True))
            root=psutil.Process(pid)
            processes=[root]+root.children(recursive=True)
            cpu_seconds=0;rss=0;identities=[]
            for process in processes:
                try:
                    cpu=process.cpu_times();cpu_seconds+=cpu.user+cpu.system
                    rss+=process.memory_info().rss
                    identities.append([process.pid,process.create_time()])
                except psutil.NoSuchProcess:pass
            now=time.monotonic()
            previous=getattr(self,'previous_container_samples',{}).get(service)
            cpu_percent=None
            if previous and now>previous['monotonic']:
                cpu_percent=max(0,cpu_seconds-previous['cpu_seconds'])/(now-previous['monotonic'])*100
            sample=dict(service=service,monotonic=now,cpu_seconds=cpu_seconds,
                        cpu_percent_one_core=cpu_percent,summed_rss_bytes=rss,processes=identities)
            if not hasattr(self,'previous_container_samples'):self.previous_container_samples={}
            self.previous_container_samples[service]=sample
            result.append(sample)
        return result

    def benchmark(self,duration=20,rate=200,payload_bytes=512):
        if not 1 <= duration <= 3600 or not 1 <= rate <= 10000 or not 64 <= payload_bytes <= 1048576:
            raise ValueError('Benchmark bounds: duration 1–3600s, rate 1–10000/s, payload 64B–1MiB')
        case=self.case('benchmark',partitions=3);worker=self.worker(case)
        self.wait(lambda:any(e.get('event')=='assigned' for e in self.events(worker)),timeout=30)
        count=int(duration*rate);lag=[];resources=[];began=time.monotonic();last_sample=0
        initial_cpu=worker['ps'].cpu_times();initial_cpu=initial_cpu.user+initial_cpu.system
        last_container_sample=0
        self.container_samples()  # establish CPU-time baselines
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(self.produce,case,count,payload_bytes=payload_bytes,rate=rate)
            while True:
                self.sample(worker)
                if time.monotonic()-last_sample>=1:
                    current=self.offsets(case);lag.append(dict(elapsed=time.monotonic()-began,partitions=current,total_lag=sum(r['lag'] for r in current)))
                    last_sample=time.monotonic()
                if time.monotonic()-last_container_sample>=3:
                    resources.extend(self.container_samples());last_container_sample=time.monotonic()
                processed=[e for e in self.events(worker) if e.get('event')=='record_processed']
                if future.done() and len(processed)>=count:break
                assert worker['process'].poll() is None,'benchmark worker stopped'
                assert time.monotonic()-began<duration+120,'benchmark did not drain'
                time.sleep(.1)
            lengths=future.result()
        elapsed=time.monotonic()-began;self.sample(worker)
        final=self.finished(case,count)
        events=self.events(worker);self.stop(worker)
        latency=[(e['committed_at_ns']/1000000-e['source_timestamp_ms']) for e in events if e.get('event')=='record_processed']
        latency.sort()
        def percentile(fraction):return latency[min(len(latency)-1,max(0,int((len(latency)-1)*fraction)))]
        errors=[e for e in events if e.get('event')=='record_error']
        unique_failures={(e['topic'],e['partition'],e['offset']) for e in errors}
        container_summary=[]
        for service in ('kafka','postgres'):
            samples=[r for r in resources if r['service']==service]
            container_summary.append(dict(service=service,peak_sampled_cpu_percent_one_core=max(r['cpu_percent_one_core'] for r in samples if r['cpu_percent_one_core'] is not None),
                                          mean_sampled_cpu_percent_one_core=statistics.mean(r['cpu_percent_one_core'] for r in samples if r['cpu_percent_one_core'] is not None),
                                          peak_sampled_summed_rss_mib=max(r['summed_rss_bytes'] for r in samples)/1048576,samples=len(samples)))
        result=dict(scenario='steady_state_benchmark',offered_records_per_second=rate,requested_duration_seconds=duration,source_records=count,
                    payload_bytes={'min':min(lengths),'max':max(lengths)},partitions=case['partitions'],workers=1,elapsed_until_committed_seconds=elapsed,
                    committed_records_per_second=count/elapsed,end_to_end_commit_latency_ms={'p50':percentile(.5),'p95':percentile(.95),'max':max(latency)},
                    peak_observed_consumer_lag=max(r['total_lag'] for r in lag),final_consumer_lag=sum(r['lag'] for r in self.offsets(case)),
                    records_with_error=len(unique_failures),record_error_rate=len(unique_failures)/count,error_attempts=len(errors),
                    worker_resources={'peak_sampled_cpu_percent_one_core':max(r['cpu_percent_one_core'] for r in worker['samples']),
                                      'mean_cpu_percent_one_core':(worker['samples'][-1]['cpu_seconds']-initial_cpu)/elapsed*100,
                                      'peak_sampled_rss_mib':max(r['rss_bytes'] for r in worker['samples'])/1048576},
                    containers=container_summary,final=final,measurement='offered-load test, not maximum sustainable throughput',topic=case['topic'])
        (self.output/'lag-samples.json').write_text(json.dumps(lag,indent=2)+'\n')
        (self.output/'container-resource-samples.json').write_text(json.dumps(resources,indent=2)+'\n')
        (self.output/'worker-resource-samples.json').write_text(json.dumps(worker['samples'],indent=2)+'\n')
        self.evidence.append(result);print(json.dumps(result),flush=True)
        return result

    def close(self):
        for worker in self.workers:self.stop(worker)

    def publish(self,status,error=None):
        report=dict(run_id=self.id,status=status,hardware_software=self.context,scenarios=self.evidence)
        if error:report['error']=error
        path=self.output/'report.json';path.write_text(json.dumps(report,indent=2)+'\n')
        print('Evidence: '+str(path),flush=True)
        return report


def main():
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['all','verify','benchmark','demo'])
    args=parser.parse_args()
    lock=(STATE/'lab.lock').open('a')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise RuntimeError('Another delivery lab run is active') from None
    lab=Lab()
    try:
        lab.setup()
        if args.mode in ('all','verify'):lab.verify()
        if args.mode in ('all','benchmark'):lab.benchmark(duration=int(os.environ.get('LAB_BENCHMARK_SECONDS','20')),rate=int(os.environ.get('LAB_RECORDS_PER_SECOND','200')),payload_bytes=int(os.environ.get('LAB_PAYLOAD_BYTES','512')))
        if args.mode=='demo':lab.outage()
        lab.publish('PASS')
    except Exception as error:
        lab.publish('FAIL',type(error).__name__+': '+str(error));raise
    finally:lab.close()

if __name__=='__main__':main()
