#!/usr/bin/env python3
"""Bounded replay with a separate consumer group; source group offsets are untouched.

Default is a dry run. Stop all source-group members before --execute. The same
source identity and target ledger are used, so prior effects are skipped.
"""
import argparse
from copy import deepcopy
import json
import os
import uuid
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient


def source_is_stopped(brokers, group):
    admin = AdminClient({'bootstrap.servers':brokers})
    description = admin.describe_consumer_groups([group], request_timeout=8)[group].result(timeout=10)
    return not description.members


def replay(job, partition, first, through, *, brokers, execute=False, emit=print):
    if partition < 0 or first < 0 or through < first:
        raise ValueError('Replay needs a nonnegative partition and inclusive offset range')
    reader = Consumer({'bootstrap.servers':brokers,'group.id':'kafkaui-plan-'+uuid.uuid4().hex,'enable.auto.commit':False,'enable.auto.offset.store':False})
    try:
        low,high=reader.get_watermark_offsets(TopicPartition(job['topic_name'],partition),timeout=10)
    finally:reader.close()
    if first < low or through >= high:
        raise ValueError('Selected offsets are outside retained source data; nothing was replayed')
    plan=dict(event='replay_plan',cluster=job['cluster_id'],topic=job['topic_name'],partition=partition,
              from_offset=first,through_offset=through,source_group=job['consumer_group'],source_offsets_modified=False,
              maximum_source_records=through-first+1,execute=execute)
    emit(plan)
    if not execute:
        return plan
    if not source_is_stopped(brokers,job['consumer_group']):
        raise ValueError('Stop every source-group member before replay; this tool does not stop or reset production consumers')
    from main import run_worker
    replay_job=deepcopy(job)
    replay_job['consumer_group']='kafkaui-replay-'+uuid.uuid4().hex
    emit(dict(event='replay_started',replay_group=replay_job['consumer_group']))
    complete=run_worker(replay_job,brokers=brokers,emit=emit,assignment=[TopicPartition(job['topic_name'],partition,first)],
               stop_offsets={partition:through+1},policy='pause')
    if not complete:
        raise RuntimeError('Replay stopped before the requested range completed; inspect errors and rerun safely')
    emit(dict(event='replay_completed',replay_group=replay_job['consumer_group'],through_offset=through))
    return plan


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--partition',type=int,required=True)
    parser.add_argument('--from-offset',type=int,required=True)
    parser.add_argument('--through-offset',type=int,required=True)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args()
    import config
    job=config.get_job(os.environ['JOB_ID'])
    replay(job,args.partition,args.from_offset,args.through_offset,brokers=os.environ['KAFKA_BROKERS'],
           execute=args.execute,emit=lambda event:print(json.dumps(event),flush=True))

if __name__=='__main__':main()
