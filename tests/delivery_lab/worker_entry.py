"""Subprocess entry: production loop plus test-only crash/delay hooks."""
import json
import os
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'worker'))
import credentials
import main
from confluent_kafka import TopicPartition

config = json.loads(Path(sys.argv[1]).read_text())
credentials.CREDENTIAL_FILE = Path(config['credentials_file'])
credentials.CA_FILE = config['ca_file']
os.environ['DESTINATION_PROFILE_JSON'] = json.dumps(config['profile'])
os.environ['DELIVERY_MAX_ATTEMPTS'] = '3'
os.environ['DELIVERY_RETRY_BASE_SECONDS'] = '.1'
os.environ['DELIVERY_RETRY_CAP_SECONDS'] = '.2'
os.environ['DELIVERY_DLQ_TIMEOUT_SECONDS'] = '1'
for sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(sig, main.handle_signal)

log = Path(config['events_file']).open('a', buffering=1)
def emit(event):
    log.write(json.dumps(dict(event, monotonic=time.monotonic(), wall_time_ns=time.time_ns()))+'\n')

def crash(stage, message):
    if config.get('failpoint') != stage:
        return
    marker = Path(config['marker'])
    if not marker.exists():
        marker.write_text(stage)
        emit({'event':'injected_crash','stage':stage,'offset':message.offset() if hasattr(message,'offset') and callable(message.offset) else message.offset})
        log.flush()
        os._exit(86)

def before(message):
    crash('before_write', message)

def after(identity):
    crash('after_write_before_commit', identity)
    if config.get('write_delay_seconds'):
        time.sleep(config['write_delay_seconds'])

try:
    if config.get('replay'):
        from replay import replay
        window=config['replay']
        replay(config['job'],window['partition'],window['start'],window['end_exclusive']-1,
               brokers=config['brokers'],execute=True,emit=emit)
    else:
        main.run_worker(config['job'], brokers=config['brokers'], emit=emit, before_process=before, after_write=after,
                        policy=config.get('policy','pause'), dlq_topic=config.get('dlq_topic'))

except Exception as error:
    emit({'event':'worker_error','error_type':type(error).__name__})
    raise
finally:
    log.close()
