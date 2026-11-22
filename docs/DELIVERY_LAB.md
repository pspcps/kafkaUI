# Delivery integration lab and performance measurements

This lab runs the actual `worker/main.py` processing loop, PostgreSQL adapter, DLQ publisher and replay tool against real isolated Kafka and PostgreSQL services. Failpoints are injected Python callbacks from a subprocess harness; production workers have no failpoint environment flags. An AFTER INSERT/UPDATE audit trigger records business effects independently of final row count, so a repeated upsert would be observable.

## One command

Prerequisites: Linux, a **local** Docker daemon with Docker Compose v2, Python 3.12+ with venv support, OpenSSL, and enough disk for images/container layers. Remote Docker process namespaces are not supported by the resource sampler. Pull/install needs internet. Allow several minutes for first image pulls and the tests.

```bash
./scripts/delivery_lab.sh all
```

This installs pinned lab Python packages in `work/delivery-lab/venv`, generates local TLS material and a random lab-only password, starts pinned Kafka 3.9.1 and PostgreSQL 16-alpine images, runs policy/credential/callback unit tests, runs the integration scenarios, and benchmarks 4,000 seeded 512-byte JSON values at 200 records/second for 20 seconds. Keys are additional bytes. Everything uses loopback ports 19092/55432 and generated `lab-*` topics/tables. No customer/cloud endpoints are accepted. Change `LAB_KAFKA_PORT`/`LAB_POSTGRES_PORT` if needed; set `DOCKER_HOST` for your local daemon if required. Run only one lab instance at a time.

The harness prints each scenario and exits nonzero on failure. Reports, event logs and raw lag/resource samples are under `work/delivery-lab/results/<run-id>/`; `report.json` states PASS/FAIL and fingerprints the tested worker/harness source. Generated credentials, private keys, configs and full logs stay in ignored `work/`; publish only reviewed reports and samples.

```bash
./scripts/delivery_lab.sh verify       # unit + failure/replay integration scenarios
./scripts/delivery_lab.sh demo         # processing → outage → recovery proof
./scripts/delivery_lab.sh benchmark    # seeded steady load + measurements
LAB_BENCHMARK_SECONDS=60 LAB_RECORDS_PER_SECOND=500 LAB_PAYLOAD_BYTES=1024 ./scripts/delivery_lab.sh benchmark
./scripts/delivery_lab.sh down         # stop lab; preserves its PostgreSQL volume
```

The outage test stops **only this lab's PostgreSQL container**, restores it in a finally block, then explicitly restarts the paused worker. Fresh random source topics/targets isolate every test. Repeated runs retain old local tables/topics; use a dedicated disposable Docker context for long campaigns. The Kafka data is container-local; deleting its container discards it. Do not delete volumes or run cleanup commands against a shared/customer Docker context.

## Assertions

| Scenario | Evidence asserted against real services |
| --- | --- |
| Destination unavailable | Three failed destination attempts; committed offset remains at failed record; lag grows; restore + explicit worker restart drains backlog |
| Invalid JSON record | Visible validation pause; no destination effect and no source advance |
| Crash before write | Child exits at injected crash; no effects/commit; restart processes record |
| Crash after write before commit | Business row + audit trigger effect + ledger entry exist, source offset uncommitted; restart reports `duplicate`, with effect count still one |
| Rebalance | Second worker joins while 600 records across three partitions are in flight; revoke/assign events captured; all 600 identities/effects present exactly once |
| DLQ unavailable | Missing topic, auto-creation disabled; three attempts then visible pause, no source advance |
| DLQ acknowledged | Raw invalid bytes and duplicate/null headers recovered from the DLQ; source offset advances only after publication acknowledgment |
| Manual selected replay | Replay offsets 0–3 from an eight-record topic; only IDs 0–3 appear; original group unchanged; original restart ends with eight effects and no repeats |

`duplicate_effects=0` compares independent audit-trigger count to ledger identities, alongside row and distinct business-ID counts. This dataset uses one business ID per input record. It does not replace business reconciliation for applications with different keys/updates. The lab does not exercise network acknowledgment loss, forced max-poll expiration, broker/node crashes, cloud authentication, control-plane callbacks or a real Kubernetes rollout. The rebalance test is a normal live membership change, not proof of every split-brain/fencing condition.

## Measurement definitions and context

The report includes CPU model, visible CPUs, RAM, OS/architecture, cgroup metadata, Docker warnings/version, PostgreSQL version, native librdkafka/Python package versions, exact image digests, producer/destination/offset batch settings, topic/partition counts, payload sizes, worker count, duration and source fingerprint.

- **Throughput:** source records divided by wall time from load start through observation of every successful offset acknowledgment. This is an offered-load test, not maximum sustainable throughput. A 200/s result at 200/s input does not prove spare capacity.
- **End-to-end p50/p95:** source producer timestamp immediately before enqueue to production worker event immediately after synchronous source-offset commit acknowledgment. Includes source, database transaction and Kafka commit. Host producer/worker share a clock; Kafka timestamps have millisecond resolution. This is not a distributed clock-synchronization study.
- **Consumer lag:** broker high watermark minus broker-confirmed committed next offset, sampled approximately once per second; report peak is an observed lower bound, not an unsampled maximum. No commit is flagged explicitly instead of silently represented as a real offset zero.
- **Worker CPU/memory:** host process CPU (100% = one full core), CPU-time mean during the load interval, sampled peak RSS. Target cadence is 100 ms; Kafka/Docker queries and scheduling can lengthen it.
- **Kafka/PostgreSQL CPU/memory:** host-visible container process tree CPU-time deltas and summed RSS, approximately every three seconds. Short-lived processes can be missed and summed RSS counts shared pages more than once; it is not unique physical memory. Docker stats is deliberately avoided on rootless/no-cgroup hosts where it can report shared host usage. Requested CPU/memory caps and whether cgroups enforce them are recorded separately.
- **Error rate:** distinct source identities emitting `record_error` divided by produced records; retry attempt count is also reported. Invalid-record scenarios are assessed separately from the steady load.
- **Recovery:** time from PostgreSQL readiness after restore until all 15 outage-demo records are applied and source lag is zero; includes the explicit restart. Five are written before outage; ten queue during it. This small recovery experiment has one partition; it is not a saturated backlog-recovery benchmark.

The fixture uses a single broker (replication factor/minimum ISR = 1), one PostgreSQL destination, verified PostgreSQL TLS, loopback plaintext Kafka, one record per database transaction, synchronous offset commit per record, producer acks=all, idempotent source producer, batch size one, linger zero, no compression, PostgreSQL fsync/synchronous_commit on, and structured worker event logging. Management HTTP callbacks are disabled in the lab. Production networking, RF=3, authorization, management overhead and larger payloads will change results. PostgreSQL data may be warm from previous scenarios; each benchmark uses a fresh table/topic.

Published runs are under [benchmarks](benchmarks/). Keep the report and raw samples together, and report limitations alongside numbers. No UI or Vercel deployment is needed to run this backend evidence.
