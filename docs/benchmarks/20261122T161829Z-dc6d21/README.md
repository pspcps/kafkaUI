# Measured delivery evidence: 20261122T161829Z-dc6d21

Status: **PASS**. Reproduce with `./scripts/delivery_lab.sh all` from the repository root. Full method and limitations: [delivery lab](../../DELIVERY_LAB.md). This is a measured offered-load run, **not a maximum-throughput claim or a production certification**.

| Measurement | Result |
| --- | --- |
| Committed throughput | 196.69 records/s at 200/s offered load |
| End-to-end p50 / p95 | 3.07 / 13.41 ms |
| Lag: observed peak / final | 3 / 0 records |
| Steady-load record error rate | 0% (0 / 4000) |
| Worker CPU mean / sampled peak | 11.31% / 26.50% of one core |
| Worker sampled peak RSS | 26.09 MiB |
| Outage recovery after destination ready | 2.48 s, including explicit worker restart |
| kafka CPU mean / sampled peak | 17.48% / 19.13% of one core |
| kafka sampled peak summed process RSS | 331.21 MiB (shared pages can be counted repeatedly) |
| postgres CPU mean / sampled peak | 11.74% / 14.48% of one core |
| postgres sampled peak summed process RSS | 102.49 MiB (shared pages can be counted repeatedly) |

## Run context

- Hardware: AMD EPYC 9V74 80-Core Processor; 3 visible vCPUs; 9968 MiB visible RAM; Linux-6.18.57-x86_64-with-glibc2.41 (x86_64). Shared virtual machine; no dedicated-hardware isolation claim.
- Software: Python 3.12.14; Docker 29.9.2; librdkafka 2.5.3; confluent-kafka 2.5.3; psycopg2-binary 2.9.9; PostgreSQL 16.16 on x86_64-pc-linux-musl, compiled by gcc (Alpine 15.2.0) 15.2.0, 64-bit.
- Source: Kafka native 3.9.1, single broker/RF=1/minISR=1; three benchmark partitions; one worker. Images pinned by digest in Compose and recorded in `report.json`.
- Load: 4,000 records, exactly 512-byte JSON values plus keys; offered at 200/s for 20 seconds; 20.336 seconds until all offset acknowledgments observed. Fresh topic/table; database/cache may be warm from preceding scenarios.
- Batching: one record per PostgreSQL transaction; one synchronous offset commit per record; producer batch one, linger zero, no compression, acks=all and idempotence. PostgreSQL fsync/synchronous_commit enabled; verified PostgreSQL TLS; worker event logging enabled; management HTTP callbacks disabled.
- Compose requests Kafka 1 CPU/512 MiB and PostgreSQL 1 CPU/256 MiB. **This rootless Docker host has no cgroups, so those caps were not enforced.** Worker runs on the host. CPU/resource results therefore describe this shared host rather than a capped deployment.
- Resource numbers use host-visible process CPU/RSS. Container RSS sums can double-count shared pages; short-lived processes can be missed. Peak lag/resources are sampled observations, not continuous maxima.
- Tested worker/harness SHA-256: `7456f3a89aa2954ef4cb6400a9726f75dac02dcc4d0d442844df4a8fd0fdc8b3`. Complete dependency versions, cgroup metadata and image digests are in the report.

## Failure and replay proof

All eight integration scenarios passed: crash before write, crash after write before commit, invalid record, unavailable DLQ, acknowledged DLQ, selected manual replay, live consumer rebalance, and destination outage/recovery. Fifteen Python unit tests and the backend Go tests/`go vet` also passed locally; the JSON report records the integration and benchmark results.

The crash-after-write test observed one database row, one independent transactional trigger effect and one ledger entry **before** restart, with no committed source offset. Restart reported `duplicate` and left trigger-effect count at one. Rebalance finished with 600 distinct rows/effects/ledger entries and zero repeated effects. Manual replay recovered only selected IDs 0–3 first, left the original group unchanged, and original processing finished with eight effects for eight records.

### Outage demonstration

The separate one-partition outage test wrote five records, stopped PostgreSQL, and queued ten more. After three failed attempts the source's committed next offset stayed at 5 while its high watermark reached 15 (lag 10). PostgreSQL was restored, then the paused worker was explicitly restarted. Final counts: **15 rows, 15 distinct business IDs, 15 trigger effects, 15 ledger entries; zero repeated effects and lag zero**. Recovery from destination readiness took 2.48 seconds. This is a small recovery demonstration, not a saturated backlog benchmark.

| Event | Seconds from first processing observation |
| --- | --- |
| processing | 0.000 |
| destination_stopped | 0.240 |
| paused | 0.568 |
| destination_restored | 1.816 |
| operator_restarts_worker | 3.084 |
| recovered | 4.293 |

## Raw evidence

- [Report, assertions and machine/software context](report.json)
- [Broker-confirmed lag samples](lag-samples.json)
- [Worker resource samples](worker-resource-samples.json)
- [Kafka/PostgreSQL process-tree resource samples](container-resource-samples.json)

PostgreSQL is the only enabled destination with this transactional replay guard. DLQ writes can repeat across a crash before the source commit. No end-to-end exactly-once guarantee is claimed. Broker failure, cloud credentials, a real Kubernetes rollout, ambiguous network acknowledgments and control-plane outages need further verification.
