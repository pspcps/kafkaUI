# Record delivery, failure and replay

## Behavior before this change

The worker at [`9a351af`](https://github.com/pspcps/kafkaUI/blob/9a351afdc50dff12042a0faa8ac5ef86435b2ce1/worker/main.py) catches a destination insert failure, reports it to the management API, **then commits the source offset anyway**. A subsequent successful commit can also advance past earlier records. A failed callback does not prevent that commit. Restarting that worker cannot automatically recover the skipped record: an operator must replay retained source data. Thus the old implementation does not provide at-least-once delivery to the destination. A green pod or zero lag is not evidence that all business writes succeeded.

Old PostgreSQL upserts can repeat transactional trigger effects; final row count alone does not prove safe replay. SQL Server's old insert path can create duplicate rows. Do not apply the guarantees below retroactively to old workers or historical data.

## Chosen policy

The new default is **bounded retries, then pause without advancing the failed record**. It prioritizes preserving partition order and making failure visible over continued availability. A worker processes records serially:

1. Read a record with automatic offset storage and commits disabled.
2. Decode UTF-8 JSON and validate/coerce mapped fields.
3. Atomically claim its source identity and write the business row in PostgreSQL.
4. After database transaction acknowledgment, synchronously commit exactly that partition's next offset.
5. On transient destination errors, retry within the configured bound. On an invalid record or exhausted retries, hold processing until an operator fixes the cause and restarts/resumes the worker.

The pause applies to **all partitions owned by that worker**, not automatically all replicas of the job. Other workers can continue on their assigned partitions. Heartbeats/rebalance callbacks continue; prefetched records are not written or committed while paused. There is no total ordering across partitions. A poison record can block progress on its partition and the worker's other assigned partitions.

| Outcome | Destination behavior | Source offset |
| --- | --- | --- |
| Successful new record | Ledger and business write commit together | Commit next offset after database acknowledgment |
| Previously applied identity | Ledger prevents repeating the business statement | Commit next offset |
| Transient destination failure | Up to the configured attempts; unsuccessful local transaction rolls back | No advance past failed record; pause after exhaustion |
| Invalid JSON/mapping/constraint | Pause immediately under default policy | No advance past failed record |
| Authentication/schema/unknown infrastructure failure | Fail closed; never automatically classify as a discardable record | No advance past failed record |
| Crash before write | Nothing applied | Resume from broker-confirmed offset after restart |
| Crash after database commit, before Kafka commit | A replay finds the ledger entry and skips the business statement | Replay then commits next offset |
| Ambiguous database acknowledgment | Reconnect and check ledger on retry | Advance only after a successful destination result |
| Kafka commit failure | Retry offset commit within the bound; never send to DLQ | Broker may or may not have accepted an ambiguous commit; restart uses broker-confirmed offset |
| Assignment/generation loss | No intentional stale-lease commit; rejoin from broker-confirmed offsets | New owner may replay; ledger guards repeated writes |
| Committed offset outside retained range | Stop with an explicit recovery error; no automatic offset reset | Operator must select a reviewed recovery position |

A new group uses the configured earliest/latest initial position. Earliest means the current retained low watermark, not records already deleted by retention. `latest` intentionally excludes preexisting records. Kafka compaction can remove offsets within a requested range.

## PostgreSQL replay guard and its limits

The identity is `(source cluster namespace, topic, partition, offset)`. The ledger primary key adds a target ID derived from the destination schema/table. Both the ledger claim and business insert/upsert are in **one local PostgreSQL transaction**. On an existing ledger entry, the business statement is not executed, so transactional INSERT/UPDATE triggers are not repeated. Concurrent workers contend on the same unique ledger key rather than racing an application-side existence check.

This is at-least-once processing with a transactional idempotency guard at the PostgreSQL destination. **It is not a Kafka/PostgreSQL distributed transaction or an end-to-end exactly-once guarantee.** Its assumptions are:

- Keep the ledger at least as long as any possible replay. Do not truncate it independently of business data or restore either from inconsistent snapshots.
- Keep the source namespace and schema/table stable for replay. Renaming a cluster ID or target creates a new identity scope. After deleting/recreating a topic with reused offsets, use a new source namespace and a reviewed target/migration. The app does not discover Kafka topic incarnations.
- Do not modify the meaning of previously applied identities. A mapping fix does not reapply ledgered records. Rebuilding with different mappings requires a new reviewed target and reconciliation.
- PostgreSQL durability, permissions, backups and transactional triggers must be configured appropriately. External HTTP calls, nontransactional trigger effects and downstream systems are outside the ledger transaction.
- Deduplication is per source identity, not business event ID. Two Kafka records with the same business event and different offsets are different identities. A business primary key can update one row multiple times for those distinct events.
- Kafka retention must preserve unprocessed records through outages. Broker loss, management callback delivery, cloud credentials/rotation and a production Kubernetes deployment are not validated by this local lab.

Only PostgreSQL is enabled for replication in this change. New MySQL, MongoDB and SQL Server jobs are rejected, and the worker factory rejects those adapters until an equivalent guard and real integration evidence exist. Their old source modules are retained for reference; they are not covered by these guarantees. JSON is supported here; Avro/Protobuf/schema-registry decoding and tombstone-to-delete mapping require separate implementation. Tombstones pause by default.

## Optional dead-letter policy

An operator can select `DELIVERY_FAILURE_POLICY=dead_letter` and provision a distinct `DELIVERY_DLQ_TOPIC`. Only invalid records (validation errors and PostgreSQL data/constraint SQLSTATE classes 22/23) are eligible. Database outages, authentication/schema errors, unknown errors and source commit failures never drain valid records into a DLQ.

The worker publishes an envelope with `acks=all` and idempotent production, waits for acknowledgment, then commits the source offset. Missing topics are not auto-created. A failed/unavailable DLQ receives bounded retries, then the worker pauses without source advance. Provision replication/minimum ISR, retention, access control and monitoring to meet your durability requirements. The lab's single broker cannot prove broker-loss durability.

The envelope contains a stable `event_id`, source coordinates, safe error type/SQLSTATE, original timestamp, base64 original key/value, and a list preserving duplicate, binary and null headers. It intentionally contains the original payload: treat the DLQ as sensitive data, with access and retention policies at least as strict as the source. Operational logs/callbacks contain coordinates and error categories rather than payloads or driver connection strings.

DLQ acknowledgment and the source offset commit are not atomic. Crashes/acknowledgment ambiguity can create duplicate DLQ entries; downstream tooling must deduplicate `event_id`. Advancing after DLQ deliberately permits a gap in the business destination and later records to proceed. Choose default pause if strict contiguous processing is required. Recovery of a DLQ record needs a corrected mapping or explicit reviewed republishing/conversion; blindly replaying invalid bytes pauses again.

## Configuration and operations

These are operator environment settings on the backend, copied into newly created worker pods, or set directly when launching the worker. Update existing Deployment environments explicitly; editing the backend environment does not retroactively update existing pods.

| Variable | Default | Bound / meaning |
| --- | --- | --- |
| `DELIVERY_FAILURE_POLICY` | `pause` | `pause` or `dead_letter` |
| `DELIVERY_MAX_ATTEMPTS` | `3` | 1–5 total attempts, including first attempt |
| `DELIVERY_RETRY_BASE_SECONDS` | `0.5` | 0–5 seconds |
| `DELIVERY_RETRY_CAP_SECONDS` | `5` | 0–10 seconds; delay is `min(cap, base × 2^(attempt−1))` |
| `DELIVERY_DLQ_TOPIC` | unset | Required distinct preprovisioned topic for `dead_letter` |
| `DELIVERY_DLQ_TIMEOUT_SECONDS` | `10` | 1–10 seconds per acknowledgment attempt |

PostgreSQL uses a 10-second connect timeout, statement/lock timeouts and TCP keepalives. Retry counts/backoff are bounded; elapsed failure time also includes driver/network timeouts and scheduling. These are not hard real-time outage deadlines. Kafka max poll interval is five minutes; a lost generation is fenced and replayed through the ledger.

On failure, look for structured `record_error`, `record_retry` and `record_paused` events. `record_paused` names stage, attempts, source coordinates and `offsets_advanced=false`; Kafka acknowledgment ambiguity is still possible as described above. The management callback stores the error and marks the job paused. If that callback is unavailable, the record remains protected but the UI may be stale: alert from worker logs and actual committed-offset lag too. Pod liveness alone is not progress. The status sync preserves a reported delivery pause even if the pod is healthy. Resume changes the pod template to restart held workers and clears the displayed error; progress follows deployment reconciliation, not instantly on the HTTP response. Callback/control-plane loss and multi-replica coordination remain production verification work.

For default pause recovery: fix the destination or mapped record, inspect the failure coordinates, then explicitly Resume/restart. Do not reset offsets just to restart. If repairing the original record is impossible, choose a reviewed DLQ/skip policy and record the decision. Check final business effects, ledger entries and committed offsets, not only lag.

### Safe selected replay

Run the replay CLI in the worker environment with the same mounted profile, credentials, CA, `MGMT_DB_DSN`, `JOB_ID` and `KAFKA_BROKERS` as the source job:

```bash
# Dry run: prints identity/range and checks retention; performs no destination writes.
python worker/replay.py --partition 0 --from-offset 120 --through-offset 129

# First stop ALL source-group workers and keep them stopped during this command.
python worker/replay.py --partition 0 --from-offset 120 --through-offset 129 --execute
```

Offsets are inclusive. In the worker image's `/app` directory use `python replay.py ...`. No passwords are command arguments. The CLI rejects an out-of-retention selection and refuses execution while source-group members are present. This check is point-in-time, not a distributed maintenance lock: an operator must prevent restarts/other writers for the duration. It uses a separate replay group and leaves original-group offsets untouched. Replay uses the same ledger; it skips previously applied identities and applies previously missing ones. A paused/incomplete replay does not report completion. A compacted range can contain fewer records than its offset span. Repeat per partition and verify selected IDs/counts before resuming the original job.

Replaying historical updates can overwrite newer business state when that old identity has never been applied. Review ordering/business semantics and use a separate target for reconciliation where needed. Source offset reset is a different operation; the UI's broad reset is not this bounded replay command.

### Migration

Stop old workers first and record group offsets, failed coordinates and destination counts. Choose a new destination table or a reviewed offline migration: legacy tables without the new source-identity metadata are rejected. Do not fabricate ledger entries from incomplete legacy metadata or assume old rows have not already caused trigger effects. Reconcile old rows and previously skipped records from retained Kafka data/backups before cutover. Review primary keys and uniqueness assumptions. Retain and back up the ledger together with the destination. Build/version the backend and worker together; deploying frontend demo code alone does not install these guarantees.

See [the executable lab](DELIVERY_LAB.md) and its published measurement report for the evidence and reproduction steps.
