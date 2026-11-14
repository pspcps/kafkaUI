# Managed replication connections

Jobs now select an approved `connectionId` and target table/collection. The job creation UI does not request database passwords, access keys, Vault tokens, provider URLs, secret names or role ARNs. New create/update requests containing inline connection credentials or endpoint overrides are rejected. Job responses remove legacy credentials as well as private endpoint/user fields. Existing records are not automatically erased or rotated.

## Operator configuration

Mount an operator-owned JSON file into the backend and set `CONNECTION_PROFILES_FILE` to its absolute path. Start from `config/connections.example.json`. Operator-only CSI templates are in `config/secret-provider-classes.example.yaml`; adapt and validate them for your installed provider version. Every profile has an ID, name, destination type, provider and a dedicated Kubernetes service account. The API exposes only ID, name, type and provider. Missing configuration yields an empty list; invalid configuration fails closed. Only admins can list/select profiles under the current role model.

This file must not contain static passwords, cloud access keys or tokens. Profiles are an allowlist, not an invitation for users to supply arbitrary secret paths. No cloud accounts, secret stores, CSI drivers, service accounts or policies are provisioned by the app or by the hosted demo.

## Supported paths

| Provider | Credential resolution | Operator prerequisites |
| --- | --- | --- |
| RDS IAM | Worker calls AWS SDK `generate_db_auth_token` when establishing each PostgreSQL/MySQL connection | Enable database IAM auth, grant `rds-db:connect` for the exact database/user, configure the profile's dedicated service account with IRSA/EKS Pod Identity (or equivalent default-chain identity), set region/endpoint/database/user and CA Secret |
| Vault | Secrets Store CSI driver projects credentials into the worker; Python reads the mounted document | Install the driver and Vault CSI provider; configure a least-privilege Vault Kubernetes-auth role for that service account and namespace, and a SecretProviderClass that projects the approved secret as `credentials.json` |
| AWS Secrets Manager | Secrets Store CSI driver projects credentials into the worker | Install the driver and AWS provider; scope the worker identity to the exact secret ARN and required KMS key, and configure a SecretProviderClass that projects the approved secret as `credentials.json` |
| Kubernetes Secret | Read-only Secret volume in the worker | Provision the named Secret, with a `credentials.json` key, through your secure secret-management workflow |

`credentials.json` is a UTF-8 JSON object containing `host`, `database`, `username`, `password`, and optional `port`. Never put its actual contents in this repository, a job request, screenshot, browser storage, or ticket. Workers read the fixed path `/var/run/kafkaui-credentials/credentials.json`; a job cannot override that path. The CSI provider owns upstream authentication, lease renewal and file rotation. Reconnects reread the mounted file. Existing database sessions can remain valid until closed; this is not instant revocation.

For PostgreSQL/MySQL, configure `caSecretName`; the Secret must have key `ca.pem` containing the CA bundle. It mounts at `/var/run/kafkaui-ca/ca.pem`. PostgreSQL uses `verify-full`; MySQL verifies both certificate and hostname. RDS IAM tokens are generated again when a new connection is established. This does not change the worker's delivery guarantees or implement full connection-failure recovery.

MongoDB uses verified TLS and optional custom CA. SQL Server now uses encryption with certificate verification; its CA must be trusted by the worker OS/ODBC installation. The current worker image still needs the SQL Server ODBC driver installed separately. These requirements must be validated for your exact destination before enabling a profile.

All worker deployments also reference `kafbat-worker-api` (key `token`) and **`kafbat-worker-db` (key `dsn`)**. The management DSN is no longer embedded directly in the Deployment manifest. Give the worker's database role read-only access only to the job/schema tables required by `worker/config.py`; do not reuse the backend administrator database role. Restrict the backend Kubernetes permissions and namespace because creating Deployments can delegate access to mounted Secrets and identities.

## Rollout and migration

1. Install/configure provider prerequisites and CA trust in a non-production namespace. Apply dedicated service accounts and least-privilege policies. Ensure the backend and worker can reach the intended private endpoints.
2. Provision the management API/DB Secrets, connection SecretProviderClasses or Secrets, and backend profile file.
3. Rebuild the worker image (including boto3) and backend. Publish a versioned image digest. New worker Deployments use the selected profile's service account and read-only credential/CA mounts.
4. Pause each legacy job, record its consumer group and committed offsets, and create a replacement job using an approved profile. Verify offset/replay behavior before starting it; do not run two unintended copies. New workers reject inline legacy credentials rather than silently using them. Changing an existing job to a different profile requires recreating its deployment; the update API returns a conflict instead of claiming an unmounted provider works.
5. Verify real destination writes, TLS hostname failures, denied identity/secret access, token refresh on reconnect and secret rotation. Confirm no passwords appear in job API responses or callback logs.
6. After verification, remove old credentials from management records/backups according to policy, and revoke/rotate previously stored passwords. Redaction alone does not remove historical exposure.

The Vercel preview shows sample profile choices and simulated job actions only. It never obtains real secrets or assumes a real cloud role. Unit tests cover allowlisting, redaction, identifier rejection, mount configuration, rotation reads and IAM token calls. No live AWS/Vault/Kubernetes/database integration was performed in this task. Tenant scoping, audit trails, SSO/session revocation and per-job callback identity remain enterprise prerequisites.

## Other verification limits

Topic-to-group discovery scans consumer subscriptions and retained offsets; it needs Kafka describe/group permissions and has not been load-tested with large group counts. Broker/permission failures return errors rather than an empty success list. Unknown/uncommitted lag is excluded from known totals. Message copy exports the API's text representation and available metadata; byte-preserving binary export, tombstone distinctions and duplicate-header fidelity still require a richer message API.

## Checks for this change

- Go unit tests and `go vet ./...` pass, including Kafka association/unknown-offset/permission cases, profile allowlisting, credential redaction, worker mount/identity configuration, and explicit errors when Kubernetes is unavailable.
- Python credential/callback tests pass; they mock AWS calls and temporary mounted files. Python destination modules compile.
- Next.js demo production build and both browser suites pass. Tests verify clipboard contents and denied clipboard access, per-topic consumer links, collapsed multi-topic lag, cross-links, password-free profile selection, browser-state cleanup, job creation, message/topic persistence and reset. No backend API requests or browser exceptions were observed in these demo checks.
- The backend/worker images and live Kafka, Vault, AWS, Kubernetes or destination databases were not exercised for this change. Only the frontend demo is deployed by the Vercel preview workflow.
