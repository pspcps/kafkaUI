# Enterprise readiness review

Reviewed baseline: `7d77c94` (`main`). This is an initial source review and first security patch, not a security certification or a completed enterprise release.

## Product direction

Start with one isolated deployment per company, connecting to multiple Kafka clusters inside its private network. This is the working assumption pending a hosting decision. Supporting many companies as customers does not require a shared database or shared Kafka credentials. A shared SaaS needs a separate tenant model, enforced ownership checks on every resource and worker operation, and tenant isolation tests before hosting customer data.

Keep the Next.js UI, Go API, and Python workers initially. Fix ownership, contracts, configuration, and operations before considering a rewrite.

## Confirmed findings

| Priority | Finding at baseline | Effect / action |
| --- | --- | --- |
| Critical | `/api/v1/clusters` serializes `ClusterConfig.Auth`, including passwords and AWS credentials | Fixed in this patch with an explicit metadata response. Rotate credentials previously exposed to users who should not hold them. |
| Critical | `RequireRole` exists but is never attached to API routes | Fixed: viewers can read; mutation requests require admin. Unknown roles are denied. |
| High | Worker metric/error callbacks have no authentication | Fixed with a separate service credential; missing/short configuration disables callbacks. |
| High | Job responses serialize destination passwords; workers query the management DB directly | This patch restricts job routes to admins. Next: safe response DTOs, write-only secret updates, secret references, least-privilege worker configuration API. Admin responses still contain credentials. |
| High | JWT verification accepts any HMAC method and does not require expiration or issuer | Fixed: HS256, issuer and expiration required; positive configured TTL. |
| High | Repository history includes a commit removing AWS credentials | Removal from the current tree does not revoke old credentials. Verify revocation/rotation and audit their use; do not copy historical secrets into tickets or logs. |
| High | Frontend pins Next.js 14.1.3; backend and worker dependencies are old | This patch updates Next.js/eslint-config-next to 14.2.35 and compatible lockfile dependencies. The audit decreases from 18 findings to 11 (6 moderate, 4 high, 1 critical); a supported framework migration is still a release blocker. Audit backend/worker dependencies too, generate an SBOM and scan release images. No claim of dependency safety in this patch. |
| High | Python Kafka consumer config only sets brokers/group/offsets | Backend MSK/SASL support does not provide cloud auth to workers. Implement and integration-test each worker authentication mode. |
| Medium | Worker image uses `PullNever` and desktop-specific addresses | Pull policy changed to `IfNotPresent`. Cloud deployment still needs registry images, service DNS, workload identity, and private connectivity. |
| Medium | Frontend Dockerfile copies a missing `public` directory | Fixed missing directory in this patch; image builds still need CI verification. |
| Medium | Next public API URL is compiled at build; Compose supplies it at runtime | Use same-origin requests through a runtime-configured server proxy. Test one image across environments. |
| Medium | README refers to absent `.env.example` and `SETUP.md` | Files restored with required Postgres/auth configuration and worker caveats; live Compose verification remains pending. |
| Medium | Tokens live in browser localStorage; login lacks rate limiting | Move to secure HttpOnly sessions with CSRF protection, add throttling, session revocation, OIDC and MFA via corporate identity providers. |
| Medium | No existing automated tests or CI configuration; Go dependency sums absent | Security regression tests added. Follow with reproducible builds, integration tests, vulnerability checks and release gates. |

## First patch: compatibility and rollout

- Cluster responses retain `id`, `name`, `brokers`; omit `auth` entirely.
- Viewers receive 403 on all mutations and all job routes. Job access remains admin-only until destination secrets are removed from responses. Cluster/topic/message reads remain available to viewers; these roles are not cluster-scoped yet.
- Set `WORKER_API_TOKEN` on the backend to a random secret with at least 32 characters (for example, generate with `openssl rand -hex 32`). Use a different secret from `JWT_SECRET`.
- In the worker namespace, provision Kubernetes Secret `kafbat-worker-api` with key `token`, containing the same value, via your secret manager or secure deployment workflow. New deployments reference this Secret instead of embedding the token in the deployment manifest.
- Upgrade backend and workers together. Existing worker deployments must be rolled out with the token environment reference; rebuilding a worker image alone does not update existing pod configuration. Old pods without a token will receive 401. An absent/short backend token returns 503 for callbacks. A missing Kubernetes Secret prevents new workers from starting.
- The shared worker token authorizes callbacks across jobs in this deployment; job-scoped identity, rotation without downtime, and quotas remain follow-up work. Use HTTPS on any untrusted network.
- Standard existing login tokens remain valid if they have the expected issuer, HS256 signature, expiration and user fields.
- `IfNotPresent` preserves locally loaded images and allows remote registry images. Pin release digests rather than mutable `latest` tags.

## Delivery sequence and acceptance criteria

### 1. Secure and reproducible foundation

Complete safe job response DTOs and write-only secret updates; encrypt/reference secrets; scope worker identity and DB permissions. Add login throttling, structured audit records for writes and denied actions, body limits, request timeouts, graceful shutdown, and real readiness checks. Upgrade dependencies and fix all clean builds. CI must run Go tests/vet, Python tests, TypeScript/build checks, vulnerability scans, and secret scanning.

Acceptance: a fresh checkout starts from documented configuration; viewer requests cannot mutate data; no API/log response reveals secrets; credential rotation and revoked sessions are tested; all CI gates pass.

### 2. Company identity and authorization

Add OIDC login using a configurable issuer, group-to-role mappings, admin/operator/viewer roles, cluster/topic resource scopes, production read-only policies, and audit retention/export. Use a versioned database migration runner and a separate migration role. Agree permission rules before implementing the UI role model.

Acceptance: two test teams cannot access each other's restricted clusters or jobs; production deletion and offset resets require explicit permission; offboarding revokes access.

### 3. Portable operations and cloud connectivity

Ship versioned non-root container images and a Helm chart with namespace-scoped RBAC, service accounts, resource limits, probes, disruption budgets, TLS ingress, secrets integration and network policies. Remove home kubeconfig mounts and desktop addresses from production paths. Separate API/read workloads from privileged worker orchestration where practical. Prefer workload identity to static cloud keys.

Create an executable compatibility matrix: self-managed Kafka (TLS and SASL), AWS MSK (IAM), Confluent Cloud, Azure Event Hubs Kafka endpoint, and GCP-hosted Kafka. Kafka-compatible services have API differences; advertise only tested capabilities. Add mTLS/custom CA and required SASL mechanisms based on customer requirements. Test both management API operations and replication workers independently.

Acceptance: the same image digests deploy to generic Kubernetes and at least one managed Kubernetes platform; provider smoke tests cover topics, messages, lag, authentication renewal and worker recovery. Track unsupported operations explicitly.

### 4. Reliability and functional correctness

Test cancellation, broker loss, partial cluster outages, bounded message browsing, concurrent load, and API pagination. Make configured-but-disconnected clusters visible with retry/status instead of silently dropping them. Validate replication behavior around destination failure, duplicate delivery, offset commits, partition rebalances, restart and schema changes. Document delivery guarantees. Add metrics, tracing, alerts and backup/restore drills for the management database.

Acceptance: failure tests demonstrate no silent message loss; documented recovery procedures work; latency and scale targets are agreed and load-tested.

### 5. Operator experience

Use a consistent cluster/environment switcher and persistent production indicator. Provide searchable, sortable topic/consumer tables with useful lag/health signals. Improve message browsing with pause/resume, clear offset/time navigation, structured payloads and masking. Provide connection diagnostics and actionable empty/error states. Make job failures explain the cause and recovery action. Hide unavailable actions based on permissions while retaining server enforcement; never imply that client-side hiding provides security.

Use accessible dialogs and keyboard navigation. For deleting topics or resetting offsets, show the target cluster, topic/group, and impact with an explicit confirmation. Test mobile/narrow layouts, long topic names, large payloads and screen readers.

Acceptance: browser tests cover connecting a cluster, finding consumer lag, inspecting a message, and recovering a failed job; destructive actions are unambiguous; viewers see a coherent read-only experience.

## Validation limits

A local compile/test does not establish connectivity, delivery guarantees, cloud compatibility or tenant isolation. No customer Kafka cluster, managed Postgres instance, or Kubernetes credentials were supplied for this review. Full live integration and browser checks remain required before release. Resolve licensing for third-party adoption: the repository currently has no license; the owner must choose appropriate terms.

## Checks for this patch

- Go 1.23.12: `go test ./...` and `go vet ./...` pass, including tests for actual mutation routes, worker callback protection, metadata redaction, and JWT validation.
- Python 3.12: `python -m unittest worker.test_callbacks` passes; callback modules compile.
- Frontend: clean `npm ci`, Next.js 14.2.35 production build including type checks.
- `npm audit`: 11 remaining findings (6 moderate, 4 high, 1 critical). Dependency migration remains required before a production release.
- Backend container build uses the project Dockerfile with a temporary session-only CA secret mount to trust this environment's outbound proxy. The repository Dockerfile contains no session CA material. No container has been deployed to customer infrastructure.
- Live Kafka, PostgreSQL, Kubernetes, full Compose, frontend container, and interactive browser flows are not validated by these checks.
