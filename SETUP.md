# Development setup

The supplied Compose stack is a local development setup. Production prerequisites and known limitations are in [enterprise readiness](docs/ENTERPRISE_READINESS.md).

1. Copy `.env.example` to `.env`.
2. Supply distinct random `POSTGRES_PASSWORD`, `MONGO_PASSWORD`, `JWT_SECRET`, `KAFBAT_ADMIN_PASSWORD`, and `WORKER_API_TOKEN` values. Generate secrets with `openssl rand -hex 32`. Keep the file private.
3. For bundled Kafka, keep `KAFKA_BOOTSTRAP=kafka:9093` and run `docker compose --profile local up --build`.
4. Open `http://localhost:3000` and sign in with the configured admin credentials. The backend is on port 8080. Replication additionally needs Kubernetes; follow the worker credential rollout instructions before creating jobs.

For remote Kafka, set reachable bootstrap addresses and the matching backend authentication settings in `.env`, then run `docker compose up --build` without the local profile. Both bootstrap and advertised broker addresses must be reachable from the backend. See the README environment reference. Remote replication is not yet validated: workers currently lack the backend's SASL/IAM configuration support.

For native development, provision PostgreSQL, apply the SQL migrations in filename order, and export `MGMT_DB_DSN`, `JWT_SECRET`, and initial admin credentials. Configure Kafka environment variables for addresses reachable from your host. Run `go mod download && go run ./cmd/server` in `backend`, and `npm ci && npm run dev` in `frontend`. Native processes do not automatically load `.env`.

The current admin bootstrap updates that account's password and role whenever the configured credentials are present on startup. Remove both KAFBAT_ADMIN_USER and KAFBAT_ADMIN_PASSWORD after provisioning and use a managed identity solution before enterprise rollout. Do not reuse this development database account or Docker Desktop kubeconfig arrangement in production.
