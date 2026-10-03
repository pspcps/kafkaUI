# KafkaUI

A Kafka cluster management UI built with **Next.js 14** (frontend) and **Go + Gin** (backend), inspired by Kafbat UI.

## Features

- **Cluster Dashboard** — broker count, topic count, consumer groups, controller info
- **Topics Management** — list, create, delete topics; view partition details & configs
- **Message Browser** — browse messages with partition/offset/key/value filters; produce messages
- **Consumer Groups** — list groups with state & lag; per-topic per-partition lag breakdown

---

## Project Structure

```
kafbat-ui/
├── backend/              # Go + Gin REST API
│   ├── cmd/server/       # Main entrypoint
│   ├── internal/
│   │   ├── api/          # HTTP handlers & router
│   │   ├── kafka/        # Kafka admin, consumer, producer
│   │   └── models/       # Shared data models
│   ├── go.mod
│   └── Dockerfile
├── frontend/             # Next.js 14 (App Router)
│   ├── src/app/          # Pages: /, /topics, /topics/[name], /consumer-groups, /brokers
│   ├── src/components/   # UI components
│   ├── src/lib/          # API client, utilities
│   └── src/types/        # TypeScript types
└── docker-compose.yml    # Full stack (Zookeeper + Kafka + Backend + Frontend)
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/cluster` | Cluster overview |
| GET | `/api/v1/brokers` | List brokers |
| GET | `/api/v1/topics` | List topics |
| POST | `/api/v1/topics` | Create topic |
| GET | `/api/v1/topics/:name` | Topic detail |
| DELETE | `/api/v1/topics/:name` | Delete topic |
| GET | `/api/v1/topics/:name/messages` | Browse messages |
| POST | `/api/v1/topics/:name/messages` | Produce message |
| GET | `/api/v1/consumer-groups` | List consumer groups |
| GET | `/api/v1/consumer-groups/:id` | Consumer group detail |
| GET | `/health` | Health check |

---

## Prerequisites

- **Go 1.21+** — [install](https://go.dev/doc/install)
- **Node.js 20+** — via nvm: `nvm install 20 && nvm use 20`
- **Docker + Docker Compose** — for running Kafka locally
- **Kafka** — running on `localhost:9092` (or configure `KAFKA_BROKERS`)

---

## Quick Start

> **Need provider-specific instructions, troubleshooting, or multi-cluster setup?** See **[SETUP.md](SETUP.md)** for the full integration guide (AWS MSK, Azure Event Hubs, Confluent Cloud, self-managed, native dev, troubleshooting).

### 1. Configure your cluster

```bash
cp .env.example .env
# Open .env and uncomment one of the provider blocks:
#   • local dev (bundled Kafka)
#   • AWS MSK (IAM)
#   • Azure Event Hubs (SASL/PLAIN over TLS)
#   • Confluent Cloud / GCP / self-managed (SASL/SCRAM-SHA-512 over TLS)
#   • self-managed plain TLS
```

### 2. Start the stack

**Pointing at a remote cluster (MSK / Azure / Confluent / self-managed):**
```bash
docker compose up --build
```

**Local development (boots a bundled Kafka broker):**
```bash
docker compose --profile local up --build
```

- Frontend: http://localhost:3000
- Backend API: http://localhost:8080

### Local dev without Docker

```bash
# Backend
cd backend && go mod tidy && go run ./cmd/server

# Frontend (separate terminal)
cd frontend && npm install && npm run dev
```

The backend reads the same `KAFKA_*` env vars whether it runs in Docker or natively.

---

## Cluster connection — `.env` reference

Single-cluster setups configure everything via env vars. All variables are documented in [.env.example](.env.example).

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_BOOTSTRAP` | yes (single-cluster) | Comma-separated `host:port` list |
| `KAFKA_CLUSTER_NAME` | no | Display name (default: `Default`) |
| `KAFKA_AUTH_TYPE` | no | `none` \| `msk_iam` \| `sasl_scram` \| `sasl_plain` (default: `none`) |
| `KAFKA_USERNAME` | for SASL | SASL username (or `$ConnectionString` for Azure Event Hubs) |
| `KAFKA_PASSWORD` | for SASL | SASL password / API secret / connection string |
| `KAFKA_TLS` | no | `true` to force TLS when `KAFKA_AUTH_TYPE=none` |
| `KAFKA_AWS_REGION` | for `msk_iam` | e.g. `us-west-2` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | optional | Static MSK creds; otherwise the default AWS credential chain is used |

### Multi-cluster setups

For more than one cluster, point the backend at a JSON file instead:

```bash
CLUSTERS_CONFIG_FILE=/app/clusters.json
```

Each entry uses the same fields as [backend/internal/models/models.go](backend/internal/models/models.go) `ClusterConfig`. See [clusters.json](clusters.json) for an example.

### Other env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Backend HTTP port |
| `GIN_MODE` | `debug` | `debug` or `release` |
| `NEXT_PUBLIC_API_URL` | `http://localhost:8080` | Frontend → backend URL |

---

## Installing Go (if not installed)

```bash
# macOS via Homebrew
brew install go

# Or download from https://go.dev/dl/
```

## Upgrading Node.js via nvm

```bash
nvm install 20
nvm use 20
nvm alias default 20
```
