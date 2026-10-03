#!/usr/bin/env bash
# =============================================================================
# 01_create_topics.sh
# Creates all application topics via the KafkaUI REST API.
#
# Usage:
#   ./scripts/01_create_topics.sh [cluster_id]
#
# Examples:
#   ./scripts/01_create_topics.sh            # defaults to "local"
#   ./scripts/01_create_topics.sh dev        # targets MSK dev cluster
#
# Environment overrides:
#   API_URL            base URL of the backend  (default: http://localhost:8080)
#   PARTITIONS         partition count           (default: 3)
#   REPLICATION_FACTOR replication factor        (default: 1 for local, 3 for non-local)
# =============================================================================
set -euo pipefail

# Load local env defaults when running directly from repo root.
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

CLUSTER="${1:-local}"
API_URL="${API_URL:-http://localhost:8080}"
KAFBAT_ADMIN_USER="${KAFBAT_ADMIN_USER:-admin}"
KAFBAT_ADMIN_PASSWORD="${KAFBAT_ADMIN_PASSWORD:-}"
KAFBAT_TOKEN="${KAFBAT_TOKEN:-}"

# Default RF: 1 for local Kafka, 3 for remote MSK clusters
if [[ "$CLUSTER" == "local" ]]; then
  RF="${REPLICATION_FACTOR:-1}"
else
  RF="${REPLICATION_FACTOR:-3}"
fi
PARTITIONS="${PARTITIONS:-3}"

BASE="$API_URL/api/v1/clusters/$CLUSTER"
AUTH_HEADER=()

# ── helpers ──────────────────────────────────────────────────────────────────

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()    { echo -e "  ${YELLOW}→${NC}  $*"; }
success() { echo -e "  ${GREEN}✓${NC}  $*"; }
fail()    { echo -e "  ${RED}✗${NC}  $*"; }

authenticate() {
  if [[ -n "$KAFBAT_TOKEN" ]]; then
    AUTH_HEADER=(-H "Authorization: Bearer $KAFBAT_TOKEN")
    success "Using provided KAFBAT_TOKEN"
    return 0
  fi

  if [[ -z "$KAFBAT_ADMIN_PASSWORD" ]]; then
    fail "KAFBAT_ADMIN_PASSWORD is required (set in .env or env)"
    return 1
  fi

  local token
  token=$(curl -sS -X POST "$API_URL/api/v1/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$KAFBAT_ADMIN_USER\",\"password\":\"$KAFBAT_ADMIN_PASSWORD\"}" \
    | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')

  if [[ -z "$token" ]]; then
    fail "Auth failed for user '$KAFBAT_ADMIN_USER'"
    return 1
  fi

  AUTH_HEADER=(-H "Authorization: Bearer $token")
  success "Authenticated as $KAFBAT_ADMIN_USER"
}

create_topic() {
  local name="$1"
  local partitions="${2:-$PARTITIONS}"
  local rf="${3:-$RF}"
  local retention_ms="${4:-604800000}"   # 7 days default

  info "Creating topic: $name  (partitions=$partitions, RF=$rf)"

  local body
  body=$(cat <<JSON
{
  "name": "$name",
  "partitions": $partitions,
  "replicationFactor": $rf,
  "configs": {
    "retention.ms": "$retention_ms",
    "cleanup.policy": "delete",
    "compression.type": "lz4"
  }
}
JSON
)

  local http_code
  http_code=$(curl -s -o /tmp/kafkaui_resp.json -w "%{http_code}" \
    -X POST "$BASE/topics" \
    -H "Content-Type: application/json" \
    "${AUTH_HEADER[@]}" \
    -d "$body")

  if [[ "$http_code" == "201" ]]; then
    success "$name created"
  elif [[ "$http_code" == "500" ]] && grep -q "already exists" /tmp/kafkaui_resp.json 2>/dev/null; then
    success "$name already exists — skipping"
  else
    fail "$name  (HTTP $http_code: $(cat /tmp/kafkaui_resp.json))"
    return 1
  fi
}

# ── pre-flight ────────────────────────────────────────────────────────────────

echo ""
echo "=========================================="
echo " KafkaUI — Create Topics"
echo " Cluster : $CLUSTER"
echo " API     : $API_URL"
echo " Parts   : $PARTITIONS  |  RF: $RF"
echo "=========================================="
echo ""

# Verify API is reachable
if ! curl -sf "$API_URL/health" > /dev/null 2>&1; then
  fail "Cannot reach API at $API_URL — is the backend running?"
  exit 1
fi
success "API is reachable"

if ! authenticate; then
  exit 1
fi
echo ""

# ── topics ───────────────────────────────────────────────────────────────────

echo "── Trouble Codes ──────────────────────────"
create_topic "device.vnet.troublecodes" "$PARTITIONS" "$RF" "604800000"

echo ""
echo "── Trips ──────────────────────────────────"
create_topic "trips.trip" "$PARTITIONS" "$RF" "2592000000"          # 30 days
create_topic "trips.trip.unifieddistance" "$PARTITIONS" "$RF" "2592000000"

echo ""
echo "=========================================="
success "All topics created on cluster \"$CLUSTER\""
echo "=========================================="
echo ""
