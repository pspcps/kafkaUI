#!/usr/bin/env bash
# =============================================================================
# 03_seed_troublecodes.sh
# Publishes additional messages to the device.vnet.troublecodes topic.
#
# Usage:
#   ./scripts/03_seed_troublecodes.sh [cluster_id] [message_count]
#
# Examples:
#   ./scripts/03_seed_troublecodes.sh                # local, 10 messages
#   ./scripts/03_seed_troublecodes.sh dev 25         # cluster=dev, 25 messages
#
# Environment overrides:
#   API_URL   base URL of the backend  (default: http://localhost:8080)
# =============================================================================
set -euo pipefail

CLUSTER="${1:-local}"
COUNT="${2:-10}"
API_URL="${API_URL:-http://localhost:8080}"
BASE="$API_URL/api/v1/clusters/$CLUSTER"

# ── helpers ──────────────────────────────────────────────────────────────────

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()    { echo -e "  ${YELLOW}→${NC}  $*"; }
success() { echo -e "  ${GREEN}✓${NC}  $*"; }
fail()    { echo -e "  ${RED}✗${NC}  $*"; }
section() { echo -e "\n${CYAN}── $* ${NC}"; }

produce() {
  local topic="$1"
  local key="$2"
  local value="$3"
  local headers="$4"

  local body
  body=$(cat <<JSON
{
  "key": "$key",
  "value": $value,
  "headers": $headers
}
JSON
)

  local http_code
  http_code=$(curl -s -o /tmp/kafkaui_resp.json -w "%{http_code}" \
    -X POST "$BASE/topics/$topic/messages" \
    -H "Content-Type: application/json" \
    -d "$body")

  if [[ "$http_code" == "201" ]]; then
    success "[$topic]  key=$key"
  else
    fail "[$topic]  key=$key  (HTTP $http_code: $(cat /tmp/kafkaui_resp.json))"
  fi
}

rand_suffix() { echo "${RANDOM}${1:-}"; }
now_iso()     { date -u +"%Y-%m-%dT%H:%M:%S.%6NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# ── pre-flight ────────────────────────────────────────────────────────────────

echo ""
echo "=========================================="
echo " KafkaUI — Seed: device.vnet.troublecodes"
echo " Cluster : $CLUSTER"
echo " API     : $API_URL"
echo " Count   : $COUNT messages"
echo "=========================================="

if ! curl -sf "$API_URL/health" > /dev/null 2>&1; then
  fail "Cannot reach API at $API_URL — is the backend running?"
  exit 1
fi
success "API is reachable"

# ─────────────────────────────────────────────────────────────────────────────
# device.vnet.troublecodes — extended seed set
# ─────────────────────────────────────────────────────────────────────────────
section "device.vnet.troublecodes ($COUNT messages)"

SERIAL_NUMBERS=(
  "QM00085001" "QM00085002" "QM00085003" "QM00085004" "QM00085005"
  "QM00085006" "QM00085007" "QM00085008" "QM00085009" "QM00085010"
  "QM00085011" "QM00085012" "QM00085013" "QM00085014" "QM00085015"
)

J1939_CODES=(
  "1A2B3C4D" "2B3C4D5E" "3C4D5E6F" "4D5E6F7A" "5E6F7A8B"
  "6F7A8B9C" "7A8B9CAD" "8B9CADBE" "9CADBECF" "ADBECFD0"
  "BECFD0E1" "CFD0E1F2" "D0E1F2A3" "E1F2A3B4" "F2A3B4C5"
)

# SPN (Suspect Parameter Number) + FMI (Failure Mode Identifier) combos
SPNS=(
  "100 FMI: 1"   # Engine Oil Pressure — low
  "110 FMI: 0"   # Engine Coolant Temp — high
  "190 FMI: 2"   # Engine Speed — data erratic
  "91 FMI: 3"    # Accelerator Pedal — voltage high
  "174 FMI: 4"   # Fuel Temp — voltage low
  "105 FMI: 0"   # Intake Manifold Temp — high
  "102 FMI: 1"   # Boost Pressure — low
  "1127 FMI: 5"  # Turbocharger Speed — current low
  "3031 FMI: 14" # DPF Soot Load — special instruction
  "3251 FMI: 0"  # DPF Differential Pressure — high
  "3480 FMI: 3"  # DEF Tank Heater — voltage high
  "3362 FMI: 4"  # DEF Dosing Unit — voltage low
  "1569 FMI: 31" # Engine Derate — condition exists
  "5246 FMI: 7"  # AFT Fuel Injector — mechanical error
  "2791 FMI: 9"  # EGR Valve — abnormal update rate
)

ECU_SOURCES=(0 3 11 33 61 72 128 144 175 232 255 48 96 200 216)

STACK_IDS=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10")

COMPANY_IDS=("9490" "5957" "13415" "8823" "4401" "7732" "2210" "6643")

VEHICLE_NAMES=(
  "TRUCK-A01" "TRUCK-A02" "FLEET-B01" "FLEET-B02" "VAN-C01"
  "VAN-C02"   "BUS-D01"   "BUS-D02"   "RIG-E01"   "RIG-E02"
  "0024059TFIN" "KK15ZLGP" "P306" "HVY-F01" "HVY-F02"
)

MODEL_NUMBERS=("ER-SF300V2" "ER-SF400" "ER-SF400V2" "ER-SF300" "ER-SF500")
FIRMWARE_VERSIONS=("6.58.304" "6.59.100" "6.60.012" "6.57.210" "6.61.001")
GROUP_NAMES=("ELP" "NorthFleet" "SouthFleet" "EastOps" "WestOps" "MidWest" "PacificHub")
UTC_OFFSETS=("-300" "-360" "-420" "-480" "0" "60" "120" "180" "330" "540")
TIMEZONES=("EST" "CST" "MST" "PST" "GMT" "CET" "EET" "MSK" "IST" "JST")
IANA_TIMEZONES=(
  "America/New_York" "America/Chicago" "America/Denver" "America/Los_Angeles"
  "Europe/London" "Europe/Paris" "Europe/Helsinki" "Europe/Moscow"
  "Asia/Kolkata" "Asia/Tokyo"
)

NUM_SERIALS=${#SERIAL_NUMBERS[@]}
NUM_CODES=${#J1939_CODES[@]}
NUM_SPNS=${#SPNS[@]}
NUM_ECU=${#ECU_SOURCES[@]}
NUM_STACKS=${#STACK_IDS[@]}
NUM_COMPANIES=${#COMPANY_IDS[@]}
NUM_VEHICLES=${#VEHICLE_NAMES[@]}
NUM_MODELS=${#MODEL_NUMBERS[@]}
NUM_FW=${#FIRMWARE_VERSIONS[@]}
NUM_GROUPS=${#GROUP_NAMES[@]}
NUM_UTC=${#UTC_OFFSETS[@]}
NUM_TZ=${#TIMEZONES[@]}

for i in $(seq 1 "$COUNT"); do
  idx=$(( (i - 1) % NUM_SERIALS ))

  serial="${SERIAL_NUMBERS[$idx]}"
  code="${J1939_CODES[$(( idx % NUM_CODES ))]}"
  spn="${SPNS[$(( idx % NUM_SPNS ))]}"
  ecu="${ECU_SOURCES[$(( idx % NUM_ECU ))]}"
  occurrence=$(( RANDOM % 100 + 1 ))
  stack="${STACK_IDS[$(( idx % NUM_STACKS ))]}"
  company="${COMPANY_IDS[$(( idx % NUM_COMPANIES ))]}"
  vehicle="${VEHICLE_NAMES[$(( idx % NUM_VEHICLES ))]}"
  model="${MODEL_NUMBERS[$(( idx % NUM_MODELS ))]}"
  fw="${FIRMWARE_VERSIONS[$(( idx % NUM_FW ))]}"
  group="${GROUP_NAMES[$(( idx % NUM_GROUPS ))]}"
  utc_off="${UTC_OFFSETS[$(( idx % NUM_UTC ))]}"
  tz="${TIMEZONES[$(( idx % NUM_TZ ))]}"
  iana_tz="${IANA_TIMEZONES[$(( idx % NUM_TZ ))]}"
  active=$( [[ $(( i % 3 )) -ne 0 ]] && echo "true" || echo "false" )
  ts=$(now_iso)

  hex_i=$(printf '%04x' "$i")
  device_id="3500ffff-6b4a-ada7-ce6a-60a3e1$(printf '%06x' $i)"
  vehicle_id="9100ffff-48a9-e863-021e-60a3e1$(printf '%06x' $i)"
  group_id="5100ffff-60b6-e4cd-ad9a-60a3e1$(printf '%06x' $i)"
  corr_id="f2687d9d-$(rand_suffix)-$(rand_suffix)-a6cf-eab71$(printf '%07x' $i)"

  value=$(cat <<JSON
{
  "timestamp": "$ts",
  "source": "J1939",
  "code": "$code",
  "description": "SPN: $spn Occurrence Count: $occurrence~Received Network Data in Error",
  "active": $active,
  "ecuSource": $ecu
}
JSON
)

  headers=$(cat <<JSON
{
  "deviceType": "ER",
  "strEventRecorderId": "$device_id",
  "utcOffsetMinutes": "$utc_off",
  "vehicleName": "$vehicle",
  "serialNumber": "$serial",
  "upstreamName": "Lytx.DeviceIngest.Enricher",
  "timezone": "$tz",
  "stackId": "$stack",
  "strVehicleId": "$vehicle_id",
  "upstreamVersion": "1.0.75",
  "strCorrelationId": "$corr_id",
  "companyId": "$company",
  "groupName": "$group",
  "strGroupId": "$group_id",
  "strDeviceId": "$device_id",
  "strRootGroupId": "5100ffff-60b6-e4cd-9871-60a3e15b0000",
  "strCompanyId": "$company",
  "modelNumber": "$model",
  "firmwareVersion": "$fw",
  "ianaTimezone": "$iana_tz"
}
JSON
)

  produce "device.vnet.troublecodes" "$serial" "$value" "$headers"
done

echo ""
success "Done — $COUNT messages published to device.vnet.troublecodes"
echo ""
