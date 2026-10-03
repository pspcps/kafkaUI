#!/usr/bin/env bash
# =============================================================================
# 02_seed_data.sh
# Publishes realistic dummy messages to all application topics.
#
# Usage:
#   ./scripts/02_seed_data.sh [cluster_id] [messages_per_topic]
#
# Examples:
#   ./scripts/02_seed_data.sh                # local, 5 messages/topic
#   ./scripts/02_seed_data.sh dev 20         # MSK dev, 20 messages/topic
#
# Environment overrides:
#   API_URL   base URL of the backend  (default: http://localhost:8080)
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
COUNT="${2:-5}"
API_URL="${API_URL:-http://localhost:8080}"
KAFBAT_ADMIN_USER="${KAFBAT_ADMIN_USER:-admin}"
KAFBAT_ADMIN_PASSWORD="${KAFBAT_ADMIN_PASSWORD:-}"
KAFBAT_TOKEN="${KAFBAT_TOKEN:-}"
BASE="$API_URL/api/v1/clusters/$CLUSTER"
AUTH_HEADER=()

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

# Produce a single message; args: topic key value_json headers_json
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
    "${AUTH_HEADER[@]}" \
    -d "$body")

  if [[ "$http_code" == "201" ]]; then
    success "[$topic]  key=$key"
  else
    fail "[$topic]  key=$key  (HTTP $http_code: $(cat /tmp/kafkaui_resp.json))"
  fi
}

# Lightweight random-ish string using timestamp + counter
rand_suffix() { echo "${RANDOM}${1:-}"; }
now_iso()     { date -u +"%Y-%m-%dT%H:%M:%S.%6NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# ── pre-flight ────────────────────────────────────────────────────────────────

echo ""
echo "=========================================="
echo " KafkaUI — Seed Data"
echo " Cluster : $CLUSTER"
echo " API     : $API_URL"
echo " Count   : $COUNT msg/topic"
echo "=========================================="

if ! curl -sf "$API_URL/health" > /dev/null 2>&1; then
  fail "Cannot reach API at $API_URL — is the backend running?"
  exit 1
fi
success "API is reachable"

if ! authenticate; then
  exit 1
fi

# ─────────────────────────────────────────────────────────────────────────────
# 1. device.vnet.troublecodes
# ─────────────────────────────────────────────────────────────────────────────
section "device.vnet.troublecodes ($COUNT messages)"

SERIAL_NUMBERS=("QM00074286" "QM00074287" "QM00074288" "QM00074289" "QM00074290"
                "QM00074291" "QM00074292" "QM00074293" "QM00074294" "QM00074295")
J1939_CODES=("7FEEB99B" "A1C3D2E4" "B2D4E5F6" "C3E5F6A7" "D4F6A7B8"
             "E5A7B8C9" "F6B8C9DA" "A7C9DAEB" "B8DAEBFC" "C9EBFC0D")
SPNS=("524011 FMI: 19" "524012 FMI: 03" "524013 FMI: 05" "524014 FMI: 07"
      "524015 FMI: 11" "524016 FMI: 13" "524017 FMI: 02" "524018 FMI: 15")
ECU_SOURCES=(232 128 64 255 16 33 72 144)
STACK_IDS=("01" "02" "03" "04" "05" "06" "07" "08")
COMPANY_IDS=("9490" "5957" "13415" "8823" "4401")
VEHICLE_NAMES=("0024059TFIN" "KK15ZLGP" "P306" "TRUCK001" "FLEET007")

for i in $(seq 1 "$COUNT"); do
  idx=$(( (i - 1) % 10 ))
  serial="${SERIAL_NUMBERS[$idx]}"
  code="${J1939_CODES[$idx]}"
  spn="${SPNS[$(( idx % ${#SPNS[@]} ))]}"
  ecu="${ECU_SOURCES[$(( idx % ${#ECU_SOURCES[@]} ))]}"
  occurrence=$(( RANDOM % 50 + 1 ))
  stack="${STACK_IDS[$(( idx % ${#STACK_IDS[@]} ))]}"
  company="${COMPANY_IDS[$(( idx % ${#COMPANY_IDS[@]} ))]}"
  vehicle="${VEHICLE_NAMES[$(( idx % ${#VEHICLE_NAMES[@]} ))]}"
  active=$( [[ $(( i % 3 )) -ne 0 ]] && echo "true" || echo "false" )
  ts=$(now_iso)

  device_id="3500ffff-6b4a-ada7-ce6a-60a3e15b$(printf '%04d' $i)"
  corr_id="f2687d9d-$(rand_suffix)-$(rand_suffix)-a6cf-eab71dfd$(printf '%04d' $i)"

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
  "utcOffsetMinutes": "-300",
  "vehicleName": "$vehicle",
  "serialNumber": "$serial",
  "upstreamName": "Lytx.DeviceIngest.Enricher",
  "timezone": "EST",
  "stackId": "$stack",
  "strVehicleId": "9100ffff-48a9-e863-021e-60a3e15b$(printf '%04d' $i)",
  "upstreamVersion": "1.0.75",
  "strCorrelationId": "$corr_id",
  "companyId": "$company",
  "groupName": "ELP",
  "strGroupId": "5100ffff-60b6-e4cd-ad9a-60a3e15b$(printf '%04d' $i)",
  "strDeviceId": "$device_id",
  "strRootGroupId": "5100ffff-60b6-e4cd-9871-60a3e15b0000",
  "strCompanyId": "$company",
  "modelNumber": "ER-SF300V2",
  "firmwareVersion": "6.58.304",
  "ianaTimezone": "America/Chicago"
}
JSON
)

  produce "device.vnet.troublecodes" "$serial" "$value" "$headers"
done

# ─────────────────────────────────────────────────────────────────────────────
# 2. trips.trip
# ─────────────────────────────────────────────────────────────────────────────
section "trips.trip ($COUNT messages)"

LATS=("-30.555967" "35.165454" "-26.204103" "51.507351" "-33.868820"
      "40.712776"  "48.856613" "55.755826"  "-1.286389" "35.689487")
LNGS=("29.426884" "-80.883483" "28.047305" "-0.127758" "151.209296"
      "-74.005974" "2.352222"  "37.617298"  "36.817223" "139.691711")
TIMEZONES=("AFD" "EST" "PST" "GMT" "AEST" "CST" "CET" "MSK" "EAT" "JST")
IANA_TZONES=("Africa/Johannesburg" "America/New_York" "America/Los_Angeles"
             "Europe/London" "Australia/Sydney" "America/Chicago"
             "Europe/Paris" "Europe/Moscow" "Africa/Nairobi" "Asia/Tokyo")
UTC_OFFSETS=("120" "-300" "-480" "0" "600" "-360" "60" "180" "180" "540")
TRIP_TYPES=("IGNITION" "IGNITION" "IGNITION" "MANUAL" "IGNITION")
MODELS=("ER-SF400" "ER-SF300V2" "ER-SF400" "ER-SF300" "ER-SF400V2")
VEHICLE_NAMES2=("KK15ZLGP" "TRUCK002" "FLEET009" "VAN001" "BUS007"
                "TRUCK010" "FLEET001" "VAN002"  "BUS001" "TRUCK005")
COMPANY_IDS2=("5957" "9490" "13415" "8823" "4401" "5957" "13415" "8823" "5957" "4401")

for i in $(seq 1 "$COUNT"); do
  idx=$(( (i - 1) % 10 ))
  trip_id="ffff$(printf '%04x' $RANDOM)-ffff-ffff-$(printf '%04x' $RANDOM)-9c720e85$(printf '%04x' $i)"
  vehicle_id="ffff$(printf '%04x' $RANDOM)-ffff-ffff-$(printf '%04x' $RANDOM)-9c720e85$(printf '%04x' $i)"
  device_id="3500ffff-6b4a-aca7-d1f7-8e63f0$(printf '%06x' $i)"
  lat="${LATS[$idx]}"
  lng="${LNGS[$idx]}"
  tz="${TIMEZONES[$idx]}"
  iana_tz="${IANA_TZONES[$idx]}"
  utc_off="${UTC_OFFSETS[$idx]}"
  trip_type="${TRIP_TYPES[$(( idx % ${#TRIP_TYPES[@]} ))]}"
  model="${MODELS[$(( idx % ${#MODELS[@]} ))]}"
  vehicle="${VEHICLE_NAMES2[$idx]}"
  company="${COMPANY_IDS2[$idx]}"
  ts=$(now_iso)
  corr_id="a3f4f495-$(rand_suffix 'a')-4cc0-bc2d-3ed1abf8$(printf '%04d' $i)"
  stack="${STACK_IDS[$(( idx % ${#STACK_IDS[@]} ))]}"
  reopen=$( [[ $(( i % 5 )) -eq 0 ]] && echo "true" || echo "false" )

  value=$(cat <<JSON
{
  "tripType": "$trip_type",
  "reopenedTrip": $reopen,
  "startDateTime": "$ts",
  "endDateTime": "9999-12-31T23:59:59.9999999",
  "startPosition": {
    "latitude": $lat,
    "longitude": $lng,
    "occurrenceDateTime": "$ts"
  }
}
JSON
)

  serial="QM$(printf '%08d' $(( 41089600 + i )))"

  headers=$(cat <<JSON
{
  "deviceType": "ER",
  "strEventRecorderId": "$device_id",
  "utcOffsetMinutes": "$utc_off",
  "vehicleName": "$vehicle",
  "serialNumber": "$serial",
  "upstreamName": "Lytx.Trips.Service",
  "timezone": "$tz",
  "stackId": "$stack",
  "strVehicleId": "$vehicle_id",
  "upstreamVersion": "1.0.10",
  "strCorrelationId": "$corr_id",
  "companyId": "$company",
  "groupName": "Auto X Durban",
  "strGroupId": "ffffe80c-ffff-ffff-507b-52f00c7f$(printf '%04x' $i)",
  "strDeviceId": "$device_id",
  "strRootGroupId": "5100ffff-60b6-d9cd-84af-9543e7480000",
  "strCompanyId": "$company",
  "modelNumber": "$model",
  "firmwareVersion": "6.58.304",
  "ianaTimezone": "$iana_tz"
}
JSON
)

  produce "trips.trip" "$trip_id" "$value" "$headers"
done

# ─────────────────────────────────────────────────────────────────────────────
# 3. trips.trip.unifieddistance
# ─────────────────────────────────────────────────────────────────────────────
section "trips.trip.unifieddistance ($COUNT messages)"

CITIES=("Charlotte" "Atlanta" "Dallas" "Denver" "Seattle"
        "Phoenix"   "Chicago" "Miami"  "Boston" "Portland")
STATES=("North Carolina" "Georgia" "Texas" "Colorado" "Washington"
        "Arizona" "Illinois" "Florida" "Massachusetts" "Oregon")
POSTAL_CODES=("28217" "30301" "75201" "80201" "98101"
              "85001" "60601" "33101" "02101" "97201")
COUNTIES=("Mecklenburg" "Fulton" "Dallas" "Denver" "King"
          "Maricopa" "Cook" "Miami-Dade" "Suffolk" "Multnomah")
START_LATS=("35.16545486450195" "33.749001" "32.776665" "39.742043"
            "47.606209" "33.448376" "41.878113" "25.773996"
            "42.361145" "45.523064")
START_LNGS=("-80.88348388671875" "-84.387982" "-96.796989" "-104.991531"
            "-122.332069" "-112.074036" "-87.629799" "-80.193657"
            "-71.057083" "-122.676483")
DISTANCES=("3.0" "5.2" "7.8" "2.1" "10.5" "4.3" "6.7" "8.9" "1.5" "12.3")
MAX_SPEEDS=("24.375" "45.0" "60.0" "35.5" "80.5" "50.0" "65.0" "55.5" "30.0" "90.0")
GEOCODE_SVC=("OSM" "OSM" "HERE" "OSM" "HERE" "OSM" "HERE" "OSM" "OSM" "HERE")
TRIP_VEHICLES=("P306" "TRUCK003" "FLEET002" "VAN003" "BUS002"
               "TRUCK006" "FLEET003" "VAN004" "BUS003" "TRUCK007")
GROUP_NAMES=("Charlotte" "Atlanta HQ" "Dallas Fleet" "Denver Ops" "Seattle Hub"
             "Phoenix Base" "Chicago Depot" "Miami Port" "Boston Yard" "Portland DC")
SOURCES=("IGNITION" "IGNITION" "MANUAL" "IGNITION" "IGNITION"
         "MANUAL" "IGNITION" "IGNITION" "MANUAL" "IGNITION")

for i in $(seq 1 "$COUNT"); do
  idx=$(( (i - 1) % 10 ))
  trip_id="dfb3e3d8-50b3-$(printf '%04x' $RANDOM)-$(printf '%04x' $RANDOM)-0b3512$(printf '%06x' $i)"
  driver_id="00000000-0000-0000-0000-000000000000"
  device_id="3500ffff-6b4a-afa7-439d-60a3e15b$(printf '%04x' $i)"
  vehicle_id="9100ffff-48a9-e963-b2dc-60a3e15b$(printf '%04x' $i)"

  city="${CITIES[$idx]}"
  state="${STATES[$idx]}"
  postal="${POSTAL_CODES[$idx]}"
  county="${COUNTIES[$idx]}"
  slat="${START_LATS[$idx]}"
  slng="${START_LNGS[$idx]}"
  dist="${DISTANCES[$idx]}"
  max_speed="${MAX_SPEEDS[$idx]}"
  geocoder="${GEOCODE_SVC[$idx]}"
  vehicle="${TRIP_VEHICLES[$idx]}"
  group="${GROUP_NAMES[$idx]}"
  source="${SOURCES[$idx]}"

  # end lat/lng slightly offset from start
  elat=$(echo "$slat + 0.0005" | bc 2>/dev/null || echo "$slat")
  elng=$(echo "$slng + 0.0003" | bc 2>/dev/null || echo "$slng")

  start_ts=$(now_iso)
  end_ts=$(now_iso)
  company="${COMPANY_IDS[$(( idx % ${#COMPANY_IDS[@]} ))]}"
  serial="QM$(printf '%08d' $(( 41036000 + i )))"

  value=$(cat <<JSON
{
  "distanceKm": $dist,
  "startDateTime": "$start_ts",
  "endDateTime": "$end_ts",
  "startPosition": {
    "localDateTime": "${start_ts%Z}",
    "country": "United States of America",
    "address": "service, $city, $state, $postal, United States of America",
    "streetNumber": null,
    "city": "$city",
    "timezone": "America/New_York",
    "latitude": $slat,
    "postalCode": "$postal",
    "county": "$county",
    "stateProv": "$state",
    "streetName": "service",
    "timezoneOffset": "-240",
    "occurrenceDateTime": "$start_ts",
    "longitude": $slng
  },
  "endPosition": {
    "localDateTime": "${end_ts%Z}",
    "country": "United States of America",
    "address": "service, $city, $state, $postal, United States of America",
    "streetNumber": null,
    "city": "$city",
    "timezone": "America/New_York",
    "latitude": $elat,
    "postalCode": "$postal",
    "county": "$county",
    "stateProv": "$state",
    "streetName": "service",
    "timezoneOffset": "-240",
    "occurrenceDateTime": "$end_ts",
    "longitude": $elng
  },
  "eventRecorderTimezone": "CST",
  "maxSpeedKph": $max_speed,
  "tripId": "$trip_id",
  "driverId": "$driver_id",
  "assignmentTriggerType": "NOT_AVAILABLE",
  "assignmentTriggerTypeId": 0,
  "maxSpeedKm": $max_speed,
  "groupName": "$group",
  "driverFirstName": null,
  "driverLastName": null,
  "vehicleName": "$vehicle",
  "reverseGeocodeService": "$geocoder",
  "deviceDistanceKm": null
}
JSON
)

  headers=$(cat <<JSON
{
  "strEventRecorderId": "$device_id",
  "companyId": "$company",
  "serialNumber": "$serial",
  "upstreamName": "flink-osm-reverse-geocoder",
  "strGroupId": "ffff2037-ffff-ffff-252d-$(printf '%012x' $i)",
  "strDeviceId": "$device_id",
  "strVehicleId": "$vehicle_id",
  "strRootGroupId": "5100ffff-60b6-e5cd-e3fa-60a3e15b0000",
  "upstreamVersion": "3.2.0",
  "strUserId": "$driver_id",
  "source": "$source",
  "distanceOnlyUpdate": "true"
}
JSON
)

  produce "trips.trip.unifieddistance" "$trip_id" "$value" "$headers"
done

# ── summary ───────────────────────────────────────────────────────────────────

TOTAL=$(( COUNT * 3 ))
echo ""
echo "=========================================="
success "Seeded $TOTAL messages across 3 topics on cluster \"$CLUSTER\""
echo "=========================================="
echo ""
echo "  View in UI → http://localhost:3000/clusters/$CLUSTER/topics"
echo ""
