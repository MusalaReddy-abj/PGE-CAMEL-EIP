#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# load-simulation.sh
# High-throughput simulation of the Kraken CIS Integration API.
# Demonstrates elasticity by ramping up request volume and observing
# Kafka consumer lag growth and pod scale-out via KEDA.
#
# Usage:
#   ./scripts/load-simulation.sh [HOST] [RATE] [DURATION_SEC]
#
# Defaults:
#   HOST          http://localhost:9080
#   RATE          50   (requests per second across all endpoints)
#   DURATION_SEC  120  (run for 2 minutes)
#
# Requirements: curl, parallel (brew install parallel / apt install parallel)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

HOST="${1:-http://localhost:9080}"
RATE="${2:-50}"
DURATION="${3:-120}"

ALARM_URL="${HOST}/api/v1/alarms"
RCDC_URL="${HOST}/api/v1/rcdc"
RCDC_RESP_URL="${HOST}/api/v1/rcdc/response"

echo "═══════════════════════════════════════════════════"
echo " Kraken CIS — Load Simulation"
echo "  Host     : ${HOST}"
echo "  Rate     : ${RATE} req/s"
echo "  Duration : ${DURATION}s"
echo "═══════════════════════════════════════════════════"

# ── Helper: generate a random meter ID ────────────────────────────────────────
meter_id() { printf "MTR-%08d" $((RANDOM % 99999999)); }
corr_id()  { cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen; }

# ── Send one alarm batch (10 events) ─────────────────────────────────────────
send_alarms() {
  local cid; cid=$(corr_id)
  curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${ALARM_URL}" \
    -H "Content-Type: application/json" \
    -H "X-Correlation-ID: ${cid}" \
    -d "{
      \"header\": { \"verb\": \"POST\", \"noun\": \"Alarm\" },
      \"payload\": {
        \"events\": [
          { \"category\": \"POWER_OUTAGE\",        \"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"0V\" },
          { \"category\": \"TAMPER_ALERT\",         \"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"DETECTED\" },
          { \"category\": \"COMMUNICATION_FAILURE\",\"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"OFFLINE\" },
          { \"category\": \"VOLTAGE_SAG\",          \"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"195V\" },
          { \"category\": \"POWER_OUTAGE\",        \"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"0V\" },
          { \"category\": \"TAMPER_ALERT\",         \"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"DETECTED\" },
          { \"category\": \"COMMUNICATION_FAILURE\",\"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"OFFLINE\" },
          { \"category\": \"VOLTAGE_SAG\",          \"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"195V\" },
          { \"category\": \"POWER_OUTAGE\",        \"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"0V\" },
          { \"category\": \"TAMPER_ALERT\",         \"createdDateTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"deviceIdentifierNumber\": \"$(meter_id)\", \"value\": \"DETECTED\" }
        ]
      }
    }"
}

# ── Send one RCDC command ─────────────────────────────────────────────────────
send_rcdc() {
  local cid; cid=$(corr_id)
  local mrid; mrid="1.21.120.0.0.0.0.0.0.0.0.0.0.0.0.0.0.$(( RANDOM % 9999 ))"
  local state; state=$([ $(( RANDOM % 2 )) -eq 0 ] && echo CONNECT || echo DISCONNECT)
  curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${RCDC_URL}" \
    -H "Content-Type: application/xml" \
    -H "X-Correlation-ID: ${cid}" \
    -d "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<requestMessage xmlns=\"http://xmlns.oracle.com/ouaf/iec\">
  <header>
    <verb>Create</verb><noun>RCDSwitchState</noun>
    <replyAddress>http://hes.pge.com/callback/rcdc</replyAddress>
    <correlationID>${cid}</correlationID>
  </header>
  <payLoad>
    <RCDSwitchState>
      <endDeviceAsset><mRID>${mrid}</mRID></endDeviceAsset>
      <state>${state}</state>
    </RCDSwitchState>
  </payLoad>
</requestMessage>"
}

# ── Send one HES response callback ───────────────────────────────────────────
send_hes_response() {
  local cid; cid=$(corr_id)
  curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${RCDC_RESP_URL}" \
    -H "Content-Type: application/json" \
    -H "X-Correlation-ID: ${cid}" \
    -d "{
      \"Header\": {
        \"Verb\": \"Reply\", \"Noun\": \"RCDSwitchState\",
        \"Timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
        \"CorrelationID\": \"${cid}\"
      },
      \"Reply\": { \"ReplyCode\": \"0\" },
      \"Payload\": {
        \"DefaultResponse\": {
          \"EndDeviceAsset\": { \"mRID\": \"$(meter_id)\" }
        }
      }
    }"
}

export -f send_alarms send_rcdc send_hes_response meter_id corr_id
export ALARM_URL RCDC_URL RCDC_RESP_URL

# ── Ramp: 3 phases ───────────────────────────────────────────────────────────
PHASE=$((DURATION / 3))

echo ""
echo "Phase 1: RAMP UP (${PHASE}s) — $(( RATE / 3 )) req/s"
seq 1 $(( RATE / 3 * PHASE )) | \
  parallel -j$(( RATE / 3 )) --delay 1 \
    'n=$(( {%} % 3 )); [ $n -eq 0 ] && send_alarms || [ $n -eq 1 ] && send_rcdc || send_hes_response' 2>/dev/null | \
  sort | uniq -c | awk '{print "  HTTP " $2 ": " $1 " responses"}' &

sleep ${PHASE}

echo ""
echo "Phase 2: PEAK LOAD (${PHASE}s) — ${RATE} req/s"
seq 1 $(( RATE * PHASE )) | \
  parallel -j${RATE} --delay 1 \
    'n=$(( {%} % 3 )); [ $n -eq 0 ] && send_alarms || [ $n -eq 1 ] && send_rcdc || send_hes_response' 2>/dev/null | \
  sort | uniq -c | awk '{print "  HTTP " $2 ": " $1 " responses"}' &

sleep ${PHASE}

echo ""
echo "Phase 3: COOL DOWN (${PHASE}s) — $(( RATE / 5 )) req/s"
seq 1 $(( RATE / 5 * PHASE )) | \
  parallel -j$(( RATE / 5 )) --delay 1 \
    'n=$(( {%} % 3 )); [ $n -eq 0 ] && send_alarms || [ $n -eq 1 ] && send_rcdc || send_hes_response' 2>/dev/null | \
  sort | uniq -c | awk '{print "  HTTP " $2 ": " $1 " responses"}' &

sleep ${PHASE}
wait

echo ""
echo "═══════════════════════════════════════════════════"
echo " Simulation complete."
echo " Check Kafka consumer lag:"
echo "   kafka-consumer-groups.sh --bootstrap-server ${HOST##http://}:9092 \\"
echo "     --describe --group kraken-cis-group"
echo " Check pod count (KEDA scaling):"
echo "   kubectl get pods -l app=kraken-cis"
echo " Check Actuator route stats:"
echo "   curl ${HOST%:*}:9091/actuator/camelroutes"
echo "═══════════════════════════════════════════════════"
