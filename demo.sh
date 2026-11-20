#!/bin/bash
# Single entry point for the OTel-vs-Kafka resiliency demo.
#
#   ./demo.sh up <otel|kafka>        start a fresh stack to explore (Kibana http://localhost:5601)
#   ./demo.sh run <scenario> [--stress]   one experiment: reset -> load -> fault -> verify
#   ./demo.sh matrix                 the twelve scenarios x REPS (default 3), then the results table
#   ./demo.sh load <otel|kafka>      start the data sources against a stack started with 'up'
#   ./demo.sh chaos <fault> [secs]   inject a fault into a running stack
#   ./demo.sh verify <scenario>      compare the sources' ledger with Elastic (+ Kafka)
#   ./demo.sh monitor <otel|kafka>   live view: generated / acked / in Elastic / sink lag
#   ./demo.sh down                   remove everything (KEEP_COLIMA=1 keeps the VM)
#
# scenarios: otel-e2e | otel-e2e-kill | otel-e2e-sync | otel-e2e-sync-kill | otel-e2e-stateless | otel-e2e-stateless-kill | otel-kafka | otel-kafka-kill
#   otel-e2e      : Agent acks the source after writing to ITS OWN disk queue, then forwards to the Gateway in the background
#   otel-e2e-sync : Agent has no disk and acks the source only after the Gateway wrote to ITS disk (same ack depth as otel-kafka)
#   otel-e2e-stateless : no disk anywhere; the ack travels back only after Elastic confirmed (source <- Agent <- Gateway <- Elastic)
#   --stress  : "kill" scenarios only; takes Elastic down first so a backlog exists (disk-loss-es-down)
# env: RATE (events/s in total, split over 3 sources, default 15000)  DURATION (s, 60)  CLIENT_RETRY (on|off, on)
#      BATCH (events per OTLP request, 500)  REPS (3)  FAULT_AT (s into the load, 5)  KILLS (5)  KILL_GAP (s, 10)  ES_DOWN (s, 60)
#   *-es-kill : ELASTIC is killed (kill -9) and kept down for ES_DOWN seconds (default 60), then restarted (the fault is on the destination)
# faults: disk-loss disk-loss-es-down agent-disk-loss gateway-kill broker-kill two-brokers agent-kill es-outage partition none
cd "$(dirname "$0")" || exit 1
ROOT="$PWD"
source scripts/docker-env.sh          # sets $DC, colima_start/colima_stop

export RATE="${RATE:-15000}" DURATION="${DURATION:-60}" CLIENT_RETRY="${CLIENT_RETRY:-on}" BATCH="${BATCH:-500}"
FAULT_AT="${FAULT_AT:-5}"
ts()  { date +%H:%M:%S; }
say() { echo "[$(ts)] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------- scenario names
parse_scenario() {   # sets ARCH (otel|kafka), FAULT, NAME
  local s="$1" stress="$2"
  GATEWAY_CFG=gateway.yaml
  AGENT_OTEL_CFG=agent-otel.yaml     # reset: the sync scenarios override it below (no leaking between scenarios in a matrix)
  case "$s" in
    otel-e2e)         ARCH=otel;  FAULT=none ;;
    otel-e2e-kill)    ARCH=otel;  FAULT=disk-loss ;;
    otel-e2e-sync)       ARCH=otel;  FAULT=none;      AGENT_OTEL_CFG=agent-otel-sync.yaml ;;
    otel-e2e-sync-kill)  ARCH=otel;  FAULT=disk-loss; AGENT_OTEL_CFG=agent-otel-sync.yaml ;;
    otel-e2e-stateless)       ARCH=otel; FAULT=none;      AGENT_OTEL_CFG=agent-otel-sync.yaml; GATEWAY_CFG=gateway-stateless.yaml ;;
    otel-e2e-stateless-kill)  ARCH=otel; FAULT=disk-loss; AGENT_OTEL_CFG=agent-otel-sync.yaml; GATEWAY_CFG=gateway-stateless.yaml ;;
    otel-kafka)       ARCH=kafka; FAULT=none ;;
    otel-kafka-kill)  ARCH=kafka; FAULT=disk-loss ;;
    otel-e2e-es-kill)           ARCH=otel;  FAULT=es-kill ;;
    otel-e2e-sync-es-kill)      ARCH=otel;  FAULT=es-kill; AGENT_OTEL_CFG=agent-otel-sync.yaml ;;
    otel-e2e-stateless-es-kill) ARCH=otel;  FAULT=es-kill; AGENT_OTEL_CFG=agent-otel-sync.yaml; GATEWAY_CFG=gateway-stateless.yaml ;;
    otel-kafka-es-kill)         ARCH=kafka; FAULT=es-kill ;;
    *) die "unknown scenario '$s' (otel-e2e | otel-e2e-kill | otel-e2e-sync | otel-e2e-sync-kill | otel-e2e-stateless | otel-e2e-stateless-kill | otel-kafka | otel-kafka-kill | <otel-e2e|otel-e2e-sync|otel-e2e-stateless|otel-kafka>-es-kill)" ;;
  esac
  NAME="$s"
  export AGENT_OTEL_CFG GATEWAY_CFG
  if [ -n "$stress" ]; then
    [ "$FAULT" = disk-loss ] || die "--stress only applies to the -kill scenarios"
    FAULT=disk-loss-es-down; NAME="$s-es-down"
  fi
  export SCENARIO="$ARCH"
}

need_docker() { colima_start; docker info > /dev/null 2>&1 || die "Docker is not running."; }

wait_for() {  # label, command, max attempts (5 s each)
  local label="$1" cmd="$2" max="${3:-60}" i=0 start; start=$(date +%s)
  printf "  %-16s " "$label"
  while [ $i -lt "$max" ]; do
    if bash -c "$cmd" > /dev/null 2>&1; then echo "ready ($(( $(date +%s) - start ))s)"; return 0; fi
    i=$((i+1)); printf "."; sleep 5
  done
  echo " TIMED OUT"; return 1
}

# ---------------------------------------------------------------- up / down
cmd_up() {
  ARCH="${1:-kafka}"; [ "$ARCH" = otel ] || [ "$ARCH" = kafka ] || die "usage: ./demo.sh up <otel|kafka>"
  export SCENARIO="$ARCH"
  KIB=kibana; [ -n "$NO_KIBANA" ] && KIB=""   # experiments (run/matrix) skip Kibana; 'up' keeps it
  need_docker
  say "resetting previous stack (volumes included)"
  $DC --profile otel --profile kafka --profile load down -v --remove-orphans > /dev/null 2>&1
  mkdir -p data/ledger; rm -rf data/ledger/*; touch data/ledger/.gitkeep   # keep the folder (and its placeholder) in git, clear its contents
  $DC --profile "$ARCH" build 2>&1 | tail -2
  if [ "$ARCH" = otel ]; then
    $DC --profile otel up -d elasticsearch es-init $KIB gateway
  else
    $DC --profile kafka up -d elasticsearch es-init $KIB broker-1 broker-2 broker-3 topic-init connect metrics-relay
  fi
  echo; echo "Waiting for services..."
  wait_for "Elasticsearch" "curl -sf http://localhost:9200/_cluster/health" 60 || return 1
  wait_for "ES templates"  "curl -sf http://localhost:9200/_index_template/demo-logs" 30 || return 1
  if [ "$ARCH" = kafka ]; then
    wait_for "Kafka Connect" "curl -sf http://localhost:8083/connectors" 60 || return 1
    deploy_connector || return 1
  fi
  # agents last, so the data sources never meet a half-built pipeline
  $DC --profile "$ARCH" up -d agent-$ARCH-1 agent-$ARCH-2 agent-$ARCH-3
  sleep 4   # fail fast if an Agent cannot start (for example a configuration error), instead of running with dead Agents
  local ag
  for ag in 1 2 3; do
    if ! docker ps --format '{{.Names}}' | grep -q "^agent-$ARCH-$ag$"; then
      echo "ERROR: agent-$ARCH-$ag is not running. Last log lines:"
      docker logs "agent-$ARCH-$ag" 2>&1 | grep -v franz | tail -6
      return 1
    fi
  done
  if [ -z "$NO_KIBANA" ]; then
    wait_for "Kibana" "curl -sf http://localhost:5601/api/status" 60 || true
    for idx in otel-logs kafka-logs; do
      curl -s -X POST localhost:5601/api/data_views/data_view -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
        -d "{\"data_view\":{\"title\":\"$idx\",\"name\":\"$idx\",\"timeFieldName\":\"ingested_at\"},\"override\":true}" > /dev/null || true
    done
  fi
  echo; echo "Stack ready ($ARCH).  Kibana http://localhost:5601   Elastic http://localhost:9200   Connect http://localhost:8083"
}

deploy_connector() {
  curl -s -X DELETE http://localhost:8083/connectors/es-sink-logs > /dev/null 2>&1
  local code i
  for i in $(seq 1 30); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
           --data @connectors/es-sink-logs.json http://localhost:8083/connectors)
    [ "$code" = 201 ] && { echo "  connector es-sink-logs created"; return 0; }
    sleep 3
  done
  echo "ERROR creating connector (HTTP $code)"; return 1
}

cmd_down() {
  ROOT="$PWD"
  docker info > /dev/null 2>&1 && $DC --profile otel --profile kafka --profile load down -v --remove-orphans
  [ -n "$KEEP_COLIMA" ] || colima_stop
}

# ---------------------------------------------------------------- load
start_load() {
  $DC --profile load rm -f generator-1 generator-2 generator-3 > /dev/null 2>&1
  $DC --profile load up -d --build generator-1 generator-2 generator-3 > /dev/null 2>&1
  say "data sources started: rate=$RATE/s (total, 3 sources) duration=${DURATION}s retry=$CLIENT_RETRY batch=$BATCH"
}

# ---------------------------------------------------------------- chaos
wipe() {  # wipe a named volume (its container must be stopped); show what was in it
  docker run --rm -v "otel-kafka_$1:/v" alpine sh -c 'b=$(find /v -type f | wc -l); rm -rf /v/* /v/.[!.]* 2>/dev/null; a=$(find /v -type f | wc -l); echo "  wiped volume '"$1"': files before=$b, after=$a"' 2>/dev/null
}
volume_bytes() { docker run --rm -v "otel-kafka_$1:/v" alpine sh -c 'du -sk /v | cut -f1' 2>/dev/null; }

cmd_chaos() {
  local fault="$1" arg="$2"
  ARCH="${ARCH:-$(docker ps --format '{{.Names}}' | grep -q '^agent-kafka-1$' && echo kafka || echo otel)}"
  case "$fault" in
    disk-loss)        # TOTAL DISK LOSS of the middle tier, Elastic stays healthy. Repeated KILLS times, KILL_GAP seconds apart,
                      # so that some kills land while requests are in flight (one kill is a coin flip).
      local k kills="${KILLS:-5}" gap="${KILL_GAP:-10}"
      for k in $(seq 1 "$kills"); do
        if [ "$ARCH" = otel ]; then
          local c; c=$($DC ps -q gateway | head -1)
          say "CHAOS $k/$kills: gateway queue volume holds $(volume_bytes gateway-queue) KiB; kill -9 gateway AND WIPE ITS DISK (Elastic healthy)"
          docker kill -s KILL "$c" > /dev/null; wipe gateway-queue; docker start "$c" > /dev/null
        else
          say "CHAOS $k/$kills: broker-1 volume holds $(volume_bytes broker-1-data) KiB; kill -9 broker-1 AND WIPE ITS DISK (Elastic healthy)"
          docker kill -s KILL broker-1 > /dev/null; wipe broker-1-data; docker start broker-1 > /dev/null
        fi
        [ "$k" -lt "$kills" ] && sleep "$gap"
      done ;;
    disk-loss-es-down)  # stress test: Elastic down first so a backlog exists
      say "CHAOS: stop elasticsearch (backlog builds upstream)"; docker stop -t 1 elasticsearch > /dev/null; sleep 20
      KILLS=1 cmd_chaos disk-loss
      sleep 20; say "CHAOS: start elasticsearch"; docker start elasticsearch > /dev/null ;;
    agent-disk-loss)
      local hop; if [ "$ARCH" = otel ]; then hop=$($DC ps -q gateway | head -1); else hop=connect; fi
      say "CHAOS: stop next hop ($hop) for 30s"; docker stop -t 1 "$hop" > /dev/null; sleep 10
      say "CHAOS: kill -9 agent-$ARCH-1 AND WIPE ITS DISK"; docker kill -s KILL "agent-$ARCH-1" > /dev/null
      wipe "agent-$ARCH-1-queue"; docker start "agent-$ARCH-1" > /dev/null
      sleep 20; say "CHAOS: start next hop"; docker start "$hop" > /dev/null ;;
    gateway-kill)       # process crash only (disk survives)
      if [ "$ARCH" = otel ]; then
        local c; c=$($DC ps -q gateway | head -1); say "CHAOS: kill -9 gateway"; docker kill -s KILL "$c" > /dev/null
        sleep 10; docker start "$c" > /dev/null
      else say "CHAOS: kill -9 connect"; docker kill -s KILL connect > /dev/null; sleep 10; docker start connect > /dev/null; fi ;;
    broker-kill)  say "CHAOS: kill -9 broker-1"; docker kill -s KILL broker-1 > /dev/null; sleep 30; docker start broker-1 > /dev/null ;;
    two-brokers)  say "CHAOS: kill -9 broker-1 + broker-2"; docker kill -s KILL broker-1 broker-2 > /dev/null; sleep 40; docker start broker-1 broker-2 > /dev/null ;;
    agent-kill)   say "CHAOS: kill -9 agent-$ARCH-1"; docker kill -s KILL "agent-$ARCH-1" > /dev/null; sleep 10; docker start "agent-$ARCH-1" > /dev/null ;;
    es-kill)      # ELASTIC is killed (kill -9) and kept down for ES_DOWN seconds (default 60), then restarted; its data survives
      local down="${ES_DOWN:-60}" t0; t0=$(date +%s)
      say "CHAOS: kill -9 elasticsearch; it stays down for ${down}s, then restarts"
      docker kill -s KILL elasticsearch > /dev/null; sleep "$down"; docker start elasticsearch > /dev/null
      until curl -sf "http://localhost:9200/_cluster/health" > /dev/null 2>&1; do sleep 1; done
      say "CHAOS: elasticsearch answering again; unavailable for $(( $(date +%s) - t0 )) s in total" ;;
    es-outage)    local s="${arg:-120}"; say "CHAOS: stop elasticsearch ${s}s"; docker stop -t 1 elasticsearch > /dev/null; sleep "$s"; docker start elasticsearch > /dev/null ;;
    partition)    local s="${arg:-60}" net
                  net=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "agent-$ARCH-1")
                  say "CHAOS: disconnect agent-$ARCH-1 for ${s}s"; docker network disconnect "$net" "agent-$ARCH-1"; sleep "$s"; docker network connect "$net" "agent-$ARCH-1" ;;
    none|"")      say "CHAOS: no fault" ;;
    *) die "unknown fault '$fault'" ;;
  esac
  say "CHAOS: fault sequence finished"
}

# ---------------------------------------------------------------- run / matrix
cmd_run() {
  local scenario="$1" stress=""; [ "$2" = "--stress" ] && stress=1
  parse_scenario "$scenario" "$stress"
  local label="${NAME}__rate${RATE}__retry-${CLIENT_RETRY}__run$(date +%H%M%S)"
  mkdir -p results
  echo "=== EXPERIMENT $label  (fault '$FAULT' at +${FAULT_AT}s) ==="
  NO_KIBANA=1 cmd_up "$ARCH" > "results/${label}.start.log" 2>&1 || die "stack start failed, see results/${label}.start.log"
  sleep 3
  start_load
  ( sleep "$FAULT_AT"; ARCH="$ARCH" cmd_chaos "$FAULT" ) &
  local chaos_pid=$!
  docker wait generator-1 generator-2 generator-3 > /dev/null
  wait $chaos_pid
  python3 scripts/verify.py --variant "$NAME" --scenario "$ARCH" --label "$label" --wait 600 --stable 10
}

cmd_matrix() {
  local rep s
  for rep in $(seq 1 "${REPS:-3}"); do
    echo "##### repetition $rep"
    for s in otel-e2e otel-e2e-kill otel-e2e-sync otel-e2e-sync-kill otel-e2e-stateless otel-e2e-stateless-kill otel-kafka otel-kafka-kill otel-e2e-es-kill otel-e2e-sync-es-kill otel-e2e-stateless-es-kill otel-kafka-es-kill; do cmd_run "$s"; done
  done
  python3 scripts/report.py
}

# ---------------------------------------------------------------- dispatch
case "$1" in
  up)      cmd_up "$2" ;;
  run)     [ -n "$2" ] || die "usage: ./demo.sh run <scenario> [--stress]"; cmd_run "$2" "$3" ;;
  matrix)  cmd_matrix ;;
  load)    export SCENARIO="${2:-otel}"; start_load ;;
  chaos)   cmd_chaos "$2" "$3" ;;
  verify)  parse_scenario "${2:-otel-e2e}" ""; shift 2; python3 scripts/verify.py --variant "$NAME" --scenario "$ARCH" "$@" ;;
  monitor) python3 scripts/monitor.py "${2:-otel}" ;;
  down)    cmd_down ;;
  report)  python3 scripts/report.py ;;
  *)       sed -n 2,17p "$0" ;;
esac
