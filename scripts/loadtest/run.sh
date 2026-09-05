#!/usr/bin/env bash
# Run a k6 load-test scenario against a running EdgeMesh cluster.
#
# Usage: ./scripts/loadtest/run.sh [scenario] [vus] [duration]
#   scenarios: l1_hit l2_hit cold_miss coalesce passthrough mixed
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

SCENARIO="${1:-mixed}"
VUS="${2:-50}"
DURATION="${3:-30s}"
TARGET="${TARGET:-$(first_of "$EDGEMESH_EDGES")}"

command -v k6 >/dev/null || fail "k6 is required: brew install k6 (or https://k6.io/docs/get-started/installation/)"
require_cluster

step "Recording the environment"
cat <<ENV
  host          $(uname -s) $(uname -r) $(uname -m)
  cpus          $( (sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null) || echo unknown)
  memory        $( (sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.0f GiB", $1/1024/1024/1024}') || \
                   (grep MemTotal /proc/meminfo 2>/dev/null | awk '{printf "%.0f GiB", $2/1024/1024}') || echo unknown)
  go            $(go version 2>/dev/null || echo "not on PATH")
  k6            $(k6 version 2>/dev/null | head -1)
  target        $TARGET
  scenario      $SCENARIO
  vus           $VUS
  duration      $DURATION

  These values belong in any published benchmark result. A number without its
  hardware is not a measurement.
ENV

step "Warming the cache"
# Cache-hit scenarios must measure hits, not the fill that precedes them.
case "$SCENARIO" in
  l1_hit|l2_hit|mixed)
    for i in $(seq 0 99); do
      curl -fsS -o /dev/null --max-time 5 -H "Host: $EDGEMESH_HOST" \
        "$TARGET/static/hot-object" 2>/dev/null || true
      curl -fsS -o /dev/null --max-time 5 -H "Host: $EDGEMESH_HOST" \
        "$TARGET/static/zipf-$i" 2>/dev/null || true
    done
    ok "cache warmed"
    ;;
  *) info "no warm-up needed for the $SCENARIO scenario" ;;
esac

step "Running k6"
SCENARIO="$SCENARIO" VUS="$VUS" DURATION="$DURATION" TARGET="$TARGET" \
  HOST="$EDGEMESH_HOST" \
  k6 run "$(dirname "${BASH_SOURCE[0]}")/edgemesh.js"

ok "summary written to loadtest-summary.json"
