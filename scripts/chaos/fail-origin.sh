#!/usr/bin/env bash
# Make one origin unhealthy and watch it get excluded from the pool.
#
# Retries mask the failure immediately; active health checks then remove the
# origin from selection entirely, after which requests succeed on the first
# attempt.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

TARGET="${1:-origin-1}"
require_cluster

step "Stopping $TARGET"
stopped=0
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^edgemesh-$TARGET$"; then
  docker stop "edgemesh-$TARGET" >/dev/null
  ok "stopped container edgemesh-$TARGET"
  stopped=1
else
  pid="$(pgrep -f "origin-demo .*--id $TARGET" | head -1 || true)"
  if [ -n "$pid" ]; then
    kill -9 "$pid"
    ok "killed local process $pid ($TARGET)"
    stopped=1
  fi
fi
[ "$stopped" = "1" ] || fail "could not locate $TARGET"

step "Sending traffic while the pool converges"
EDGE1="$(first_of "$EDGEMESH_EDGES")"
OK=0; FAILED=0
for i in $(seq 1 60); do
  code="$(edge_status "$EDGE1" "/dynamic/failover-$i")"
  if [ "$code" = "200" ]; then OK=$((OK+1)); else FAILED=$((FAILED+1)); fi
  sleep 0.2
done
info "requests with one origin dead: $OK ok, $FAILED failed"

step "Circuit breaker and origin health metrics"
curl -fsS --max-time 5 http://127.0.0.1:9201/metrics 2>/dev/null \
  | grep -E '^edgemesh_(origin_healthy|circuit_breaker_state|origin_retries_total)' \
  | head -20 || warn "metrics endpoint unavailable at :9201"

cat <<NOTE

  What this demonstrates:
    - retries cover the window before health checks react
    - the dead origin is excluded once it crosses the unhealthy threshold
    - the circuit breaker converts a slow failure into a fast local one

  Restart it with:
    docker start edgemesh-$TARGET     (Compose)
    ./scripts/run-local.sh restart    (local processes)

NOTE
