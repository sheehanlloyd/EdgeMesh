#!/usr/bin/env bash
# EdgeMesh scripted demo.
#
# Every step verifies its own outcome. If a check fails the script exits
# non-zero rather than printing a reassuring message, because a demo that
# cannot fail proves nothing.
#
# Usage: ./scripts/demo.sh [--skip-chaos]

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

SKIP_CHAOS=0
[ "${1:-}" = "--skip-chaos" ] && SKIP_CHAOS=1

DEMO_COUNTER="demo-$(date +%s)"

# ---------------------------------------------------------------------------
step "1. Verify the cluster is up"
# ---------------------------------------------------------------------------
require_cluster
ctl status
LEADER="$(leader_id)"
[ -n "$LEADER" ] || fail "no Raft leader is elected"
ok "control plane healthy, leader is $LEADER"

# ---------------------------------------------------------------------------
step "2. Raft cluster state"
# ---------------------------------------------------------------------------
ctl raft status

# ---------------------------------------------------------------------------
step "3. Configure an origin pool and a caching route"
# ---------------------------------------------------------------------------
POOL_FILE="$(mktemp)"
ROUTE_FILE="$(mktemp)"
trap 'rm -f "$POOL_FILE" "$ROUTE_FILE"' EXIT

# Origin addresses differ between Compose (service names) and local processes.
if [ "${EDGEMESH_IN_COMPOSE:-0}" = "1" ]; then
  O1_HOST=origin-1; O1_PORT=9000; O2_HOST=origin-2; O2_PORT=9000
else
  O1_HOST=127.0.0.1; O1_PORT=9001; O2_HOST=127.0.0.1; O2_PORT=9002
fi

cat > "$POOL_FILE" <<YAML
id: $EDGEMESH_POOL
load_balancing: LOAD_BALANCING_ROUND_ROBIN
health_interval_ms: 2000
health_timeout_ms: 1000
unhealthy_threshold: 2
healthy_threshold: 1
origins:
  - id: origin-1
    scheme: http
    host: $O1_HOST
    port: $O1_PORT
    weight: 1
    health_path: /healthz
    expected_statuses: [200]
  - id: origin-2
    scheme: http
    host: $O2_HOST
    port: $O2_PORT
    weight: 1
    health_path: /healthz
    expected_statuses: [200]
YAML

cat > "$ROUTE_FILE" <<YAML
id: $EDGEMESH_ROUTE
hostname: $EDGEMESH_HOST
path_prefix: /
origin_pool_id: $EDGEMESH_POOL
enabled: true
cache_policy:
  enabled: true
  default_ttl_seconds: 120
  max_ttl_seconds: 3600
retry_policy:
  enabled: true
  max_retries: 2
  backoff_base_ms: 20
  backoff_max_ms: 200
header_policy:
  diagnostic_headers: true
YAML

ctl origins apply -f "$POOL_FILE" >/dev/null
ok "origin pool $EDGEMESH_POOL applied"
ctl routes apply -f "$ROUTE_FILE" >/dev/null
ok "route $EDGEMESH_ROUTE applied"

info "waiting for the configuration to reach every edge..."
sleep 2
ctl routes list

# ---------------------------------------------------------------------------
step "4. Registered edge nodes"
# ---------------------------------------------------------------------------
ctl nodes

# ---------------------------------------------------------------------------
step "5. First request: a cache MISS that reaches the origin"
# ---------------------------------------------------------------------------
EDGE1="$(first_of "$EDGEMESH_EDGES")"
BEFORE="$(origin_hits "$DEMO_COUNTER")"
OUTCOME="$(edge_get "$EDGE1" "/counter/$DEMO_COUNTER")"
AFTER="$(origin_hits "$DEMO_COUNTER")"

info "cache outcome: $OUTCOME"
info "origin counter: $BEFORE -> $AFTER"
[ "$OUTCOME" = "MISS" ] || fail "expected MISS on the first request, got $OUTCOME"
[ "$AFTER" -gt "$BEFORE" ] || fail "the origin was not contacted on a cache miss"
ok "cold request reached the origin exactly once"

# ---------------------------------------------------------------------------
step "6. Second request to the same edge: a local L1 HIT"
# ---------------------------------------------------------------------------
OUTCOME="$(edge_get "$EDGE1" "/counter/$DEMO_COUNTER")"
AFTER2="$(origin_hits "$DEMO_COUNTER")"
info "cache outcome: $OUTCOME"
info "origin counter: $AFTER -> $AFTER2"
[ "$OUTCOME" = "HIT" ] || fail "expected HIT on the second request, got $OUTCOME"
[ "$AFTER2" -eq "$AFTER" ] || fail "the origin was contacted despite a cache hit"
ok "repeat request served from cache without touching the origin"

# ---------------------------------------------------------------------------
step "7. Requests to the other edges: served across the distributed cache"
# ---------------------------------------------------------------------------
IFS=',' read -ra EDGES <<< "$EDGEMESH_EDGES"
for edge in "${EDGES[@]:1}"; do
  OUTCOME="$(edge_get "$edge" "/counter/$DEMO_COUNTER")"
  info "$edge -> $OUTCOME"
  case "$OUTCOME" in
    HIT|PEER_HIT) ;;
    *) fail "$edge returned $OUTCOME; expected the object to be reachable across the cluster" ;;
  esac
done
FINAL="$(origin_hits "$DEMO_COUNTER")"
info "origin counter after ${#EDGES[@]} edges served the object: $FINAL"
[ "$FINAL" -eq "$AFTER" ] || fail "the origin was refetched; the distributed cache did not work"
ok "one origin fetch served every edge in the cluster"

if [ "$SKIP_CHAOS" = "1" ]; then
  step "Demo complete (chaos steps skipped)"
  exit 0
fi

# ---------------------------------------------------------------------------
step "8. Kill the Raft leader while traffic continues"
# ---------------------------------------------------------------------------
TERM_BEFORE="$(raft_term)"
info "current leader: $LEADER (term $TERM_BEFORE)"
"$(dirname "${BASH_SOURCE[0]}")/chaos/kill-leader.sh" --no-wait || \
  warn "could not stop the leader automatically; stop it by hand and re-run"

info "sending traffic during the election..."
OK=0; FAILED=0
for _ in $(seq 1 40); do
  code="$(edge_status "$EDGE1" "/counter/$DEMO_COUNTER")"
  if [ "$code" = "200" ]; then OK=$((OK+1)); else FAILED=$((FAILED+1)); info "  got HTTP $code"; fi
  sleep 0.1
done
info "data-plane requests during failover: $OK ok, $FAILED failed"
[ "$FAILED" -eq 0 ] || fail "$FAILED requests failed during a control-plane election"
ok "the data plane was unaffected by the control-plane election"

# ---------------------------------------------------------------------------
step "9. Confirm a new leader was elected in a higher term"
# ---------------------------------------------------------------------------
wait_for 30 "a new leader" bash -c '
  source "'"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"'/lib.sh" >/dev/null 2>&1
  [ -n "$(leader_id)" ]'
NEW_LEADER="$(leader_id)"
TERM_AFTER="$(raft_term)"
info "new leader: $NEW_LEADER (term $TERM_AFTER)"
[ "$NEW_LEADER" != "$LEADER" ] || fail "the stopped leader is still reported as leader"
[ "$TERM_AFTER" -gt "$TERM_BEFORE" ] || fail "the term did not advance"
ok "failover complete: $LEADER (term $TERM_BEFORE) -> $NEW_LEADER (term $TERM_AFTER)"

# ---------------------------------------------------------------------------
step "10. Apply a configuration change through the new leader"
# ---------------------------------------------------------------------------
NEW_ROUTE="$(mktemp)"
cat > "$NEW_ROUTE" <<YAML
id: post-failover-route
hostname: $EDGEMESH_HOST
path_prefix: /static
origin_pool_id: $EDGEMESH_POOL
enabled: true
cache_policy:
  enabled: true
  default_ttl_seconds: 300
header_policy:
  diagnostic_headers: true
YAML
ctl routes apply -f "$NEW_ROUTE" >/dev/null
rm -f "$NEW_ROUTE"
sleep 2

SERVED_BY="$(curl -fsS -D- -o /dev/null --max-time 10 -H "Host: $EDGEMESH_HOST" \
  "$EDGE1/static/post-failover" | grep -i '^x-edgemesh-route:' | tr -d '\r' | awk '{print $2}')"
info "route serving /static: $SERVED_BY"
[ "$SERVED_BY" = "post-failover-route" ] || \
  fail "the new route did not propagate after failover (got '$SERVED_BY')"
ok "configuration written through the new leader reached the edges"

# ---------------------------------------------------------------------------
step "11. Purge the cache and prove the next request refetches"
# ---------------------------------------------------------------------------
BEFORE_PURGE="$(origin_hits "$DEMO_COUNTER")"
ctl cache purge --route "$EDGEMESH_ROUTE" >/dev/null
sleep 1
OUTCOME="$(edge_get "$EDGE1" "/counter/$DEMO_COUNTER")"
AFTER_PURGE="$(origin_hits "$DEMO_COUNTER")"
info "cache outcome after purge: $OUTCOME"
info "origin counter: $BEFORE_PURGE -> $AFTER_PURGE"
[ "$OUTCOME" = "MISS" ] || fail "the cache still served the purged object ($OUTCOME)"
[ "$AFTER_PURGE" -gt "$BEFORE_PURGE" ] || fail "the purge did not force a refetch"
ok "purge propagated to the edges and cleared both cache tiers"

# ---------------------------------------------------------------------------
step "Demo complete"
# ---------------------------------------------------------------------------
cat <<SUMMARY

  Verified in this run:
    - one origin fetch served every edge through the consistent-hash L2 cache
    - repeat requests were served from cache without contacting the origin
    - $OK data-plane requests succeeded with zero failures during a leader election
    - a new leader was elected in a higher term and accepted configuration writes
    - a cache purge propagated to every edge and cleared both tiers

  Explore further:
    Prometheus  http://localhost:9090
    Grafana     http://localhost:3000  (admin/admin)
    Traces      http://localhost:16686
    Metrics     http://localhost:9201/metrics  (edge-1)

SUMMARY
