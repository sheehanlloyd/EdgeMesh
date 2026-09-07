#!/usr/bin/env bash
# Stop an edge node and watch the consistent hash ring converge.
#
# Killing the node that owns a key is the interesting case: requests for that
# key must keep succeeding through a replica or through a direct origin fetch,
# because the cache is never a hard dependency.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

TARGET="${1:-edge-3}"
require_cluster

step "Ring membership before"
ctl nodes

step "Stopping $TARGET"
stopped=0
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^edgemesh-$TARGET$"; then
  docker stop "edgemesh-$TARGET" >/dev/null
  ok "stopped container edgemesh-$TARGET"
  stopped=1
else
  n="${TARGET#edge-}"
  pid="$(pgrep -f "edge --config .*edge-$n.yaml" | head -1 || true)"
  if [ -n "$pid" ]; then
    kill -9 "$pid"
    ok "killed local process $pid ($TARGET)"
    stopped=1
  fi
fi
[ "$stopped" = "1" ] || fail "could not locate $TARGET"

step "Sending traffic through the surviving edges while membership converges"
IFS=',' read -ra ALL_EDGES <<< "$EDGEMESH_EDGES"

# Build the surviving set by probing, rather than by guessing which URL maps to
# the stopped node: the mapping between an edge id and its URL is deployment
# specific, and a wrong guess would silently measure the wrong thing.
SURVIVORS=()
for edge in "${ALL_EDGES[@]}"; do
  if curl -fsS -o /dev/null --max-time 2 "$edge/" 2>/dev/null      || [ "$(edge_status "$edge" "/")" != "000" ]; then
    SURVIVORS+=("$edge")
  fi
done
info "surviving edges: ${SURVIVORS[*]:-none}"
[ "${#SURVIVORS[@]}" -gt 0 ] || fail "no surviving edge is reachable"

OK=0; FAILED=0
COUNTER="chaos-$(date +%s)"
for _ in $(seq 1 20); do
  for edge in "${SURVIVORS[@]}"; do
    code="$(edge_status "$edge" "/counter/$COUNTER")"
    if [ "$code" = "200" ]; then OK=$((OK+1)); else FAILED=$((FAILED+1)); info "  $edge returned $code"; fi
  done
  sleep 0.1
done
info "requests across surviving edges: $OK ok, $FAILED failed"
[ "$FAILED" -eq 0 ] || fail "$FAILED requests failed on the surviving edges after $TARGET stopped"
ok "the survivors served every request while $TARGET was gone"

step "Waiting for the leader to drop $TARGET from the ring"
# The node leaves only after the dead-heartbeat threshold, so this waits rather
# than sampling once.
deadline=$(( $(date +%s) + 30 ))
dropped=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  if ctl nodes 2>/dev/null | awk -v n="$TARGET" '$1 == n {print $2}' | grep -qv healthy; then
    dropped=1
    break
  fi
  # A node removed entirely no longer appears at all.
  if ! ctl nodes 2>/dev/null | awk -v n="$TARGET" '$1 == n {found=1} END {exit !found}'; then
    dropped=1
    break
  fi
  sleep 0.5
done
if [ "$dropped" = "1" ]; then
  ok "$TARGET left the healthy set"
else
  warn "$TARGET is still healthy after 30s; check the membership thresholds"
fi

step "Ring membership after"
ctl nodes

cat <<NOTE

  What this demonstrates:
    - the dead node leaves the ring only after the dead-heartbeat threshold,
      so a transient blip does not reshuffle cache ownership
    - keys the dead node owned are remapped to the surviving nodes
    - requests kept succeeding throughout: a peer failure falls back to a
      replica and then to the origin

  Restart it with:
    docker start edgemesh-$TARGET     (Compose)
    ./scripts/run-local.sh restart    (local processes)

NOTE
