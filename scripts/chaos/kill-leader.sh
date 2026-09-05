#!/usr/bin/env bash
# Stop the current Raft leader and observe failover.
#
# The point of this scenario is the separation of failure domains: the control
# plane loses its leader and elects a new one while the data plane serves
# traffic uninterrupted.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

WAIT=1
[ "${1:-}" = "--no-wait" ] && WAIT=0

require_cluster
LEADER="$(leader_id)"
TERM_BEFORE="$(raft_term)"
[ -n "$LEADER" ] || fail "no leader to kill"

step "Stopping the Raft leader $LEADER (term $TERM_BEFORE)"

stopped=0
# Docker Compose deployment.
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^edgemesh-$LEADER$"; then
  docker stop "edgemesh-$LEADER" >/dev/null
  ok "stopped container edgemesh-$LEADER"
  stopped=1
else
  # Local process deployment.
  n="${LEADER#cp-}"
  pid="$(pgrep -f "control --config .*control-$n.yaml" | head -1 || true)"
  if [ -n "$pid" ]; then
    kill -9 "$pid"
    ok "killed local process $pid ($LEADER)"
    stopped=1
  fi
fi
[ "$stopped" = "1" ] || fail "could not locate $LEADER as a container or a local process"

if [ "$WAIT" = "0" ]; then
  exit 0
fi

step "Waiting for a new leader"
deadline=$(( $(date +%s) + 30 ))
NEW_LEADER=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  candidate="$(leader_id 2>/dev/null || true)"
  if [ -n "$candidate" ] && [ "$candidate" != "$LEADER" ]; then
    NEW_LEADER="$candidate"
    break
  fi
  sleep 0.2
done
[ -n "$NEW_LEADER" ] || fail "no new leader was elected within 30s"

TERM_AFTER="$(raft_term)"
ok "new leader $NEW_LEADER at term $TERM_AFTER (was $LEADER at term $TERM_BEFORE)"
[ "$TERM_AFTER" -gt "$TERM_BEFORE" ] || fail "the term did not advance across failover"

step "Confirming the cluster is writable again"
ctl raft status

cat <<'NOTE'

  To restore the stopped node:
    docker start edgemesh-<node>        (Compose)
    ./scripts/run-local.sh start        (local processes)

  It rejoins as a follower and catches up from the leader's log.

NOTE
