#!/usr/bin/env bash
# Shared helpers for the EdgeMesh scripts.
#
# Every script sources this so the demo, the chaos scripts, and the load test
# agree on how to find the cluster and how to report success. Nothing here
# claims success without checking: a demo that prints "OK" regardless of what
# happened is worse than no demo.

set -euo pipefail

# Control-plane admin endpoints. Any of them may be addressed; the CLI follows
# leader redirection.
EDGEMESH_ADMIN="${EDGEMESH_ADMIN:-http://127.0.0.1:7101,http://127.0.0.1:7102,http://127.0.0.1:7103}"
EDGEMESH_EDGES="${EDGEMESH_EDGES:-http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083}"
EDGEMESH_ORIGINS="${EDGEMESH_ORIGINS:-http://127.0.0.1:9001,http://127.0.0.1:9002}"
EDGEMESH_HOST="${EDGEMESH_HOST:-demo.edgemesh.local}"
EDGEMESH_ROUTE="${EDGEMESH_ROUTE:-demo-route}"
EDGEMESH_POOL="${EDGEMESH_POOL:-demo-pool}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTL="${EDGEMESH_CTL:-$REPO_ROOT/bin/edgemeshctl}"

BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'
YELLOW=$'\033[33m'; BLUE=$'\033[34m'; RESET=$'\033[0m'

step()  { printf "\n%s==> %s%s\n" "$BOLD$BLUE" "$*" "$RESET"; }
info()  { printf "    %s\n" "$*"; }
ok()    { printf "    %s✓%s %s\n" "$GREEN" "$RESET" "$*"; }
warn()  { printf "    %s!%s %s\n" "$YELLOW" "$RESET" "$*"; }
fail()  { printf "    %s✗%s %s\n" "$RED" "$RESET" "$*" >&2; exit 1; }

# first_of returns the first entry of a comma-separated list.
first_of() { echo "${1%%,*}"; }

# ctl runs edgemeshctl against the configured admin endpoints.
ctl() {
  if [ ! -x "$CTL" ]; then
    fail "edgemeshctl not found at $CTL; run 'make build' first"
  fi
  "$CTL" --server "$EDGEMESH_ADMIN" "$@"
}

# admin_get fetches a path from whichever control node answers first.
#
# It must not address only the first endpoint. The chaos scenarios kill the
# current leader, and that leader is as likely as any other to be the first node
# in the list, so a helper pinned to one address reports the whole cluster as
# gone whenever it happens to pick the node that was just stopped.
admin_get() {
  local path="$1" admin
  local IFS=,
  for admin in $EDGEMESH_ADMIN; do
    if curl -fsS --max-time 5 "$admin$path" 2>/dev/null; then
      return 0
    fi
  done
  return 1
}

# require_cluster fails fast with an actionable message when nothing is running.
require_cluster() {
  if ! admin_get /v1/status >/dev/null 2>&1; then
    fail "no control node answered on $EDGEMESH_ADMIN. Start the cluster first:
      make compose-up     (Docker)
      make run-local      (local processes)"
  fi
}

# json_field extracts a top-level field from a JSON document on stdin.
#
# Empty or non-JSON input yields an empty string rather than a traceback. The
# callers below poll every control node, including ones a chaos scenario has
# just killed, so a failed curl handing this function an empty stdin is normal
# operation and must not print a Python stack trace into the demo output.
json_field() {
  python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if isinstance(d, dict):
    print(d.get(sys.argv[1], ""))
' "$1"
}

# leader_id reports the current Raft leader.
#
# A node that has not yet learned who won an election reports an empty leader,
# so every node is asked until one names a leader. Returning the first empty
# answer would make a completed failover look like an ongoing outage.
leader_id() {
  local admin id
  local IFS=,
  for admin in $EDGEMESH_ADMIN; do
    id="$(curl -fsS --max-time 5 "$admin/v1/status" 2>/dev/null | json_field leader_id || true)"
    if [ -n "$id" ]; then
      echo "$id"
      return 0
    fi
  done
  return 0
}

# raft_term reports the current Raft term.
raft_term() {
  admin_get /v1/status | json_field term
}

# edge_get issues a proxied request and prints the cache outcome header.
# Usage: edge_get <edge-url> <path>
edge_get() {
  curl -fsS --max-time 10 -D /tmp/edgemesh-headers.$$ \
    -H "Host: $EDGEMESH_HOST" "$1$2" -o /tmp/edgemesh-body.$$
  local outcome
  outcome="$(grep -i '^x-edgemesh-cache:' /tmp/edgemesh-headers.$$ | tr -d '\r' | awk '{print $2}')"
  rm -f /tmp/edgemesh-headers.$$ /tmp/edgemesh-body.$$
  echo "${outcome:-NONE}"
}

# edge_status prints the HTTP status of a proxied request without failing.
edge_status() {
  curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
    -H "Host: $EDGEMESH_HOST" "$1$2"
}

# origin_hits sums a counter across every origin.
# Usage: origin_hits <counter-id>
origin_hits() {
  local total=0 url count
  IFS=',' read -ra urls <<< "$EDGEMESH_ORIGINS"
  for url in "${urls[@]}"; do
    count="$(curl -fsS --max-time 3 "$url/stats" 2>/dev/null \
      | python3 -c "import json,sys
try:
    print(json.load(sys.stdin).get('counters',{}).get('$1',0))
except Exception:
    print(0)" 2>/dev/null || echo 0)"
    total=$((total + count))
  done
  echo "$total"
}

# wait_for polls a command until it succeeds or the timeout expires.
# Usage: wait_for <seconds> <description> <command...>
wait_for() {
  local timeout="$1" desc="$2"; shift 2
  local deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  fail "timed out after ${timeout}s waiting for: $desc"
}
