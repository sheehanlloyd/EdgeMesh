#!/usr/bin/env bash
# Run the whole EdgeMesh topology as local processes, without Docker.
#
# This is the fastest path from a clean checkout to a working cluster, and the
# one the unit and integration suites mirror. Logs land in .run/logs so a
# failure can be diagnosed after the fact.

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RUN_DIR="$REPO_ROOT/.run"
LOG_DIR="$RUN_DIR/logs"
PID_FILE="$RUN_DIR/pids"
BIN_DIR="$REPO_ROOT/bin"

GREEN=$'\033[32m'; RED=$'\033[31m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
ok()   { printf "  %s✓%s %s\n" "$GREEN" "$RESET" "$*"; }
fail() { printf "  %s✗%s %s\n" "$RED" "$RESET" "$*" >&2; exit 1; }

start() {
  [ -x "$BIN_DIR/control" ] || fail "binaries not found; run 'make build' first"

  mkdir -p "$LOG_DIR" "$REPO_ROOT/.data"
  : > "$PID_FILE"

  printf "%sStarting EdgeMesh locally%s\n" "$BOLD" "$RESET"

  # Origins first: the edges health-check them as soon as a pool is configured.
  for n in 1 2; do
    "$BIN_DIR/origin-demo" --addr "127.0.0.1:900$n" --id "origin-$n" \
      > "$LOG_DIR/origin-$n.log" 2>&1 &
    echo "$! origin-$n" >> "$PID_FILE"
    ok "origin-$n on 127.0.0.1:900$n"
  done

  # Control plane. A quorum has to form before the edges can register.
  for n in 1 2 3; do
    "$BIN_DIR/control" --config "configs/local/control-$n.yaml" \
      > "$LOG_DIR/cp-$n.log" 2>&1 &
    echo "$! cp-$n" >> "$PID_FILE"
    ok "cp-$n  raft 700$n / admin 710$n / edge 720$n"
  done

  printf "  waiting for a Raft leader"
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 1 http://127.0.0.1:7101/v1/status 2>/dev/null \
       | grep -q '"leader_id":"cp-'; then
      printf "\n"
      ok "leader elected"
      break
    fi
    printf "."
    sleep 0.5
  done

  for n in 1 2 3; do
    "$BIN_DIR/edge" --config "configs/local/edge-$n.yaml" \
      > "$LOG_DIR/edge-$n.log" 2>&1 &
    echo "$! edge-$n" >> "$PID_FILE"
    ok "edge-$n  proxy 808$n / peer 730$n / metrics 920$n"
  done

  sleep 2
  cat <<INFO

${BOLD}EdgeMesh is running.${RESET}

  Admin API      http://127.0.0.1:7101  (any control node; the CLI follows redirects)
  Edge proxies   http://127.0.0.1:8081  8082  8083
  Origins        http://127.0.0.1:9001  9002
  Metrics        http://127.0.0.1:9201/metrics  (edge-1)
  Logs           $LOG_DIR

  Next:
    ./bin/edgemeshctl status
    make demo
    ./scripts/run-local.sh stop

INFO
}

stop() {
  if [ ! -f "$PID_FILE" ]; then
    echo "nothing to stop (no $PID_FILE)"
    return 0
  fi
  printf "%sStopping EdgeMesh%s\n" "$BOLD" "$RESET"
  # SIGTERM first so the graceful drain path is exercised rather than skipped.
  while read -r pid name; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      ok "signalled $name ($pid)"
    fi
  done < "$PID_FILE"

  sleep 2
  while read -r pid name; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
      echo "  forced $name ($pid) after the drain timeout"
    fi
  done < "$PID_FILE"

  rm -f "$PID_FILE"
  ok "stopped"
}

status() {
  for n in 1 2 3; do
    printf "cp-%s: " "$n"
    curl -fsS --max-time 2 "http://127.0.0.1:710$n/v1/status" 2>/dev/null || echo "unreachable"
    echo
  done
  for n in 1 2 3; do
    printf "edge-%s readiness: " "$n"
    curl -fsS --max-time 2 "http://127.0.0.1:920$n/readyz" 2>/dev/null || echo "unreachable"
    echo
  done
}

case "${1:-start}" in
  start)   start ;;
  stop)    stop ;;
  restart) stop; sleep 1; start ;;
  status)  status ;;
  *) echo "usage: $0 {start|stop|restart|status}" >&2; exit 2 ;;
esac
