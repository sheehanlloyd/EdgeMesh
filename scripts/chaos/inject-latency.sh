#!/usr/bin/env bash
# Inject latency and packet loss between the edges and an origin using
# Toxiproxy, then remove it again.
#
# Toxiproxy is used rather than tc/netem so the scenario is portable and needs
# no elevated privileges.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

TOXI="${TOXIPROXY_URL:-http://127.0.0.1:8474}"
PROXY_NAME="origin-1-slow"
LATENCY_MS="${1:-500}"
JITTER_MS="${2:-100}"

if ! curl -fsS --max-time 3 "$TOXI/version" >/dev/null 2>&1; then
  fail "Toxiproxy is not reachable at $TOXI. Start the Compose topology first."
fi

step "Creating a proxy in front of origin-1"
curl -fsS -X POST "$TOXI/proxies" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$PROXY_NAME\",\"listen\":\"0.0.0.0:9003\",\"upstream\":\"origin-1:9000\",\"enabled\":true}" \
  >/dev/null 2>&1 || info "proxy already exists"

step "Adding ${LATENCY_MS}ms latency (±${JITTER_MS}ms)"
curl -fsS -X POST "$TOXI/proxies/$PROXY_NAME/toxics" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"latency\",\"type\":\"latency\",\"stream\":\"downstream\",
       \"attributes\":{\"latency\":$LATENCY_MS,\"jitter\":$JITTER_MS}}" >/dev/null
ok "latency toxic applied"

cat <<NOTE

  Point a route at origin host 'toxiproxy' port 9003 to send traffic through
  the degraded link, then watch:

    - edgemesh_origin_request_duration_seconds  rises
    - edgemesh_cache_fill_duration_seconds      rises
    - request deadlines are still respected; no goroutine leak

  Remove the fault with:
    curl -X DELETE $TOXI/proxies/$PROXY_NAME/toxics/latency

  Remove the proxy entirely with:
    curl -X DELETE $TOXI/proxies/$PROXY_NAME

NOTE
