#!/usr/bin/env bash
# Create a kind cluster, load the EdgeMesh images, and install the chart.
#
# Everything here is local: no registry, no cloud, no cost.

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CLUSTER="${1:-edgemesh}"
NAMESPACE="${NAMESPACE:-edgemesh}"
RELEASE="${RELEASE:-edgemesh}"

GREEN=$'\033[32m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
step() { printf "\n%s==> %s%s\n" "$BOLD" "$*" "$RESET"; }
ok()   { printf "  %s✓%s %s\n" "$GREEN" "$RESET" "$*"; }

for tool in kind kubectl helm docker; do
  command -v "$tool" >/dev/null || { echo "$tool is required; run 'make bootstrap'" >&2; exit 1; }
done

step "Creating the kind cluster '$CLUSTER'"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  ok "cluster already exists"
else
  kind create cluster --name "$CLUSTER" --config deploy/kind/kind-config.yaml
  ok "cluster created"
fi

step "Building images"
docker build -q -f Dockerfile.control -t edgemesh/control:dev --build-arg VERSION=dev . >/dev/null
docker build -q -f Dockerfile.edge    -t edgemesh/edge:dev    --build-arg VERSION=dev . >/dev/null
docker build -q -f Dockerfile.origin  -t edgemesh/origin:dev  --build-arg VERSION=dev . >/dev/null
ok "images built"

step "Loading images into kind"
# Loading directly avoids needing a registry for a local cluster.
kind load docker-image --name "$CLUSTER" edgemesh/control:dev edgemesh/edge:dev edgemesh/origin:dev
ok "images loaded"

step "Installing the chart"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
helm upgrade --install "$RELEASE" deploy/helm/edgemesh \
  --namespace "$NAMESPACE" \
  --values deploy/kind/values-kind.yaml \
  --wait --timeout 5m
ok "chart installed"

step "Waiting for the control plane to elect a leader"
kubectl -n "$NAMESPACE" rollout status "statefulset/$RELEASE-control" --timeout=180s
for _ in $(seq 1 60); do
  leader="$(kubectl -n "$NAMESPACE" exec "$RELEASE-control-0" -c control -- \
    wget -qO- "http://127.0.0.1:7100/v1/status" 2>/dev/null \
    | sed -n 's/.*"leader_id":"\([^"]*\)".*/\1/p' || true)"
  if [ -n "$leader" ]; then
    ok "leader elected: $leader"
    break
  fi
  sleep 2
done

step "Cluster state"
kubectl -n "$NAMESPACE" get pods,svc

cat <<NOTE

${BOLD}EdgeMesh is running in kind.${RESET}

  Admin API:
    kubectl -n $NAMESPACE port-forward svc/$RELEASE-control 7100:7100
    ./bin/edgemeshctl status --server http://127.0.0.1:7100

  Edge proxy:
    kubectl -n $NAMESPACE port-forward svc/$RELEASE-edge 8080:80
    curl -H 'Host: demo.edgemesh.local' http://127.0.0.1:8080/static/hello

  Tear down:
    make kind-down

NOTE
