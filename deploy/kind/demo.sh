#!/usr/bin/env bash
# Build the images, create a Kind cluster, install the operator, create a
# GFSCluster, and run an in-cluster smoke test.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Make tools installed in the invoking user's ~/.local/bin discoverable when
# the demo is launched through sudo.
CALLER_HOME="${HOME}"
if [ -n "${SUDO_USER:-}" ]; then
  CALLER_HOME="$(getent passwd "$SUDO_USER" 2>/dev/null | cut -d: -f6 || true)"
  if [ -z "$CALLER_HOME" ]; then
    CALLER_HOME="$HOME"
  fi
fi
for tool_dir in "$CALLER_HOME/.local/bin" "/usr/local/bin" "/snap/bin"; do
  if [ -d "$tool_dir" ]; then
    case ":$PATH:" in
      *":$tool_dir:"*) ;;
      *) PATH="$tool_dir:$PATH" ;;
    esac
  fi
done
export PATH

# Keep the Kind kubeconfig in the invoking user's home when run via sudo so the
# cluster remains usable without sudo after the script finishes.
if [ -n "${SUDO_USER:-}" ] && [ -z "${KUBECONFIG:-}" ]; then
  KUBECONFIG="$CALLER_HOME/.kube/config"
  export KUBECONFIG
  mkdir -p "$CALLER_HOME/.kube"
  trap 'chown "$SUDO_USER" "$CALLER_HOME/.kube" "$KUBECONFIG" 2>/dev/null || true' EXIT
fi

CLUSTER="${KIND_CLUSTER:-gofs-demo}"
NAMESPACE="${GFS_NAMESPACE:-default}"
KIND_CONFIG="$ROOT/deploy/kind/kind-config.yaml"
OPERATOR_YAML="$ROOT/deploy/kind/operator.yaml"

for command in docker kind kubectl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "error: $command is required" >&2
    exit 1
  fi
done

if ! kind get clusters | grep -qx "$CLUSTER"; then
  echo "==> creating Kind cluster $CLUSTER"
  kind create cluster --name "$CLUSTER" --config "$KIND_CONFIG" --wait 120s
else
  echo "==> reusing Kind cluster $CLUSTER"
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

echo "==> building operator and GFS images"
(cd "$ROOT/operator" && make docker-build IMG=gofs-operator:local)
docker build -f "$ROOT/Dockerfile.master" -t gofs-master:local "$ROOT"
docker build -f "$ROOT/Dockerfile.chunkserver" -t gofs-chunkserver:local "$ROOT"
docker build -f "$ROOT/Dockerfile.client" -t gofs-client:local "$ROOT"

echo "==> loading images into Kind"
kind load docker-image \
  gofs-operator:local gofs-master:local gofs-chunkserver:local gofs-client:local \
  --name "$CLUSTER"

echo "==> installing the operator"
kubectl apply -f "$OPERATOR_YAML"
kubectl wait --for=condition=Established crd/gfsclusters.gfs.gofs.io --timeout=90s
kubectl -n gofs-operator-system rollout status deploy/gofs-operator-controller-manager --timeout=180s
kubectl apply -f "$ROOT/deploy/kind/ui-service.yaml"

echo "==> creating GFSCluster"
kubectl apply -f "$ROOT/deploy/kind/gfscluster.yaml"
kubectl wait --for=condition=Ready "gfscluster/gfs" -n "$NAMESPACE" --timeout=300s

echo "==> running in-cluster smoke test"
kubectl delete job gfs-demo-smoke -n "$NAMESPACE" --ignore-not-found >/dev/null
kubectl apply -f "$ROOT/deploy/kind/smoke-test.yaml"
if ! kubectl wait --for=condition=complete job/gfs-demo-smoke -n "$NAMESPACE" --timeout=180s; then
  kubectl logs job/gfs-demo-smoke -n "$NAMESPACE" || true
  exit 1
fi
kubectl logs job/gfs-demo-smoke -n "$NAMESPACE"

cat <<EOF

GFS is ready.

  Web UI:     http://127.0.0.1:8090
  GFS API:    http://127.0.0.1:8080
  Status:     kubectl get gfs
  Dashboard:  kubectl get configmap gfs-grafana-dashboard
  Prometheus: kubectl get servicemonitor gfs-gfs-monitor (if the CRD is installed)

Try the CLI from another terminal:

  GFS_SERVER=http://127.0.0.1:8080 ./bin/gfs-client status
  GFS_SERVER=http://127.0.0.1:8080 ./bin/gfs-client read /demo/hello

Cleanup:

  ./deploy/kind/cleanup.sh

EOF
