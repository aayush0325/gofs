# gofs-operator

A Kubernetes operator for the `gofs` Google File System implementation. It is
scaffolded with **Operator SDK v1.42.3** and reconciles a `GFSCluster` custom
resource into a complete single-master GFS deployment.

## What it creates

For each `GFSCluster` in a namespace the operator manages:

| Resource | Purpose |
| --- | --- |
| `<name>-master` StatefulSet | One metadata master with a BoltDB PVC |
| `<name>-chunkserver` StatefulSet | N chunkservers with stable pod DNS and per-pod PVCs |
| `<name>-master` Service | Internal master HTTP + gRPC endpoints |
| `<name>-client` Service | Client-facing HTTP endpoint, `ClusterIP` by default |
| `<name>-chunkservers` headless Service | Stable StatefulSet DNS for chunk replicas |
| `<name>-tls` Secret | Generated CA, server certificate, and client certificate |
| `<name>-gfs-monitor` ServiceMonitor | Prometheus scrape configuration |
| `<name>-grafana-dashboard` ConfigMap | Grafana dashboard with HTTP, gRPC, and chunk panels |
| `gofs-operator-controller-manager-ui` Service | Operator-hosted React console (port 8090) |

All managed objects receive the `GFSCluster` as controller owner. Deleting the
CR garbage-collects them; a finalizer also removes the StatefulSet-created PVCs.

## Prerequisites

- Go 1.24+ for local operator builds
- Node.js 22+ to build or develop the React console
- Kubernetes 1.27+ with a default StorageClass
- `kubectl`
- Docker for building images
- Optional: Kind for a local cluster
- Optional: Prometheus Operator and Grafana for automatic scraping/dashboards
- Optional: Operator SDK v1.42+ for regeneration

## Deploy with Kind

The repository includes a complete Kind demo in [`../deploy/kind`](../deploy/kind):

```bash
make kind-demo     # from the repository root
```

That creates the cluster, builds/loads the operator and GFS images, installs the
operator, creates a three-chunkserver cluster, and runs an in-cluster smoke test.
The API is exposed at `http://127.0.0.1:8080`.

Manual steps:

From the repository root:

```bash
# Operator image
make operator-docker
kind load docker-image gofs-operator:local

# GFS data-plane images
make docker-build
kind load docker-image gofs-master:local gofs-chunkserver:local

# CRD, RBAC, and manager
make operator-deploy

# Example GFSCluster
kubectl apply -f operator/config/samples/gfs_v1alpha1_gfscluster.yaml
kubectl get gfs -w
```

The sample references `gofs-master:local` and `gofs-chunkserver:local`. Replace
those with registry images for a non-Kind cluster.

The operator runs in `gofs-operator-system`; the GFS cluster runs in the
namespaced location of the CR (`default` in the sample).

## Web UI

The operator image embeds a React + TypeScript console. It is served by the
manager on port `8090` with these features:

- cluster list across namespaces
- ready/cluster-status cards and chunkserver table
- directory browser with breadcrumbs
- create file/folder, rename, recursive delete
- upload and download
- text preview and atomic record append
- copy-on-write snapshots
- forwards the cluster HTTP bearer token when `spec.http.tokenSecretRef` is set
- live HTTP, gRPC, chunk, and heartbeat metrics in the bundled Grafana dashboard

The UI server also exposes:

```text
GET /api/clusters
GET /api/clusters/<namespace>/<name>/v1/...
```

The proxy validates that the `GFSCluster` exists, then forwards to
`<name>-client.<namespace>.svc.cluster.local:8080`. It cannot be used to proxy
arbitrary URLs.

Access it with port forwarding:

```bash
kubectl -n gofs-operator-system port-forward svc/gofs-operator-controller-manager-ui 8090:8090
# open http://127.0.0.1:8090
```

The Kind demo also exposes it at `http://127.0.0.1:8090` using
`deploy/kind/ui-service.yaml`.

Build UI assets locally:

```bash
make web-build          # operator/web/dist
make web-dev            # Vite at :5173, proxies /api to :8090
```

The operator Dockerfile builds the console automatically and copies it to
`/web` in the runtime image.

The console itself has no login screen. The Kind demo exposes it only on
`127.0.0.1`. In a shared cluster, put it behind an authenticating ingress or
disable it with `--ui-bind-address=0` on the manager Deployment.

## Access the GFS API

```bash
kubectl port-forward svc/gfs-client 8080:8080
GFS_SERVER=http://localhost:8080 ./bin/gfs-client status
```

Or from inside the cluster:

```bash
kubectl run curl --rm -it --image=curlimages/curl --restart=Never -- \
  curl -s http://gfs-client.default.svc.cluster.local:8080/v1/status
```

## Monitoring and Grafana

Monitoring is enabled by default. The controller creates:

- a **ServiceMonitor** selecting the master and chunkserver Services through
  the `gfs.gofs.io/scrape=true` label, scraping `/metrics` on the `http` port.
- a **Grafana dashboard ConfigMap** with these panels:
  - HTTP request rate, p95 latency, and 5xx rate
  - gRPC request rate and p95 latency by method/code
  - chunk operation rate by operation/result
  - chunk and HTTP byte throughput
  - chunkserver heartbeat rate
  - process memory
  - target health

Default ServiceMonitor and dashboard labels match common kube-prometheus-stack
and Grafana sidecar conventions:

```yaml
monitoring:
  serviceMonitorLabels:
    release: kube-prometheus-stack
  grafanaDashboardLabels:
    grafana_dashboard: "1"
```

Override either map to match your installation. If the Prometheus Operator CRD
is absent, the dashboard is still created and ServiceMonitor creation is skipped
instead of failing reconciliation. Disable both with:

```yaml
monitoring:
  disabled: true
```

## GFSCluster fields

The full schema is in
[`config/crd/bases/gfs.gofs.io_gfsclusters.yaml`](config/crd/bases/gfs.gofs.io_gfsclusters.yaml).

```yaml
apiVersion: gfs.gofs.io/v1alpha1
kind: GFSCluster
metadata:
  name: gfs
spec:
  image:
    master: gofs-master:local
    chunkserver: gofs-chunkserver:local
    pullPolicy: IfNotPresent
  master:
    storage:
      size: 1Gi
      storageClassName: standard
    workload:
      resources: {}
  chunkservers:
    replicas: 3
    storage:
      size: 10Gi
    capacityBytes: 10737418240
  chunkSizeBytes: 67108864
  replicationFactor: 3
  leaseDuration: 60s
  heartbeatTimeout: 15s
  gcGrace: 1m
  maintenanceInterval: 10s
  tls:
    secretName: ""        # empty generates and owns <name>-tls
  http:
    serviceType: ClusterIP
    tokenSecretRef:
      name: gfs-http-token
      key: token
  monitoring:
    disabled: false
```

Notable rules:

- `replicationFactor` is clamped to `chunkservers.replicas`.
- `chunkSizeBytes` must match across master and chunkserver pods; the operator
  passes the same value to both.
- Storage size changes after creation are rejected by Kubernetes because
  StatefulSet volume claim templates are immutable. Resize the PVCs separately.
- `tls.secretName` lets you supply your own Secret with `ca.crt`, `server.crt`,
  `server.key`, `client.crt`, and `client.key`. The operator will not own or
  rotate a user-provided secret.
- Generated TLS certificates are regenerated when the replica count changes so
  every new StatefulSet pod DNS name is present in the SAN list. A certificate
  hash annotation rolls the StatefulSets.

## Development

```bash
cd operator
make manifests generate   # regenerate CRD, RBAC, deepcopy
make test                 # controller-gen, go vet, envtest
make build                # local manager binary
make docker-build IMG=gofs-operator:local
make build-installer IMG=gofs-operator:local
```

`make test` downloads envtest binaries and validates the controller against a
real API server, including resource creation, finalizer cleanup, TLS generation,
and monitoring ConfigMap creation. E2E tests are skipped unless `E2E_TEST=true`
and a Kind cluster is available.

The all-in-one installer written to [`dist/install.yaml`](dist/install.yaml) is
generated by `make build-installer`.

## Uninstall

```bash
kubectl delete -f config/samples/gfs_v1alpha1_gfscluster.yaml
make undeploy
make uninstall
```

Deleting a `GFSCluster` deletes its data PVCs after the finalizer runs. Take a
backup first if you need the chunk data.

## Layout

```text
api/v1alpha1              GFSCluster types and generated deepcopy
cmd/main.go               Operator SDK manager entry point
internal/controller       reconciliation, TLS, workloads, monitoring
internal/controller/assets/gfs-dashboard.json  Grafana dashboard
internal/ui               React asset server, cluster API, GFS proxy
web                       React + TypeScript console source
config/crd                CRD
config/rbac               generated RBAC
config/manager            manager Deployment
config/samples            example GFSCluster
config/default            kustomize install target
dist/install.yaml         generated all-in-one installer
```
