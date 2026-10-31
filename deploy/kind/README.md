# Kind demo

A self-contained Kubernetes demo for the gofs operator.

## Files

| File | Purpose |
| --- | --- |
| `kind-config.yaml` | Kind cluster with host `127.0.0.1:8080` mapped to NodePort `30080` |
| `operator.yaml` | Generated Operator SDK all-in-one install: CRD, RBAC, manager UI |
| `ui-service.yaml` | NodePort Service exposing the operator web console on host port 8090 |
| `gfscluster.yaml` | Three-chunkserver `GFSCluster` using the local images and NodePort `30080` |
| `smoke-test.yaml` | Kubernetes Job that mkdirs, creates, writes, appends, reads, and prints cluster status |
| `demo.sh` | Builds/loads images, creates Kind, installs the operator, creates GFS, runs smoke test |
| `cleanup.sh` | Deletes the Kind cluster |

## Quick start

From the repository root:

```bash
make kind-demo
```

This creates a Kind cluster named `gofs-demo`, builds and loads:

```text
gofs-operator:local
gofs-master:local
gofs-chunkserver:local
gofs-client:local
```

It then installs the operator, waits for the `GFSCluster` to become `Ready`, and
runs the in-cluster smoke test.

The web console and API are exposed at:

```text
Web UI:  http://127.0.0.1:8090
GFS API: http://127.0.0.1:8080
```

The React console can browse the namespace, upload/download files, create
folders/files, rename, delete, preview and append text records, take snapshots,
and inspect live chunkserver metrics.

Try it with the host CLI:

```bash
export GFS_SERVER=http://127.0.0.1:8080
./bin/gfs-client status
./bin/gfs-client read /demo/hello
./bin/gfs-client list /demo
```

## Inspect the demo

```bash
kubectl get gfs
kubectl get pods
kubectl get pvc
kubectl get svc
kubectl get servicemonitor gfs-gfs-monitor # if the Prometheus Operator CRD is installed
kubectl get configmap gfs-grafana-dashboard
kubectl -n gofs-operator-system get svc gofs-operator-controller-manager-ui
```

The operator creates a Grafana dashboard ConfigMap labelled
`grafana_dashboard: "1"`. If the Prometheus Operator `ServiceMonitor` CRD is
installed, it also creates `gfs-gfs-monitor`; otherwise it skips the
ServiceMonitor and still creates the dashboard.

## Manual installation

If the images are already in the Kind node:

```bash
kubectl apply -f deploy/kind/operator.yaml
kubectl apply -f deploy/kind/ui-service.yaml
kubectl apply -f deploy/kind/gfscluster.yaml
kubectl wait --for=condition=Ready gfscluster/gfs --timeout=300s
kubectl apply -f deploy/kind/smoke-test.yaml
kubectl logs job/gfs-demo-smoke
```

## Refresh operator.yaml

`deploy/kind/operator.yaml` is a copy of `operator/dist/install.yaml`. After
changing CRDs, RBAC, or manager manifests, regenerate it:

```bash
make kind-manifests
```

## Cleanup

```bash
make kind-down
# or
./deploy/kind/cleanup.sh
```

## Notes

- Host ports `8080` and `8090` must be free. Change the mappings in
  `kind-config.yaml`, `ui-service.yaml`, and `spec.http.nodePort` in
  `gfscluster.yaml` together if needed.
- Docker should be usable without `sudo` (add your user to the `docker` group).
  Running `sudo make kind-demo` also works: the script locates tools in the
  invoking user's `~/.local/bin` and writes the Kind kubeconfig to that user's
  home so `kubectl` continues to work without sudo.
- The CRD and operator image reference `gofs-operator:local`; the demo script
  loads it into Kind with `imagePullPolicy: IfNotPresent`.
- The client API is intentionally plain HTTP for the demo. Use `spec.http.tokenSecretRef`
  for bearer-token auth in shared clusters.
- The web console has no login screen and is exposed only on `127.0.0.1` here.
  Put it behind an authenticating ingress in shared clusters, or disable it on
  the operator manager with `--ui-bind-address=0`.
