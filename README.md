# gofs: a Google File System implementation in Go

`gofs` is a working, inspectable implementation of the architecture described in
**The Google File System** (SOSP 2003). It keeps GFS's defining ideas:

- a single metadata **master**
- many **chunkservers** storing 64 MiB chunks
- a **primary lease** protocol for mutations
- **version fencing** and replication
- **record append**
- **copy-on-write snapshots**
- background **re-replication** and lazy **garbage collection**

The paper is checked in at [`docs/gfs-sosp2003.pdf`](docs/gfs-sosp2003.pdf) and
can be downloaded with `make paper`.

## What is implemented

| GFS concept | Implementation |
| --- | --- |
| Single master | `gfs-master`: namespace, chunk metadata, placement, leases |
| Chunkserver | `gfs-chunkserver`: chunk files, checksum sidecars, forwarding |
| Client API | HTTP/JSON on `:8080`; gRPC for internal services |
| Chunk size | 64 MiB by default, configurable |
| Replication | 3 by default; placement by chunk count and capacity |
| Primary lease | Master grants a 60 s lease and advances chunk version |
| Mutation ordering | Primary forwards write/append to secondaries before applying |
| Record append | Atomic up to 4 MiB per request; pads and rolls over chunks |
| Snapshots | Metadata copied in one BoltDB transaction; chunks share until COW |
| Fault detection | Chunkserver heartbeat with local chunk inventory |
| Recovery | Re-replication from live replicas; stale replicas removed |
| Deletion | Namespace delete immediate; orphan chunks GC'd after grace period |
| Integrity | CRC32C per 64 KiB block, verified on every read |
| Transport | Mutual TLS between master and chunkservers |
| Observability | JSON logs, `/healthz`, `/readyz`, Prometheus `/metrics` |
| Kubernetes | Operator SDK `GFSCluster` operator, Prometheus ServiceMonitor, Grafana dashboard |
| Web UI | React console served by the operator image; browse files, upload/download, snapshot, append, and inspect cluster health |

The HTTP API specification is in [`api/openapi.yaml`](api/openapi.yaml), and all
internal RPCs are defined in [`proto/gfs/v1`](proto/gfs/v1).

## Architecture

```text
                         HTTP/JSON (client-facing)
  gfs-client / curl ───────────────────────────────┐
                                                   v
                                           ┌──────────────┐
                                           │  gfs-master  │
                                           │              │
  metadata (BoltDB) <──────────────────────│ namespace    │
  chunk + server records                   │ leases       │
                                           │ placement    │
                                           │ GC / repair  │
                                           └──────┬───────┘
                                                  │ mTLS gRPC
                    ┌─────────────────────────────┼─────────────────────────────┐
                    v                             v                             v
             ┌──────────────┐              ┌──────────────┐              ┌──────────────┐
             │ chunkserver1 │              │ chunkserver2 │              │ chunkserver3 │
             │ data+CRC     │              │ data+CRC     │              │ data+CRC     │
             └──────────────┘              └──────────────┘              └──────────────┘
                    ^                             ^                             ^
                    └──────── data gRPC (primary/secondary) ──────────────────┘
```

The master never carries file data. The HTTP layer acts as an in-process GFS
client: it asks the master for chunk locations and leases, then streams data
directly to the lease primary over mTLS gRPC.

### Mutation flow

1. Client asks the master for the chunk at a file offset.
2. Master checks the lease. If there is no valid lease it picks a live replica
   as primary, increments the chunk version, and fences replicas at that version.
3. If the chunk is shared with a snapshot (`RefCount > 1`), the master copies it
   to a new handle first (copy-on-write) and replaces the file's reference.
4. Client sends the mutation to the primary with a unique mutation ID.
5. Primary forwards the same mutation to secondaries and waits.
6. Primary applies locally, reports the committed size/version to the master,
   and acknowledges the client.
7. Retries reuse the mutation ID. Chunkservers keep a bounded persistent
   idempotency cache, so an append is not applied twice.

### Snapshots

`POST /v1/snapshot` duplicates the namespace subtree in a single BoltDB
transaction and increments every referenced chunk's `RefCount`. No file data is
copied. The first write or append to either the source or the snapshot sees
`RefCount > 1` and creates a private copy through `CopyChunk`, preserving the
other version.

### Record append

A record append is one 4 MiB-or-smaller request. If it fits in the last chunk,
the primary chooses the append offset and forwards the same offset to all
secondaries. If it does not fit, the HTTP client API zero-pads the remainder of
the current chunk and retries the whole record in a fresh chunk, keeping records
atomic and chunk offsets aligned.

## Requirements

- Go 1.23+
- OpenSSL (for local certificates)
- Optional: `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` to regenerate RPCs
- Optional: Docker + Docker Compose for the three-node demo
- Optional: Kubernetes + `kubectl` + `kind` for the operator demo
- Optional: Node.js 22+ for building the React console
- Optional: `operator-sdk` v1.42+ when regenerating operator scaffolding

Generated protobuf code is committed, so `protoc` is not required just to build.

## Quick start with Docker Compose

The Compose project lives in [`deploy/demo/docker-compose.yml`](deploy/demo/docker-compose.yml).

```bash
export COMPOSE_FILE=deploy/demo/docker-compose.yml
make demo          # docker compose -f $COMPOSE_FILE up --build
```

This starts one master, three chunkservers, and generates certificates in a
Docker volume. In another terminal:

```bash
export COMPOSE_FILE=deploy/demo/docker-compose.yml
docker compose --profile tools run --rm --no-deps client mkdir /data
docker compose --profile tools run --rm --no-deps client create /data/hello
echo "hello GFS" | docker compose --profile tools run --rm --no-deps -T client write /data/hello -
docker compose --profile tools run --rm --no-deps client read /data/hello
docker compose --profile tools run --rm --no-deps client stat /data/hello
docker compose --profile tools run --rm --no-deps client snapshot /data/hello /data/hello.snap
docker compose --profile tools run --rm --no-deps client status
```

Stop and remove the cluster with:

```bash
docker compose down
```

## Kubernetes operator

The [`operator/`](operator) directory is an Operator SDK project that reconciles
a namespaced `GFSCluster` custom resource into:

- a master StatefulSet and PVC
- a chunkserver StatefulSet with per-pod PVCs and stable DNS names
- internal master, client HTTP, and headless chunkserver Services
- generated mTLS credentials in a Secret
- a Prometheus `ServiceMonitor`
- a Grafana dashboard ConfigMap for the GFS metrics

The one-command Kind demo builds and loads the images, creates a Kind cluster,
installs the operator, creates a three-chunkserver cluster, and runs an
in-cluster smoke test:

```bash
make kind-demo
```

The demo files live in [`deploy/kind`](deploy/kind):

- `kind-config.yaml` — Kind cluster with host port 8080 mapped to NodePort 30080
- `operator.yaml` — generated all-in-one operator install
- `gfscluster.yaml` — three-chunkserver demo CR
- `ui-service.yaml` — NodePort Service exposing the operator web console
- `smoke-test.yaml` — Kubernetes Job that writes, appends, reads, and prints status
- `demo.sh` / `cleanup.sh`

The console and API are then available at:

```text
Web UI:  http://127.0.0.1:8090
GFS API: http://127.0.0.1:8080
```

```bash
export GFS_SERVER=http://127.0.0.1:8080
./bin/gfs-client status
./bin/gfs-client read /demo/hello
```

The React console can browse directories, create files/folders, upload and
download files, preview and append records, create snapshots, and view live
chunkserver/capacity metrics. It is served by the operator manager and talks to
each `GFSCluster` through an in-cluster proxy that validates the GFSCluster resource first.

For a manual or non-Kind deployment:

```bash
make operator-docker
make operator-deploy
kubectl apply -f operator/config/samples/gfs_v1alpha1_gfscluster.yaml
kubectl get gfs -w
```

```bash
kubectl port-forward svc/gfs-client 8080:8080
```

The operator creates the dashboard ConfigMap
`<cluster>-grafana-dashboard` with the standard Grafana sidecar label
`grafana_dashboard: "1"`. It creates a `ServiceMonitor` named
`<cluster>-gfs-monitor`; set `spec.monitoring.serviceMonitorLabels` to match your
Prometheus Operator selector. If the Prometheus Operator CRD is not installed,
the dashboard is still created and the ServiceMonitor is skipped. Set
`spec.monitoring.disabled: true` to turn both off.

The generated all-in-one installer is
[`operator/dist/install.yaml`](operator/dist/install.yaml). See
[`operator/README.md`](operator/README.md) for operator development, CRD fields,
and uninstall instructions.

The master HTTP API is also exposed at `http://localhost:8080`.

## Build and run locally

```bash
make certs
make build

# Terminal 1: master
./bin/gfs-master \
  --data-dir=./data/master \
  --grpc-listen=:9090 \
  --http-listen=:8080 \
  --replication=3

# Terminals 2-4: three chunkservers
./bin/gfs-chunkserver --id=cs1 --grpc-listen=:9091 --http-listen=:8081 \
  --advertise=localhost:9091 --data-dir=./data/cs1 --master=localhost:9090
./bin/gfs-chunkserver --id=cs2 --grpc-listen=:9092 --http-listen=:8082 \
  --advertise=localhost:9092 --data-dir=./data/cs2 --master=localhost:9090
./bin/gfs-chunkserver --id=cs3 --grpc-listen=:9093 --http-listen=:8083 \
  --advertise=localhost:9093 --data-dir=./data/cs3 --master=localhost:9090
```

Use the CLI:

```bash
./bin/gfs-client mkdir /data
./bin/gfs-client create /data/hello
echo "hello GFS" | ./bin/gfs-client write /data/hello -
./bin/gfs-client read /data/hello
./bin/gfs-client append /data/hello <<< "a second record"
./bin/gfs-client stat /data/hello
./bin/gfs-client list /data
./bin/gfs-client snapshot /data/hello /data/hello.snap
```

`make certs` creates `certs/ca.crt`, shared server credentials, and client
credentials. The server certificate includes SANs for `localhost`,
`127.0.0.1`, `master`, `gfs-master`, `gfs-chunkserver`, and
`chunkserver-1..3`. For other hostnames, set `--tls-server-name` or regenerate
the certificate with the right SANs.

## HTTP API

All paths are absolute. Errors use a stable envelope:

```json
{"error":{"code":"NOT_FOUND","message":"not found: /missing","request_id":"..."}}
```

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Master readiness |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/v1/status` | Cluster status |
| `POST` | `/v1/mkdir` | `{"path":"/data","recursive":true}` |
| `POST` | `/v1/create` | `{"path":"/data/file","mode":420}` |
| `GET` | `/v1/stat?path=/data/file` | Metadata and size |
| `GET` | `/v1/list?path=/data&recursive=false` | Directory entries |
| `DELETE` | `/v1/delete?path=/data/file&recursive=false` | Delete |
| `POST` | `/v1/rename` | `{"source":"/a","destination":"/b"}` |
| `POST` | `/v1/snapshot` | `{"source":"/a","destination":"/a.snap"}` |
| `GET` | `/v1/read?path=/data/file&offset=0&length=1024` | Stream bytes |
| `PUT` | `/v1/write?path=/data/file&offset=0` | Stream overwrite |
| `POST` | `/v1/append?path=/data/file` | Atomic record append, max 4 MiB |
| `GET` | `/v1/locations?path=/data/file&offset=0&access=read` | Chunk routing |

Examples:

```bash
curl -s -X POST localhost:8080/v1/mkdir \
  -H 'content-type: application/json' -d '{"path":"/data","recursive":true}'

curl -s -X POST localhost:8080/v1/create \
  -H 'content-type: application/json' -d '{"path":"/data/file"}'

curl -s -X PUT 'localhost:8080/v1/write?path=/data/file&offset=0' \
  --data-binary @README.md

curl -s 'localhost:8080/v1/read?path=/data/file' -o copy.md

curl -s -X POST 'localhost:8080/v1/append?path=/data/file' \
  --data-binary 'one atomic record'
```

`GET /v1/locations` returns protojson. With `access=write` or `access=append`
it has side effects: it acquires a lease and may perform copy-on-write.

## gRPC services

- `gfs.v1.Master` — registration, heartbeat, chunk locations, metadata ops,
  reports, snapshots, and cluster status.
- `gfs.v1.ChunkServer` — create/copy/read/write/append/delete/check plus version
  fencing.

Both servers require client certificates signed by the configured CA. The
master's gRPC service listens on `:9090`; each chunkserver on its configured
`--grpc-listen`. Reflection is enabled on the master, so tools such as `grpcurl`
work when given the client certificate.

## Configuration

Every flag can also be set through the listed environment variable.

### Master

| Flag | Environment | Default |
| --- | --- | --- |
| `--grpc-listen` | `GFS_MASTER_GRPC_ADDR` | `:9090` |
| `--http-listen` | `GFS_MASTER_HTTP_ADDR` | `:8080` |
| `--data-dir` | `GFS_MASTER_DATA_DIR` | `./data/master` |
| `--chunk-size` | `GFS_CHUNK_SIZE` | `67108864` |
| `--replication` | `GFS_REPLICATION` | `3` |
| `--lease-duration` | `GFS_LEASE_DURATION` | `60s` |
| `--heartbeat-timeout` | `GFS_HEARTBEAT_TIMEOUT` | `15s` |
| `--gc-grace` | `GFS_GC_GRACE` | `1m` |
| `--maintenance-interval` | `GFS_MAINTENANCE_INTERVAL` | `10s` |
| `--http-token` | `GFS_HTTP_TOKEN` | empty (no auth) |
| `--insecure` | `GFS_INSECURE` | `false` |

### Chunkserver

| Flag | Environment | Default |
| --- | --- | --- |
| `--id` | `GFS_CHUNKSERVER_ID` | hostname |
| `--grpc-listen` | `GFS_CHUNKSERVER_GRPC_ADDR` | `:9091` |
| `--http-listen` | `GFS_CHUNKSERVER_HTTP_ADDR` | `:8081` |
| `--advertise` | `GFS_CHUNKSERVER_ADVERTISE` | localhost + port |
| `--master` | `GFS_MASTER_ADDRESS` | `localhost:9090` |
| `--data-dir` | `GFS_CHUNKSERVER_DATA_DIR` | `./data/chunkserver` |
| `--capacity` | `GFS_CHUNKSERVER_CAPACITY` | `100 GiB` |
| `--chunk-size` | `GFS_CHUNK_SIZE` | `64 MiB` |
| `--heartbeat-interval` | `GFS_HEARTBEAT_INTERVAL` | `5s` |

### TLS and client

| Flag | Environment | Default |
| --- | --- | --- |
| `--tls-ca` | `GFS_TLS_CA` | `./certs/ca.crt` |
| `--tls-cert` | `GFS_TLS_CERT` | `./certs/server.crt` |
| `--tls-key` | `GFS_TLS_KEY` | `./certs/server.key` |
| `--tls-client-cert` | `GFS_TLS_CLIENT_CERT` | `./certs/client.crt` |
| `--tls-client-key` | `GFS_TLS_CLIENT_KEY` | `./certs/client.key` |
| `--tls-server-name` | `GFS_TLS_SERVER_NAME` | empty (use dial host) |
| `--server` (CLI) | `GFS_SERVER` | `http://localhost:8080` |
| `--token` (CLI) | `GFS_HTTP_TOKEN` | empty |

`--insecure` exists for tests and local debugging only. The default path is
mutual TLS.

## Project layout

```text
cmd/gfs-master         master binary: gRPC, HTTP API, metadata, GC, repair
cmd/gfs-chunkserver    chunkserver binary: storage, replication, heartbeats
cmd/gfs-client         HTTP CLI
internal/master        namespace, leases, snapshots, placement, HTTP
internal/chunkserver   data-plane RPCs, forwarding, registration
internal/chunkstore    chunk files, CRC32C blocks, idempotent mutations
internal/metadata      BoltDB namespace and chunk records
internal/chunkclient   cached mTLS gRPC pool and data helpers
internal/api           HTTP client used by the CLI
proto/gfs/v1           protobuf definitions
api/openapi.yaml       HTTP API contract
deploy/demo            Docker Compose demo
deploy/kind            Kind cluster + operator demo
deploy/docker-entrypoint.sh  container volume-prep entrypoint
operator               Operator SDK project for Kubernetes
operator/api           GFSCluster CRD types
operator/internal/controller  reconciliation, TLS, monitoring, workloads
operator/internal/ui   React asset server and GFS reverse proxy
operator/web           React + TypeScript console source
operator/config        CRD, RBAC, manager, samples
operator/dist/install.yaml  generated all-in-one installer
scripts/gen-certs.sh   local CA and mTLS certificates
docs/gfs-sosp2003.pdf  the original GFS paper
```

No source file is longer than roughly 300 lines; related operations are split
across focused files with function-level documentation.

## Tests

```bash
make test
make vet
```

The suite covers:

- metadata path validation and BoltDB transactions
- chunk checksums, corruption detection, mutation idempotency, version fences
- master placement, leases, report handling, chunk rollover, snapshots, GC
- an in-process end-to-end test with one master, three chunkservers, HTTP API,
  multi-chunk writes, append padding, snapshot isolation, and readback

## Design notes and limitations

- This is a single-master implementation, as in the paper. It does not implement
  master shadowing or automatic failover.
- HTTP metadata operations are serialized by the master mutex; fine for a demo
  and correctness testing, but not intended as a high-throughput production
  master.
- The HTTP API is not encrypted and is unauthenticated by default. Set
  `--http-token` and place it behind a TLS terminator for any non-local use.
  Internal master/chunkserver gRPC is mTLS.
- The same server certificate is used by the master and chunkservers. A
  production deployment should issue per-node certificates and rotate them.
- Writes are atomic per chunk block, not per whole HTTP request. A failed large
  write can leave earlier blocks committed.
- Record append over HTTP is capped at 4 MiB by the gRPC transfer limit. Larger
  records would require a streaming RPC.
- Checksums are verified on reads and via the internal `CheckChunk` RPC; a
  periodic background scrubber is not included.
- The GC grace period defaults to one minute rather than GFS's three days so the
  demo is observable.
- No POSIX permissions enforcement is performed; modes are metadata only.
