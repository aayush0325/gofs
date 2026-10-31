import { useEffect, useState } from "react";
import { getStatus } from "./api";
import type { Cluster, ClusterStatus } from "./types";
import { formatBytes, formatTime } from "./utils";

type Props = {
  cluster: Cluster;
  refreshKey: number;
  onStatus?: (status: ClusterStatus) => void;
};

// Dashboard shows live cluster capacity, readiness, and chunkserver placement.
export function Dashboard({ cluster, refreshKey, onStatus }: Props) {
  const [status, setStatus] = useState<ClusterStatus>();
  const [error, setError] = useState("");
  const [updated, setUpdated] = useState<number>(0);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const next = await getStatus(cluster);
        if (!cancelled) {
          setStatus(next);
          setError("");
          setUpdated(Date.now());
          onStatus?.(next);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "status request failed");
        }
      }
    };
    void load();
    const timer = window.setInterval(load, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [cluster, refreshKey, onStatus]);

  if (error) {
    return <div className="panel error-panel">Could not read cluster status: {error}</div>;
  }

  const totalCapacity = status?.chunkservers.reduce((sum, server) => sum + server.capacity_bytes, 0) ?? 0;
  const totalUsed = status?.chunkservers.reduce((sum, server) => sum + server.used_bytes, 0) ?? 0;
  const live = status?.chunkservers.filter((server) => server.alive).length ?? 0;

  return (
    <section className="dashboard-grid">
      <div className="stat-card accent">
        <div className="stat-label">Cluster phase</div>
        <div className={`stat-value phase-text-${cluster.phase?.toLowerCase() || "pending"}`}>{cluster.phase || "Pending"}</div>
        <div className="stat-foot">master {cluster.masterReady ? "ready" : "not ready"}</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Files</div>
        <div className="stat-value">{status?.files ?? "—"}</div>
        <div className="stat-foot">{status?.directories ?? "—"} directories</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Chunks</div>
        <div className="stat-value">{status?.chunks ?? "—"}</div>
        <div className="stat-foot">{status?.orphaned_chunks ?? 0} orphaned</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Chunkservers</div>
        <div className="stat-value">
          {live}/{status?.chunkservers.length ?? cluster.desiredChunkservers}
        </div>
        <div className="stat-foot">replication ×{status?.replication_factor ?? "—"}</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Chunk size</div>
        <div className="stat-value small">{status ? formatBytes(status.chunk_size_bytes) : "—"}</div>
        <div className="stat-foot">GFS block unit</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Stored data</div>
        <div className="stat-value small">{formatBytes(totalUsed)}</div>
        <div className="stat-foot">of {formatBytes(totalCapacity)}</div>
      </div>

      <div className="panel chunk-panel">
        <div className="panel-title">
          <div>
            <h2>Chunkservers</h2>
            <p>Live replicas registered with the master</p>
          </div>
          <span className="muted mono">{updated ? `updated ${new Date(updated).toLocaleTimeString()}` : "loading"}</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Status</th>
                <th>Chunks</th>
                <th>Used</th>
                <th>Capacity</th>
                <th>Last heartbeat</th>
              </tr>
            </thead>
            <tbody>
              {(status?.chunkservers ?? []).map((server) => (
                <tr key={server.id}>
                  <td className="mono">{server.id}</td>
                  <td>
                    <span className={`badge ${server.alive ? "badge-ok" : "badge-bad"}`}>
                      {server.alive ? "alive" : "offline"}
                    </span>
                  </td>
                  <td>{server.chunk_count}</td>
                  <td>{formatBytes(server.used_bytes)}</td>
                  <td>{formatBytes(server.capacity_bytes)}</td>
                  <td className="muted">{formatTime(server.last_seen_unix_nano)}</td>
                </tr>
              ))}
              {(!status || status.chunkservers.length === 0) && (
                <tr>
                  <td colSpan={6} className="muted">
                    Waiting for chunkservers…
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="panel detail-panel">
        <div className="panel-title">
          <div>
            <h2>Endpoints</h2>
            <p>All traffic inside the Kubernetes cluster</p>
          </div>
        </div>
        <dl className="endpoint-list">
          <div>
            <dt>HTTP API</dt>
            <dd className="mono">{cluster.httpEndpoint || "pending"}</dd>
          </div>
          <div>
            <dt>gRPC</dt>
            <dd className="mono">{cluster.grpcEndpoint || "pending"}</dd>
          </div>
          <div>
            <dt>Namespace</dt>
            <dd className="mono">{cluster.namespace}</dd>
          </div>
          <div>
            <dt>GFSCluster</dt>
            <dd className="mono">{cluster.name}</dd>
          </div>
        </dl>
      </div>
    </section>
  );
}
