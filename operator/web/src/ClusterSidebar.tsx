import type { Cluster } from "./types";

type Props = {
  clusters: Cluster[];
  selected?: Cluster;
  onSelect: (cluster: Cluster) => void;
  onRefresh: () => void;
  loading: boolean;
};

// ClusterSidebar lists GFSClusters visible to the operator and their readiness.
export function ClusterSidebar({ clusters, selected, onSelect, onRefresh, loading }: Props) {
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark">G</div>
        <div>
          <div className="brand-name">gofs</div>
          <div className="brand-subtitle">console</div>
        </div>
      </div>

      <div className="sidebar-heading">
        <span>Clusters</span>
        <button className="icon-button" title="Refresh clusters" onClick={onRefresh} disabled={loading}>
          ↻
        </button>
      </div>

      <div className="cluster-list">
        {clusters.length === 0 && <div className="empty-note">No GFSCluster resources found.</div>}
        {clusters.map((cluster) => {
          const active = selected?.namespace === cluster.namespace && selected?.name === cluster.name;
          return (
            <button
              key={`${cluster.namespace}/${cluster.name}`}
              className={`cluster-item ${active ? "active" : ""}`}
              onClick={() => onSelect(cluster)}
            >
              <span className={`phase-dot phase-${cluster.phase?.toLowerCase() || "pending"}`} />
              <span className="cluster-text">
                <span className="cluster-name">{cluster.name}</span>
                <span className="cluster-meta">
                  {cluster.namespace} · {cluster.readyChunkservers}/{cluster.desiredChunkservers} chunkservers
                </span>
              </span>
              <span className="cluster-phase">{cluster.phase || "Pending"}</span>
            </button>
          );
        })}
      </div>

      <div className="sidebar-footer">
        <span>Operator value from Kubernetes</span>
        <span className="mono">gfs.gofs.io/v1alpha1</span>
      </div>
    </aside>
  );
}
