import { useCallback, useEffect, useMemo, useState } from "react";
import { listClusters } from "./api";
import { ClusterSidebar } from "./ClusterSidebar";
import { Dashboard } from "./Dashboard";
import { FileBrowser } from "./FileBrowser";
import type { Cluster } from "./types";

type Toast = { id: number; message: string; kind: "ok" | "error" };
type View = "files" | "cluster";

// App is the operator console shell: cluster selection, routing, and toasts.
export function App() {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [selectedKey, setSelectedKey] = useState("");
  const [path, setPath] = useState("/");
  const [view, setView] = useState<View>("files");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [refreshKey, setRefreshKey] = useState(0);
  const [toasts, setToasts] = useState<Toast[]>([]);

  const load = useCallback(async () => {
    try {
      const next = await listClusters();
      setClusters(next);
      setError("");
      setSelectedKey((current) => {
        if (current && next.some((cluster) => `${cluster.namespace}/${cluster.name}` === current)) {
          return current;
        }
        return next[0] ? `${next[0].namespace}/${next[0].name}` : "";
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "could not list GFS clusters");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(load, 10000);
    return () => window.clearInterval(timer);
  }, [load]);

  const selected = useMemo(
    () => clusters.find((cluster) => `${cluster.namespace}/${cluster.name}` === selectedKey),
    [clusters, selectedKey],
  );

  const notify = useCallback((message: string, kind: "ok" | "error" = "ok") => {
    const id = Date.now() + Math.random();
    setToasts((current) => [...current, { id, message, kind }]);
    window.setTimeout(() => setToasts((current) => current.filter((toast) => toast.id !== id)), 4500);
  }, []);

  const selectCluster = (cluster: Cluster) => {
    setSelectedKey(`${cluster.namespace}/${cluster.name}`);
    setPath("/");
    setView("files");
  };

  const changed = useCallback(() => setRefreshKey((value) => value + 1), []);

  return (
    <div className="app-shell">
      <ClusterSidebar
        clusters={clusters}
        selected={selected}
        onSelect={selectCluster}
        onRefresh={() => void load()}
        loading={loading}
      />
      <main className="main">
        <header className="topbar">
          <div className="topbar-title">
            <span className="kicker">Kubernetes operator</span>
            <h1>{selected ? `${selected.namespace}/${selected.name}` : "gofs console"}</h1>
            {selected && (
              <span className={`phase-pill phase-${selected.phase?.toLowerCase() || "pending"}`}>
                {selected.phase || "Pending"}
              </span>
            )}
          </div>
          <div className="topbar-actions">
            {selected && (
              <nav className="tabs">
                <button className={view === "files" ? "active" : ""} onClick={() => setView("files")}>Files</button>
                <button className={view === "cluster" ? "active" : ""} onClick={() => setView("cluster")}>Cluster</button>
              </nav>
            )}
            <button className="button ghost" onClick={() => void load()}>Refresh</button>
          </div>
        </header>

        {error && <div className="banner error">{error}</div>}
        {!loading && clusters.length === 0 && (
          <div className="empty-state">
            <div className="empty-glyph">G</div>
            <h2>No GFSCluster resources</h2>
            <p>Create a GFSCluster with kubectl or apply the sample manifest to get started.</p>
            <code>kubectl apply -f deploy/kind/gfscluster.yaml</code>
          </div>
        )}
        {selected && view === "files" && (
          <FileBrowser
            cluster={selected}
            path={path}
            onPathChange={setPath}
            notify={notify}
            onChanged={changed}
          />
        )}
        {selected && view === "cluster" && (
          <Dashboard cluster={selected} refreshKey={refreshKey} />
        )}
      </main>

      <div className="toast-stack">
        {toasts.map((toast) => (
          <div key={toast.id} className={`toast ${toast.kind}`}>{toast.message}</div>
        ))}
      </div>
    </div>
  );
}
