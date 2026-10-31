import { useEffect, useState } from "react";
import { appendRecord, readText } from "./api";
import type { Cluster, FileEntry } from "./types";
import { basename, formatBytes } from "./utils";

type Props = {
  cluster: Cluster;
  file: FileEntry;
  onClose: () => void;
  onChanged: () => void;
  notify: (message: string, kind?: "ok" | "error") => void;
};

// PreviewModal shows text content and supports appending an atomic record.
export function PreviewModal({ cluster, file, onClose, onChanged, notify }: Props) {
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [record, setRecord] = useState("");
  const [appending, setAppending] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    readText(cluster, file.path)
      .then((text) => {
        if (!cancelled) {
          setContent(text);
          setError("");
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "read failed");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [cluster, file.path]);

  const append = async () => {
    if (!record) return;
    setAppending(true);
    try {
      const result = await appendRecord(cluster, file.path, record.endsWith("\n") ? record : `${record}\n`);
      notify(`appended ${result.bytes_appended} bytes at offset ${result.offset}`);
      setRecord("");
      const text = await readText(cluster, file.path);
      setContent(text);
      onChanged();
    } catch (err) {
      notify(err instanceof Error ? err.message : "append failed", "error");
    } finally {
      setAppending(false);
    }
  };

  return (
    <div className="modal-backdrop" onMouseDown={onClose}>
      <div className="modal preview-modal" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <div>
            <h2>{basename(file.path)}</h2>
            <p className="muted mono">{file.path}</p>
          </div>
          <button className="icon-button" onClick={onClose} title="Close">
            ×
          </button>
        </div>
        <div className="preview-meta">
          <span>{formatBytes(file.size)}</span>
          <span>{file.chunk_count} chunk(s)</span>
          {file.snapshot_of && <span>snapshot of {file.snapshot_of}</span>}
        </div>
        <pre className="preview-content">
          {loading ? "loading…" : error ? `error: ${error}` : content || "(empty file)"}
        </pre>
        <div className="append-row">
          <textarea
            value={record}
            onChange={(event) => setRecord(event.target.value)}
            placeholder="Append one atomic record…"
            rows={3}
          />
          <button className="button primary" onClick={append} disabled={appending || record.length === 0}>
            {appending ? "Appending…" : "Append record"}
          </button>
        </div>
      </div>
    </div>
  );
}
