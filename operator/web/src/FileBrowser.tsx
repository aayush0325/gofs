import { useEffect, useMemo, useRef, useState } from "react";
import {
  createFile, deletePath, listDirectory, mkdir, readBlob, renamePath, snapshot, overwriteFile,
} from "./api";
import { InputDialog } from "./InputDialog";
import { PreviewModal } from "./PreviewModal";
import type { Cluster, FileEntry } from "./types";
import { basename, breadcrumbs, formatBytes, formatTime, isTextLike, joinPath, parentPath } from "./utils";

type Props = {
  cluster: Cluster;
  path: string;
  onPathChange: (path: string) => void;
  notify: (message: string, kind?: "ok" | "error") => void;
  onChanged: () => void;
};

type DialogState =
  | { kind: "mkdir" | "file" | "snapshot" }
  | { kind: "rename"; file: FileEntry }
  | undefined;

// FileBrowser provides namespace navigation, uploads/downloads, and mutations.
export function FileBrowser({ cluster, path, onPathChange, notify, onChanged }: Props) {
  const [entries, setEntries] = useState<FileEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [dialog, setDialog] = useState<DialogState>();
  const [preview, setPreview] = useState<FileEntry>();
  const [refreshKey, setRefreshKey] = useState(0);
  const uploadRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    listDirectory(cluster, path)
      .then((next) => {
        if (cancelled) return;
        setEntries(
          [...next].sort((a, b) => {
            if (a.type !== b.type) return a.type === "directory" ? -1 : 1;
            return basename(a.path).localeCompare(basename(b.path));
          }),
        );
        setError("");
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "list failed");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [cluster, path, refreshKey]);

  const refresh = () => {
    setRefreshKey((value) => value + 1);
    onChanged();
  };

  const crumbs = useMemo(() => breadcrumbs(path), [path]);

  const download = async (file: FileEntry) => {
    try {
      const blob = await readBlob(cluster, file.path);
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = basename(file.path);
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      notify(err instanceof Error ? err.message : "download failed", "error");
    }
  };

  const remove = async (file: FileEntry) => {
    if (!window.confirm(`Delete ${file.path}?`)) return;
    try {
      await deletePath(cluster, file.path, true);
      notify(`deleted ${file.path}`);
      refresh();
    } catch (err) {
      notify(err instanceof Error ? err.message : "delete failed", "error");
    }
  };

  const upload = async (file: File) => {
    const destination = joinPath(path, file.name);
    try {
      await overwriteFile(cluster, destination, file);
      notify(`uploaded ${destination}`);
      refresh();
    } catch (err) {
      notify(err instanceof Error ? err.message : "upload failed", "error");
    }
  };

  const runDialog = async (value: string) => {
    if (!dialog) return;
    if (dialog.kind === "mkdir") {
      const destination = value.startsWith("/") ? value : joinPath(path, value);
      await mkdir(cluster, destination);
      notify(`created directory ${destination}`);
    } else if (dialog.kind === "file") {
      const destination = value.startsWith("/") ? value : joinPath(path, value);
      await createFile(cluster, destination);
      notify(`created file ${destination}`);
    } else if (dialog.kind === "snapshot") {
      const destination = value.startsWith("/") ? value : joinPath(path, value);
      await snapshot(cluster, path, destination);
      notify(`snapshot created at ${destination}`);
    } else if (dialog.kind === "rename") {
      const destination = value.startsWith("/") ? value : joinPath(parentPath(dialog.file.path), value);
      await renamePath(cluster, dialog.file.path, destination);
      notify(`renamed to ${destination}`);
    }
    refresh();
  };

  return (
    <section className="panel file-panel">
      <div className="browser-toolbar">
        <div className="breadcrumbs">
          <button
            className="icon-button"
            title="Parent directory"
            disabled={path === "/"}
            onClick={() => onPathChange(parentPath(path))}
          >
            ↑
          </button>
          {crumbs.map((crumb, index) => (
            <span key={crumb.path} className="crumb-wrap">
              {index > 0 && <span className="crumb-sep">/</span>}
              <button
                className={`crumb ${crumb.path === path ? "current" : ""}`}
                onClick={() => onPathChange(crumb.path)}
              >
                {crumb.label}
              </button>
            </span>
          ))}
        </div>
        <div className="toolbar-actions">
          <button className="button ghost" onClick={() => setDialog({ kind: "mkdir" })}>New folder</button>
          <button className="button ghost" onClick={() => setDialog({ kind: "file" })}>New file</button>
          <button className="button ghost" onClick={() => uploadRef.current?.click()}>Upload</button>
          <button
            className="button ghost"
            disabled={path === "/"}
            title={path === "/" ? "Snapshot a child path, not root" : "Copy-on-write snapshot"}
            onClick={() => setDialog({ kind: "snapshot" })}
          >
            Snapshot
          </button>
          <button className="button ghost" onClick={refresh}>Refresh</button>
          <input
            ref={uploadRef}
            type="file"
            hidden
            onChange={(event) => {
              const selected = event.target.files?.[0];
              if (selected) void upload(selected);
              event.target.value = "";
            }}
          />
        </div>
      </div>

      {error && <div className="inline-error">{error}</div>}
      <div className="table-wrap file-table">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Size</th>
              <th>Chunks</th>
              <th>Modified</th>
              <th className="actions-column">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={6} className="muted">Loading…</td></tr>
            )}
            {!loading && entries.length === 0 && (
              <tr><td colSpan={6} className="muted">This directory is empty.</td></tr>
            )}
            {entries.map((entry) => (
              <tr
                key={entry.path}
                className={entry.type === "directory" ? "clickable" : ""}
                onDoubleClick={() => entry.type === "directory" && onPathChange(entry.path)}
              >
                <td>
                  <button
                    className="file-name"
                    onClick={() => {
                      if (entry.type === "directory") onPathChange(entry.path);
                      else if (isTextLike(entry.path)) setPreview(entry);
                      else void download(entry);
                    }}
                  >
                    <span className={`file-icon ${entry.type}`}>{entry.type === "directory" ? "▸" : "·"}</span>
                    {basename(entry.path)}
                  </button>
                  {entry.snapshot_of && <span className="snapshot-tag">snapshot</span>}
                </td>
                <td className="muted">{entry.type}</td>
                <td>{entry.type === "file" ? formatBytes(entry.size) : "—"}</td>
                <td>{entry.type === "file" ? entry.chunk_count : "—"}</td>
                <td className="muted">{formatTime(entry.mtime_unix_nano)}</td>
                <td className="row-actions">
                  {entry.type === "file" && (
                    <button
                      className="link-button"
                      onClick={() => isTextLike(entry.path) ? setPreview(entry) : void download(entry)}
                    >
                      {isTextLike(entry.path) ? "Open" : "Download"}
                    </button>
                  )}
                  {entry.type === "file" && <button className="link-button" onClick={() => void download(entry)}>Download</button>}
                  <button className="link-button" onClick={() => setDialog({ kind: "rename", file: entry })}>Rename</button>
                  <button className="link-button danger" onClick={() => void remove(entry)}>Delete</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {dialog && (
        <InputDialog
          title={dialog.kind === "mkdir" ? "New folder" : dialog.kind === "file" ? "New file" : dialog.kind === "snapshot" ? "Create snapshot" : "Rename"}
          label={dialog.kind === "snapshot" ? "Snapshot destination" : "Name or absolute path"}
          initial={dialog.kind === "snapshot" ? `${path}.snap` : dialog.kind === "rename" ? basename(dialog.file.path) : ""}
          submitLabel={dialog.kind === "snapshot" ? "Snapshot" : dialog.kind === "rename" ? "Rename" : "Create"}
          onSubmit={runDialog}
          onClose={() => setDialog(undefined)}
        />
      )}

      {preview && (
        <PreviewModal
          cluster={cluster}
          file={preview}
          onClose={() => setPreview(undefined)}
          notify={notify}
          onChanged={refresh}
        />
      )}
    </section>
  );
}
