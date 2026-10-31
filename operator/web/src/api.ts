import type { AppendResult, Cluster, ClusterStatus, FileEntry } from "./types";

// APIError carries the GFS JSON error envelope.
export class APIError extends Error {
  code: string;
  status: number;

  constructor(code: string, status: number, message: string) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

// requestJSON performs a JSON request and translates GFS error envelopes.
async function requestJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  if (!response.ok) {
    let code = `HTTP_${response.status}`;
    let message = response.statusText;
    try {
      const body = (await response.json()) as { error?: { code?: string; message?: string } };
      code = body.error?.code ?? code;
      message = body.error?.message ?? message;
    } catch {
      // Keep the HTTP status text when the body is not JSON.
    }
    throw new APIError(code, response.status, message);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

// listClusters returns all GFSCluster resources managed by the operator.
export async function listClusters(): Promise<Cluster[]> {
  const response = await requestJSON<{ clusters: Cluster[] }>("/api/clusters");
  return response.clusters ?? [];
}

// clusterURL builds a proxied GFS API URL.
function clusterURL(cluster: Cluster, path: string): string {
  return `${cluster.proxyPath}${path}`;
}

// getStatus returns live cluster capacity and placement information.
export async function getStatus(cluster: Cluster): Promise<ClusterStatus> {
  return requestJSON<ClusterStatus>(clusterURL(cluster, "/v1/status"));
}

// listDirectory returns direct children of a directory.
export async function listDirectory(cluster: Cluster, path: string): Promise<FileEntry[]> {
  const query = new URLSearchParams({ path, recursive: "false" });
  const response = await requestJSON<{ entries: FileEntry[] }>(clusterURL(cluster, `/v1/list?${query}`));
  return response.entries ?? [];
}

// mkdir creates a directory.
export async function mkdir(cluster: Cluster, path: string): Promise<FileEntry> {
  return requestJSON<FileEntry>(clusterURL(cluster, "/v1/mkdir"), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ path }),
  });
}

// createFile creates an empty file.
export async function createFile(cluster: Cluster, path: string): Promise<FileEntry> {
  return requestJSON<FileEntry>(clusterURL(cluster, "/v1/create"), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ path, mode: 420 }),
  });
}

// deletePath removes a file or recursively removes a directory.
export async function deletePath(cluster: Cluster, path: string, recursive = true): Promise<void> {
  const query = new URLSearchParams({ path, recursive: String(recursive) });
  await requestJSON<void>(clusterURL(cluster, `/v1/delete?${query}`), { method: "DELETE" });
}

// renamePath moves a file or directory.
export async function renamePath(cluster: Cluster, source: string, destination: string): Promise<void> {
  await requestJSON<void>(clusterURL(cluster, "/v1/rename"), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ source, destination }),
  });
}

// snapshot creates a copy-on-write snapshot.
export async function snapshot(cluster: Cluster, source: string, destination: string): Promise<FileEntry> {
  return requestJSON<FileEntry>(clusterURL(cluster, "/v1/snapshot"), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ source, destination }),
  });
}

// readBlob downloads an entire file.
export async function readBlob(cluster: Cluster, path: string): Promise<Blob> {
  const query = new URLSearchParams({ path });
  const response = await fetch(clusterURL(cluster, `/v1/read?${query}`));
  if (!response.ok) {
    throw new APIError(`HTTP_${response.status}`, response.status, response.statusText);
  }
  return response.blob();
}

// readText returns up to maxBytes of a file for preview.
export async function readText(cluster: Cluster, path: string, maxBytes = 512 * 1024): Promise<string> {
  const query = new URLSearchParams({ path, offset: "0", length: String(maxBytes) });
  const response = await fetch(clusterURL(cluster, `/v1/read?${query}`));
  if (!response.ok) {
    throw new APIError(`HTTP_${response.status}`, response.status, response.statusText);
  }
  return response.text();
}

// overwriteFile deletes an existing file if needed, creates it, and streams data
// into it at offset zero. Deleting first avoids trailing bytes from a larger
// previous file because GFS has no truncate operation.
export async function overwriteFile(cluster: Cluster, path: string, data: Blob): Promise<void> {
  try {
    await deletePath(cluster, path, false);
  } catch (error) {
    if (!(error instanceof APIError) || error.status !== 404) {
      throw error;
    }
  }
  await createFile(cluster, path);
  const query = new URLSearchParams({ path, offset: "0" });
  const response = await fetch(clusterURL(cluster, `/v1/write?${query}`), {
    method: "PUT",
    headers: { "content-type": "application/octet-stream" },
    body: data,
  });
  if (!response.ok) {
    throw new APIError(`HTTP_${response.status}`, response.status, response.statusText);
  }
}

// appendRecord appends one atomic record (up to 4 MiB).
export async function appendRecord(cluster: Cluster, path: string, record: string): Promise<AppendResult> {
  const query = new URLSearchParams({ path });
  return requestJSON<AppendResult>(clusterURL(cluster, `/v1/append?${query}`), {
    method: "POST",
    headers: { "content-type": "application/octet-stream" },
    body: record,
  });
}
