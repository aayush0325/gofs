// API types returned by the operator UI and the GFS HTTP API.

export type Cluster = {
  name: string;
  namespace: string;
  phase: string;
  masterReady: boolean;
  readyChunkservers: number;
  desiredChunkservers: number;
  httpEndpoint: string;
  grpcEndpoint: string;
  proxyPath: string;
};

export type FileEntry = {
  path: string;
  type: "file" | "directory";
  mode: number;
  size: number;
  mtime_unix_nano: number;
  chunk_count: number;
  snapshot_of?: string;
};

export type Chunkserver = {
  id: string;
  address: string;
  capacity_bytes: number;
  used_bytes: number;
  last_seen_unix_nano: number;
  alive: boolean;
  chunk_count: number;
};

export type ClusterStatus = {
  chunk_size_bytes: number;
  replication_factor: number;
  files: number;
  directories: number;
  chunks: number;
  orphaned_chunks: number;
  chunkservers: Chunkserver[];
  started_at_unix_nano: number;
};

export type AppendResult = {
  path: string;
  offset: number;
  bytes_appended: number;
  size: number;
};
