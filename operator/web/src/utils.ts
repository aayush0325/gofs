// Formatting and path helpers shared by the UI components.

// formatBytes returns a compact binary byte size.
export function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const exponent = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** exponent).toFixed(exponent === 0 ? 0 : 1)} ${units[exponent]}`;
}

// formatTime renders a Unix nanosecond timestamp in the browser locale.
export function formatTime(unixNano: number): string {
  if (!unixNano) return "—";
  return new Date(unixNano / 1_000_000).toLocaleString();
}

// formatRate returns a compact per-second rate.
export function formatRate(value: number): string {
  return `${formatBytes(value)}/s`;
}

// joinPath appends a child name to a canonical GFS path.
export function joinPath(base: string, name: string): string {
  return base === "/" ? `/${name}` : `${base}/${name}`;
}

// basename returns the final component of a path.
export function basename(path: string): string {
  if (path === "/" || path === "") return "/";
  return path.slice(path.lastIndexOf("/") + 1);
}

// breadcrumbs splits an absolute path into navigable pieces.
export function breadcrumbs(path: string): { label: string; path: string }[] {
  const parts = path.split("/").filter(Boolean);
  const result = [{ label: "root", path: "/" }];
  let current = "";
  for (const part of parts) {
    current += `/${part}`;
    result.push({ label: part, path: current });
  }
  return result;
}

// parentPath returns the parent of a canonical path.
export function parentPath(path: string): string {
  if (path === "/" || path === "") return "/";
  const index = path.lastIndexOf("/");
  return index <= 0 ? "/" : path.slice(0, index);
}

// isTextLike gives a rough guard for previewing file contents.
export function isTextLike(name: string): boolean {
  const ext = name.split(".").pop()?.toLowerCase() ?? "";
  return [
    "txt", "md", "json", "yaml", "yml", "csv", "log", "go", "ts", "tsx", "js",
    "jsx", "css", "html", "xml", "sh", "py", "toml", "ini", "conf", "sql",
  ].includes(ext);
}
