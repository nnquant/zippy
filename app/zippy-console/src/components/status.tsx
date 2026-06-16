import { Badge } from "./ui/badge";

type Tone = "ok" | "warning" | "error" | "muted";

export function statusTone(status: unknown): Tone {
  const normalized = String(status || "").toLowerCase();
  if (["ok", "ready", "registered", "running", "online", "healthy"].includes(normalized)) {
    return "ok";
  }
  if (["warning", "stale", "degraded", "starting", "stopping", "restarting"].includes(normalized)) {
    return "warning";
  }
  if (["error", "failed", "unhealthy", "crashed", "stopped"].includes(normalized)) {
    return "error";
  }
  return "muted";
}

export function StatusBadge({ value }: { value: unknown }): JSX.Element {
  return <Badge tone={statusTone(value)}>{displayValue(value)}</Badge>;
}

export function displayValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return String(value);
}

export function displayNumber(value: unknown): string {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue.toLocaleString() : displayValue(value);
}
