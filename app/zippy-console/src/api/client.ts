export type StatusValue = string | number | boolean | null | undefined;

export interface TableSummary {
  name: string;
  namespace: string;
  catalog: string;
  table_type: string;
  schema?: { fields: SchemaField[] };
  snapshots?: Array<Record<string, StatusValue>>;
}

export function tableKey(table: TableSummary): string {
  return JSON.stringify([table.namespace, table.name]);
}

export function tableLabel(table: TableSummary): string {
  return `${table.namespace} / ${table.name}`;
}

export interface SchemaField {
  name?: string;
  data_type?: string;
  nullable?: StatusValue;
  timezone?: string;
}

export interface TaskInfo {
  name: string;
  kind?: string;
  status?: string;
  process_id?: StatusValue;
  cmd?: string;
  input_stream?: string;
  output_stream?: string;
  metrics?: {
    health?: string;
    restart_count?: StatusValue;
    schedule_state?: string;
    started_at?: string;
    stopped_at?: string;
  };
}

export interface LogEntry {
  time?: string;
  level?: string;
  source?: string;
  message?: string;
}

export interface DashboardPayload {
  generated_at: string;
  webui: {
    pid: number;
    uptime_sec: number;
    host: string;
    port: number;
  };
  catalog: { status: string; error: string | null };
  pm: { status: string; error: string | null };
  totals: { tables: number; namespaces: number; tasks: number };
  tasks: TaskInfo[];
  tables: TableSummary[];
  log_tail: Array<LogEntry | string>;
}

export interface TableDetailPayload {
  ok: boolean;
  table?: TableSummary;
  error?: string;
}

export interface TableDataPayload {
  ok: boolean;
  table_name: string;
  namespace: string;
  schema?: { fields: SchemaField[] };
  snapshots?: Array<Record<string, StatusValue>>;
  mode: "head" | "tail";
  limit: number;
  columns: string[];
  rows: Array<Record<string, StatusValue>>;
  row_count: number;
  truncated: boolean;
  error?: string | null;
}

export async function fetchDashboard(): Promise<DashboardPayload> {
  return fetchJson("/api/dashboard");
}

export async function fetchTableDetail(
  table: TableSummary,
  signal?: AbortSignal,
): Promise<TableDetailPayload> {
  const params = new URLSearchParams({ namespace: table.namespace });
  return fetchJson(`/api/table/${encodeURIComponent(table.name)}?${params}`, signal);
}

export async function fetchTableData(
  table: TableSummary,
  mode: "head" | "tail",
  limit: number,
  orderBy: string[],
  descending: boolean,
  signal?: AbortSignal,
): Promise<TableDataPayload> {
  const params = new URLSearchParams({
    namespace: table.namespace, mode, limit: String(limit), descending: String(descending),
  });
  orderBy.forEach((column) => params.append("order_by", column));
  return fetchJson(`/api/table/${encodeURIComponent(table.name)}/data?${params}`, signal);
}

async function fetchJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { cache: "no-store", signal });
  if (!response.ok) {
    throw new Error(`request failed status=[${response.status}] url=[${url}]`);
  }
  return (await response.json()) as T;
}
