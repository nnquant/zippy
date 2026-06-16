export type StatusValue = string | number | boolean | null | undefined;

export interface AlertInfo {
  severity?: string;
  kind?: string;
  message?: string;
  summary?: string;
  reason?: string;
  table_name?: string;
}

export interface TableSummary {
  stream_name: string;
  status?: string;
  health_status?: string;
  schema_hash?: string;
  writer_process_id?: string;
  writer_epoch?: StatusValue;
  descriptor_generation?: StatusValue;
  row_count?: StatusValue;
  active_rows?: StatusValue;
  reader_count?: StatusValue;
  sealed_count?: StatusValue;
  persisted_count?: StatusValue;
  persist_event_count?: StatusValue;
  alerts?: AlertInfo[];
  fields?: SchemaField[];
  schema?: {
    fields?: SchemaField[];
  };
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
  master: {
    status?: string;
    error?: string | null;
  };
  overall_status?: string;
  gateway?: {
    status?: string;
    healthy?: boolean;
  };
  gateway_status?: string;
  totals: {
    tables: number;
    tasks: number;
    alerts: number;
    readers: number;
    sealed_segments: number;
    persisted_files: number;
  };
  tasks: TaskInfo[];
  tables: TableSummary[];
  stale_error_tables: TableSummary[];
  log_tail: Array<LogEntry | string>;
}

export interface TableDetailPayload {
  ok: boolean;
  table?: TableSummary;
  health?: {
    status?: string;
    alerts?: AlertInfo[];
  };
  error?: string;
}

export interface TableDataPayload {
  ok: boolean;
  table_name: string;
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

export async function fetchTableDetail(tableName: string): Promise<TableDetailPayload> {
  return fetchJson(`/api/table/${encodeURIComponent(tableName)}`);
}

export async function fetchTableData(
  tableName: string,
  mode: "head" | "tail",
  limit: number,
): Promise<TableDataPayload> {
  const params = new URLSearchParams({
    mode,
    limit: String(limit),
  });
  return fetchJson(`/api/table/${encodeURIComponent(tableName)}/data?${params.toString()}`);
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`request failed status=[${response.status}] url=[${url}]`);
  }
  return (await response.json()) as T;
}
