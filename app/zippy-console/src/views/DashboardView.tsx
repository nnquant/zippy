import {
  ChevronRight,
  ListChecks,
  PenLine,
  Server,
  Table2,
  UsersRound,
} from "lucide-react";

import type { DashboardPayload, LogEntry, TableSummary, TaskInfo } from "../api/client";
import { MessageCell } from "../components/MessageCell";
import { MetricCard } from "../components/MetricCard";
import { displayNumber, displayValue, StatusBadge, statusTone } from "../components/status";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { DataTable } from "../components/ui/table";

interface DashboardViewProps {
  data: DashboardPayload;
  onSelectTable: (tableName: string) => void;
}

export function DashboardView({
  data,
  onSelectTable,
}: DashboardViewProps): JSX.Element {
  const writers = new Set(data.tables.map((table) => table.writer_process_id).filter(Boolean)).size;
  const overallStatus = data.overall_status || data.master.status || "unknown";
  const overallTone = statusTone(overallStatus);
  const overallHealthy = overallTone === "ok";

  return (
    <div className="dashboard-stack">
      <div className="metric-grid">
        <MetricCard
          icon={Server}
          label="Status"
          tone={statusTone(overallStatus)}
          value={statusMetricValue(data, overallStatus, overallHealthy)}
        />
        <MetricCard icon={Table2} label="Tables" value={displayNumber(data.totals.tables)} />
        <MetricCard icon={UsersRound} label="Readers" value={displayNumber(data.totals.readers)} />
        <MetricCard icon={PenLine} label="Writers" value={displayNumber(writers)} />
        <MetricCard icon={ListChecks} label="Processes" value={displayNumber(data.totals.tasks)} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Tables</CardTitle>
        </CardHeader>
        <CardContent className="table-card-content">
          <div className="tables-table-wrap">
            <DataTable className="dashboard-table tables-table">
              <thead>
                <tr>
                  <th className="table-column">Table</th>
                  <th className="status-column">Status</th>
                  <th className="message-column">Message</th>
                  <th className="writer-column">Writer</th>
                  <th className="epoch-column">Writer Epoch</th>
                  <th className="numeric-column">Readers</th>
                  <th className="numeric-column">Sealed</th>
                  <th className="numeric-column">Persisted</th>
                  <th className="action-column" aria-label="Open table detail" />
                </tr>
              </thead>
              <tbody>
                {data.tables.length ? (
                  data.tables.map((table) => (
                    <TableRow key={table.stream_name} onSelectTable={onSelectTable} table={table} />
                  ))
                ) : (
                  <tr>
                    <td colSpan={9}>
                      <EmptyState title="No tables" />
                    </td>
                  </tr>
                )}
              </tbody>
            </DataTable>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Processes</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable className="dashboard-table">
            <thead>
              <tr>
                <th>Task</th>
                <th>Mode</th>
                <th>Status</th>
                <th>PID</th>
                <th>Health</th>
                <th>Restarts</th>
                <th>Schedule / Start</th>
              </tr>
            </thead>
            <tbody>
              {data.tasks.length ? (
                data.tasks.slice(0, 80).map((task) => <TaskRow key={task.name} task={task} />)
              ) : (
                <tr>
                  <td colSpan={7}>
                    <EmptyState title="No processes" />
                  </td>
                </tr>
              )}
            </tbody>
          </DataTable>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Alerts</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable className="dashboard-table">
            <thead>
              <tr>
                <th style={{ width: "14%" }}>Time</th>
                <th style={{ width: "12%" }}>Level</th>
                <th style={{ width: "18%" }}>Source</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {alertLogs(data).length ? (
                alertLogs(data).map((row, index) => <LogRow key={`${row.time}-${index}`} row={row} />)
              ) : (
                <tr>
                  <td colSpan={4}>
                    <EmptyState title="No Alerts" />
                  </td>
                </tr>
              )}
            </tbody>
          </DataTable>
        </CardContent>
      </Card>
    </div>
  );
}

function TableRow({
  onSelectTable,
  table,
}: {
  onSelectTable: (tableName: string) => void;
  table: TableSummary;
}): JSX.Element {
  return (
    <tr className="clickable" onClick={() => onSelectTable(table.stream_name)}>
      <td className="strong table-name-cell" title={table.stream_name}>
        {table.stream_name}
      </td>
      <td>
        <StatusBadge value={table.health_status || table.status} />
      </td>
      <td className="table-message-cell">
        <MessageCell table={table} />
      </td>
      <td title={displayValue(table.writer_process_id)}>{displayValue(table.writer_process_id)}</td>
      <td className="numeric-cell">{displayValue(table.writer_epoch)}</td>
      <td className="numeric-cell">{displayNumber(table.reader_count)}</td>
      <td className="numeric-cell">{displayNumber(table.sealed_count)}</td>
      <td className="numeric-cell">{displayNumber(table.persisted_count)}</td>
      <td className="action-column">
        <ChevronRight className="row-action-icon" size={16} />
      </td>
    </tr>
  );
}

function TaskRow({ task }: { task: TaskInfo }): JSX.Element {
  const metrics = task.metrics || {};
  const updated = metrics.schedule_state || metrics.started_at || metrics.stopped_at || "-";
  return (
    <tr>
      <td className="strong" title={task.cmd || task.name}>
        {task.name}
      </td>
      <td>{displayValue(task.kind)}</td>
      <td>
        <StatusBadge value={task.status} />
      </td>
      <td>{displayValue(task.process_id)}</td>
      <td>
        <StatusBadge value={metrics.health} />
      </td>
      <td>{displayNumber(metrics.restart_count)}</td>
      <td title={updated}>{updated}</td>
    </tr>
  );
}

function LogRow({ row }: { row: LogEntry }): JSX.Element {
  return (
    <tr>
      <td>{displayValue(row.time)}</td>
      <td>
        <StatusBadge value={String(row.level || "").toLowerCase()} />
      </td>
      <td>{displayValue(row.source)}</td>
      <td title={displayValue(row.message)}>{displayValue(row.message)}</td>
    </tr>
  );
}

function alertLogs(data: DashboardPayload): LogEntry[] {
  return data.log_tail
    .map((entry) => (typeof entry === "string" ? parseLogLine(entry) : entry))
    .filter((entry): entry is LogEntry => Boolean(entry && isAlertLevel(entry.level)));
}

function parseLogLine(line: string): LogEntry {
  try {
    const parsed = JSON.parse(line) as LogEntry;
    return parsed;
  } catch {
    return {
      level: line.match(/\b(ERROR|WARN|WARNING|CRITICAL|FATAL)\b/i)?.[1] || "info",
      message: line,
      source: "log",
      time: "-",
    };
  }
}

function isAlertLevel(level: unknown): boolean {
  return ["warning", "warn", "error", "critical", "fatal"].includes(String(level || "").toLowerCase());
}

function statusMetricValue(
  data: DashboardPayload,
  overallStatus: string,
  overallHealthy: boolean,
): string {
  const issueCount = data.stale_error_tables.length || data.totals.alerts || 0;
  if (overallHealthy) {
    return "0 warning";
  }
  return `${displayNumber(issueCount || 1)} ${displayValue(overallStatus)}`;
}

export function EmptyState({ title }: { title: string }): JSX.Element {
  return <div className="empty-state">{title}</div>;
}
