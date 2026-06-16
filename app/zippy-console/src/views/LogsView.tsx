import { AlertTriangle, Copy, Download, Pause, Play, Search } from "lucide-react";
import { useMemo, useState } from "react";

import type { DashboardPayload, LogEntry } from "../api/client";
import { displayValue, StatusBadge } from "../components/status";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { DataTable } from "../components/ui/table";
import { cn } from "../lib/utils";
import { EmptyState } from "./DashboardView";

type LevelFilter = "all" | "error" | "warning" | "info" | "debug";

const logColumns = ["Time", "Level", "Source", "Event", "Message"];

interface ParsedLogEntry extends LogEntry {
  event?: string;
  raw: Record<string, unknown>;
}

interface LogsViewProps {
  data: DashboardPayload;
}

export function LogsView({ data }: LogsViewProps): JSX.Element {
  const [levelFilter, setLevelFilter] = useState<LevelFilter>("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [searchQuery, setSearchQuery] = useState("");
  const [searchOpen, setSearchOpen] = useState(false);
  const [onlyAlerts, setOnlyAlerts] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set());
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);

  const logs = useMemo(() => data.log_tail.map(parseLogEntry).reverse(), [data.log_tail]);
  const sources = useMemo(() => uniqueSources(logs), [logs]);
  const filteredLogs = useMemo(
    () => visibleLogs(logs, levelFilter, sourceFilter, searchQuery, onlyAlerts),
    [levelFilter, logs, onlyAlerts, searchQuery, sourceFilter],
  );
  const selectedLog = selectedIndex === null ? filteredLogs[0] : filteredLogs[selectedIndex] || null;

  function toggleExpanded(index: number): void {
    setExpandedRows((current) => {
      const next = new Set(current);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });
  }

  return (
    <div className="logs-layout">
      <Card className="logs-main-card">
        <CardHeader>
          <CardTitle>Logs</CardTitle>
        </CardHeader>
        <CardContent className="tab-panel data-tab-panel logs-panel">
          <div className="data-toolbar logs-toolbar">
            <div className="data-toolbar-group">
              <select
                className="select-box"
                onChange={(event) => setLevelFilter(event.target.value as LevelFilter)}
                value={levelFilter}
              >
                <option value="all">All levels</option>
                <option value="error">Error</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
                <option value="debug">Debug</option>
              </select>
              <select
                className="select-box"
                onChange={(event) => setSourceFilter(event.target.value)}
                value={sourceFilter}
              >
                <option value="all">All sources</option>
                {sources.map((source) => (
                  <option key={source} value={source}>
                    {source}
                  </option>
                ))}
              </select>
            </div>
            <div className="data-toolbar-group data-toolbar-grow">
              <Button
                aria-label="Toggle search"
                className={cn("toolbar-icon-button", searchOpen && "active")}
                onClick={() => setSearchOpen((current) => !current)}
                size="icon"
                title="Search"
                variant="outline"
              >
                <Search size={16} />
              </Button>
              {searchOpen && (
                <input
                  className="data-search-input"
                  onChange={(event) => setSearchQuery(event.target.value)}
                  placeholder="Search logs"
                  value={searchQuery}
                />
              )}
            </div>
            <div className="data-toolbar-group">
              <Button
                aria-label="Show only alert logs"
                className={cn("toolbar-icon-button", onlyAlerts && "active")}
                onClick={() => setOnlyAlerts((current) => !current)}
                size="icon"
                title="Only Alerts"
                variant="outline"
              >
                <AlertTriangle size={16} />
              </Button>
              <Button
                aria-label={autoRefresh ? "Pause auto refresh" : "Start auto refresh"}
                className={cn("toolbar-icon-button", autoRefresh && "active")}
                onClick={() => setAutoRefresh((current) => !current)}
                size="icon"
                title="Auto Refresh"
                variant="outline"
              >
                {autoRefresh ? <Pause size={16} /> : <Play size={16} />}
              </Button>
              <Button
                aria-label="Copy visible logs"
                className="toolbar-icon-button"
                disabled={!filteredLogs.length}
                onClick={() => void copyLogs(filteredLogs)}
                size="icon"
                title="Copy visible logs"
                variant="outline"
              >
                <Copy size={16} />
              </Button>
              <Button
                aria-label="Export JSONL"
                className="toolbar-icon-button"
                disabled={!filteredLogs.length}
                onClick={() => exportLogs(filteredLogs)}
                size="icon"
                title="Export JSONL"
                variant="outline"
              >
                <Download size={16} />
              </Button>
            </div>
          </div>

          <div className="data-scroll logs-table-wrap">
            <DataTable className="data-table logs-table">
              <thead>
                <tr>
                  {logColumns.map((column) => <th key={column}>{column}</th>)}
                </tr>
              </thead>
              <tbody>
                {filteredLogs.length ? (
                  filteredLogs.map((entry, index) => (
                    <tr
                      className="clickable"
                      key={`${entry.time}-${entry.event}-${index}`}
                      onClick={() => setSelectedIndex(index)}
                    >
                      <td>{displayValue(entry.time)}</td>
                      <td>
                        <StatusBadge value={logLevel(entry)} />
                      </td>
                      <td>{displayValue(entry.source)}</td>
                      <td>{displayValue(entry.event)}</td>
                      <td>
                        <div
                          className={cn(
                            "log-message-text",
                            expandedRows.has(index) && "expanded",
                          )}
                          title={displayValue(entry.message)}
                        >
                          {displayValue(entry.message)}
                        </div>
                        {displayValue(entry.message).length > 96 && (
                          <button
                            className="link-button"
                            onClick={(event) => {
                              event.stopPropagation();
                              toggleExpanded(index);
                            }}
                            type="button"
                          >
                            {expandedRows.has(index) ? "Less" : "More"}
                          </button>
                        )}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5}>
                      <EmptyState title="No logs" />
                    </td>
                  </tr>
                )}
              </tbody>
            </DataTable>
          </div>
        </CardContent>
      </Card>

      <Card className="log-detail-panel">
        <CardHeader>
          <CardTitle>Raw JSON</CardTitle>
        </CardHeader>
        <CardContent>
          {selectedLog ? (
            <div className="log-json" role="tree">
              <JsonValue value={selectedLog.raw} />
            </div>
          ) : (
            <EmptyState title="Select a log row." />
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function parseLogEntry(entry: LogEntry | string): ParsedLogEntry {
  if (typeof entry === "string") {
    const level = entry.match(/\b(ERROR|WARN|WARNING|INFO|DEBUG|CRITICAL|FATAL)\b/i)?.[1] || "info";
    return {
      level,
      message: entry,
      source: "log",
      time: "-",
      raw: { level, message: entry, source: "log", time: "-" },
    };
  }
  const raw = entry as Record<string, unknown>;
  return {
    ...entry,
    event: displayValue(raw.event),
    level: displayValue(raw.level || raw.severity || "info"),
    message: displayValue(raw.message || raw.msg || raw.event),
    source: displayValue(raw.source || raw.logger || raw.module || "log"),
    time: displayValue(raw.time || raw.timestamp || raw.ts || "-"),
    raw,
  };
}

function visibleLogs(
  logs: ParsedLogEntry[],
  levelFilter: LevelFilter,
  sourceFilter: string,
  searchQuery: string,
  onlyAlerts: boolean,
): ParsedLogEntry[] {
  const query = searchQuery.trim().toLowerCase();
  return logs.filter((entry) => {
    const level = logLevel(entry);
    if (onlyAlerts && !isAlertLog(entry)) {
      return false;
    }
    if (levelFilter !== "all" && level !== levelFilter) {
      return false;
    }
    if (sourceFilter !== "all" && entry.source !== sourceFilter) {
      return false;
    }
    if (!query) {
      return true;
    }
    return [entry.time, entry.level, entry.source, entry.event, entry.message]
      .map(displayValue)
      .some((value) => value.toLowerCase().includes(query));
  });
}

function isAlertLog(entry: ParsedLogEntry): boolean {
  return ["warning", "warn", "error", "critical", "fatal"].includes(logLevel(entry));
}

function logLevel(entry: ParsedLogEntry): string {
  const level = displayValue(entry.level).toLowerCase();
  if (level === "warn") {
    return "warning";
  }
  if (["critical", "fatal"].includes(level)) {
    return "error";
  }
  return level || "info";
}

async function copyLogs(logs: ParsedLogEntry[]): Promise<void> {
  if (navigator.clipboard) {
    await navigator.clipboard.writeText(logs.map(logJson).join("\n"));
  }
}

function exportLogs(logs: ParsedLogEntry[]): void {
  const blob = new Blob([logs.map(logJson).join("\n")], {
    type: "application/jsonl;charset=utf-8",
  });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = "zippy-logs.jsonl";
  anchor.click();
  URL.revokeObjectURL(url);
}

function logJson(entry: ParsedLogEntry): string {
  return JSON.stringify(entry.raw, null, 2);
}

function JsonValue({
  value,
  depth = 0,
  trailingComma = false,
}: {
  value: unknown;
  depth?: number;
  trailingComma?: boolean;
}): JSX.Element {
  if (Array.isArray(value)) {
    return (
      <div className="json-node">
        <div className="json-line" style={{ paddingLeft: depth * 14 }}>
          <span className="json-punctuation">[</span>
        </div>
        {value.map((item, index) => (
          <JsonValue
            depth={depth + 1}
            key={`${depth}-${index}`}
            trailingComma={index < value.length - 1}
            value={item}
          />
        ))}
        <div className="json-line" style={{ paddingLeft: depth * 14 }}>
          <span className="json-punctuation">]{trailingComma ? "," : ""}</span>
        </div>
      </div>
    );
  }

  if (isJsonRecord(value)) {
    const entries = Object.entries(value);
    return (
      <div className="json-node">
        <div className="json-line" style={{ paddingLeft: depth * 14 }}>
          <span className="json-punctuation">{"{"}</span>
        </div>
        {entries.map(([key, item], index) => (
          <div className="json-entry" key={key}>
            <div className="json-line" style={{ paddingLeft: (depth + 1) * 14 }}>
              <span className="json-key">{JSON.stringify(key)}</span>
              <span className="json-punctuation">: </span>
              {!isJsonRecord(item) && !Array.isArray(item) && (
                <>
                  <JsonScalar value={item} />
                  <span className="json-punctuation">{index < entries.length - 1 ? "," : ""}</span>
                </>
              )}
            </div>
            {(isJsonRecord(item) || Array.isArray(item)) && (
              <JsonValue
                depth={depth + 1}
                trailingComma={index < entries.length - 1}
                value={item}
              />
            )}
          </div>
        ))}
        <div className="json-line" style={{ paddingLeft: depth * 14 }}>
          <span className="json-punctuation">{"}"}{trailingComma ? "," : ""}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="json-line" style={{ paddingLeft: depth * 14 }}>
      <JsonScalar value={value} />
      <span className="json-punctuation">{trailingComma ? "," : ""}</span>
    </div>
  );
}

function JsonScalar({ value }: { value: unknown }): JSX.Element {
  const scalarClass =
    value === null
      ? "json-null"
      : typeof value === "string"
        ? "json-string"
        : typeof value === "number"
          ? "json-number"
          : typeof value === "boolean"
            ? "json-boolean"
            : "json-null";
  const rendered = value === undefined ? "null" : JSON.stringify(value);
  return <span className={`json-scalar ${scalarClass}`}>{rendered}</span>;
}

function isJsonRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function uniqueSources(logs: ParsedLogEntry[]): string[] {
  return Array.from(new Set(logs.map((entry) => displayValue(entry.source)))).sort();
}
