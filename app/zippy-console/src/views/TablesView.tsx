import {
  ArrowDownToLine,
  ArrowUpToLine,
  ClipboardList,
  Columns3,
  Copy,
  Download,
  Maximize2,
  Minimize2,
  Pause,
  Play,
  RefreshCw,
  Search,
  WrapText,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import {
  fetchTableData,
  fetchTableDetail,
  tableKey,
  tableLabel,
  type DashboardPayload,
  type SchemaField,
  type StatusValue,
  type TableDataPayload,
  type TableDetailPayload,
  type TableSummary,
} from "../api/client";
import { displayNumber, displayValue } from "../components/status";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardHeader } from "../components/ui/card";
import { DataTable } from "../components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "../components/ui/tabs";
import { cn } from "../lib/utils";
import { EmptyState } from "./DashboardView";

interface TablesViewProps {
  data: DashboardPayload;
  selectedTable: string | null;
  onSelectedTableChange: (tableName: string) => void;
}

type TableTab = "summary" | "data";
type DataMode = "head" | "tail";

export function TablesView({
  data,
  onSelectedTableChange,
  selectedTable,
}: TablesViewProps): JSX.Element {
  const [tab, setTab] = useState<TableTab>("summary");
  const [detail, setDetail] = useState<TableDetailPayload | null>(null);
  const [tableData, setTableData] = useState<TableDataPayload | null>(null);
  const [dataMode, setDataMode] = useState<DataMode>("head");
  const [dataLimit, setDataLimit] = useState(100);
  const [autoRefresh, setAutoRefresh] = useState(false);

  const [orderBy, setOrderBy] = useState<string[]>([]);
  const [descending, setDescending] = useState(false);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const selectedSummary = useMemo(
    () => data.tables.find((table) => tableKey(table) === selectedTable) || null,
    [data.tables, selectedTable],
  );
  const namespace = selectedSummary?.namespace;
  const name = selectedSummary?.name;

  useEffect(() => {
    setDetail(null);
    setTableData(null);
    setOrderBy([]);
    setDataMode("head");
    if (!selectedSummary) return;
    const controller = new AbortController();
    void fetchTableDetail(selectedSummary, controller.signal).then((payload) => {
      if (!controller.signal.aborted) setDetail(payload);
    }).catch((error: unknown) => {
      if (!controller.signal.aborted) setDetail({ ok: false, error: String(error) });
    });
    return () => controller.abort();
  }, [namespace, name]);

  useEffect(() => {
    setTableData(null);
    if (!selectedSummary || tab !== "data") return;
    const controller = new AbortController();
    void fetchTableData(
      selectedSummary, dataMode, dataLimit, orderBy, descending, controller.signal,
    ).then((payload) => {
      if (!controller.signal.aborted) setTableData(payload);
    }).catch((error: unknown) => {
      if (!controller.signal.aborted) setTableData({
        ok: false, table_name: selectedSummary.name, namespace: selectedSummary.namespace,
        mode: dataMode, limit: dataLimit, columns: [], rows: [], row_count: 0,
        truncated: false, error: String(error),
      });
    });
    return () => controller.abort();
  }, [namespace, name, tab, dataMode, dataLimit, orderBy, descending, refreshVersion]);

  useEffect(() => {
    if (!selectedTable || tab !== "data" || !autoRefresh || !tableData) return;
    const timer = window.setTimeout(() => setRefreshVersion((current) => current + 1), 5000);
    return () => window.clearTimeout(timer);
  }, [autoRefresh, selectedTable, tab, tableData]);

  function handleTabChange(value: string): void {
    setTab(value === "data" ? "data" : "summary");
  }

  function handleLimitChange(value: string): void {
    setDataLimit(Math.min(Math.max(Number(value) || 100, 1), 1000));
  }

  return (
    <div className="tables-layout">
      <Card className="tables-list-panel">
        <CardHeader>
          <h2 className="ui-card-title">Tables</h2>
        </CardHeader>
        <CardContent className="tables-list-scroll">
          <ul className="table-name-list">
            {data.tables.map((table) => (
              <TableNameItem
                key={tableKey(table)}
                selected={tableKey(table) === selectedTable}
                table={table}
                onSelect={onSelectedTableChange}
              />
            ))}
          </ul>
        </CardContent>
      </Card>

      <Card className="table-inspector">
        <CardHeader>
          <div className="inspector-name">{selectedSummary ? tableLabel(selectedSummary) : "Select a table"}</div>
        </CardHeader>
        <Tabs onValueChange={handleTabChange} value={tab}>
          <TabsList>
            <TabsTrigger activeValue={tab} onClick={() => handleTabChange("summary")} value="summary">
              Summary
            </TabsTrigger>
            <TabsTrigger activeValue={tab} onClick={() => handleTabChange("data")} value="data">
              Data
            </TabsTrigger>
          </TabsList>
        </Tabs>
        {tab === "summary" ? (
          <SummaryPanel detail={detail} summary={selectedSummary} />
        ) : (
          <DataPanel
            autoRefresh={autoRefresh}
            data={tableData}
            dataLimit={dataLimit}
            dataMode={dataMode}
            detail={detail}
            disabled={!selectedTable}
            onAutoRefreshChange={setAutoRefresh}
            onLimitChange={handleLimitChange}
            onModeChange={setDataMode}
            orderBy={orderBy}
            onOrderByChange={(columns) => { setOrderBy(columns); if (!columns.length) setDataMode("head"); }}
            descending={descending}
            onDescendingChange={setDescending}
            onRefresh={() => setRefreshVersion((current) => current + 1)}
          />
        )}
      </Card>
    </div>
  );
}

function TableNameItem({
  onSelect,
  selected,
  table,
}: {
  onSelect: (tableName: string) => void;
  selected: boolean;
  table: TableSummary;
}): JSX.Element {
  return (
    <li>
      <button
        className={cn("table-name-item", selected && "selected")}
        onClick={() => onSelect(tableKey(table))}
        title={tableLabel(table)}
        type="button"
      >
        <span className="table-name">{tableLabel(table)}</span>
      </button>
    </li>
  );
}

function SummaryPanel({
  detail,
  summary,
}: {
  detail: TableDetailPayload | null;
  summary: TableSummary | null;
}): JSX.Element {
  if (!summary) {
    return (
      <CardContent className="tab-panel">
        <EmptyState title="Select a table from the list." />
      </CardContent>
    );
  }
  if (!detail) {
    return (
      <CardContent className="tab-panel">
        <EmptyState title="Loading table info..." />
      </CardContent>
    );
  }
  if (!detail.ok) {
    return (
      <CardContent className="tab-panel">
        <EmptyState title={detail.error || "Failed to load table info"} />
      </CardContent>
    );
  }

  const table = detail.table || summary;
  const fields = table.schema?.fields || [];
  const items: Array<[string, unknown]> = [
    ["Namespace", table.namespace], ["Name", table.name], ["Catalog", table.catalog],
    ["Object type", table.table_type],
    ...Object.entries(table.snapshots?.[0] || {}).map(([key, value]): [string, unknown] => [key, value]),
  ];

  return (
    <CardContent className="tab-panel">
      <div className="info-grid">
        {items.map(([label, value]) => (
          <div className="info-item" key={label}>
            <div className="info-label">{label}</div>
            <div className="info-value" title={displayValue(value)}>
              {displayValue(value)}
            </div>
          </div>
        ))}
      </div>
      <div className="section-title">Schema</div>
      <DataTable className="schema-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Nullable</th>
          </tr>
        </thead>
        <tbody>
          {fields.length ? (
            fields.map((field) => (
              <tr key={field.name || "-"}>
                <td className="strong">{displayValue(field.name)}</td>
                <td title={displayValue(field.data_type)}>{displayValue(field.data_type)}</td>
                <td>{displayValue(field.nullable)}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={3}>
                <EmptyState title="No schema fields" />
              </td>
            </tr>
          )}
        </tbody>
      </DataTable>
    </CardContent>
  );
}

function DataPanel({
  autoRefresh,
  data,
  dataLimit,
  dataMode,
  detail,
  disabled,
  onAutoRefreshChange,
  onLimitChange,
  onModeChange,
  onRefresh,
  orderBy, onOrderByChange, descending, onDescendingChange,
}: {
  autoRefresh: boolean;
  data: TableDataPayload | null;
  dataLimit: number;
  dataMode: DataMode;
  detail: TableDetailPayload | null;
  disabled: boolean;
  onAutoRefreshChange: (checked: boolean) => void;
  onLimitChange: (value: string) => void;
  onModeChange: (mode: DataMode) => void;
  onRefresh: () => void;
  orderBy: string[];
  onOrderByChange: (columns: string[]) => void;
  descending: boolean;
  onDescendingChange: (value: boolean) => void;
}): JSX.Element {
  const columns = data?.columns || [];
  const columnTypes = useMemo(() => dataTypeByColumn(data), [data]);
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set());
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [wrapCells, setWrapCells] = useState(false);
  const [fullscreen, setFullscreen] = useState(false);
  const visibleColumns = useMemo(
    () => columns.filter((column) => !hiddenColumns.has(column)),
    [columns, hiddenColumns],
  );
  const rows = useMemo(
    () => visibleRows(data, visibleColumns, searchQuery),
    [data, searchQuery, visibleColumns],
  );
  const canUseRows = Boolean(data?.ok && visibleColumns.length);

  useEffect(() => {
    setHiddenColumns(new Set());
    setSearchQuery("");
    setSearchOpen(false);
  }, [data?.namespace, data?.table_name]);

  function toggleColumn(column: string): void {
    setHiddenColumns((current) => {
      const next = new Set(current);
      if (next.has(column)) {
        next.delete(column);
      } else if (visibleColumns.length > 1) {
        next.add(column);
      }
      return next;
    });
  }

  return (
    <CardContent className={cn("tab-panel data-tab-panel", fullscreen && "fullscreen")}>
      <div className="data-toolbar">
        <div className="data-toolbar-group" aria-label="Data range">
          <Button
            aria-label="Load tail rows"
            className={cn("toolbar-icon-button", dataMode === "tail" && "active")}
            disabled={disabled || !orderBy.length}
            onClick={() => onModeChange("tail")}
            size="icon"
            title="Tail requires explicit sort columns"
            variant="outline"
          >
            <ArrowDownToLine size={16} />
          </Button>
          <Button
            aria-label="Load head rows"
            className={cn("toolbar-icon-button", dataMode === "head" && "active")}
            disabled={disabled}
            onClick={() => onModeChange("head")}
            size="icon"
            title="Head"
            variant="outline"
          >
            <ArrowUpToLine size={16} />
          </Button>
          <select
            className="select-box rows-select"
            disabled={disabled}
            onChange={(event) => onLimitChange(event.target.value)}
            title="Rows"
            value={dataLimit}
          >
            <option value="100">100 rows</option>
            <option value="500">500 rows</option>
            <option value="1000">1000 rows</option>
          </select>
        </div>

        <div className="data-toolbar-group" aria-label="Sort controls">
          <details className="toolbar-menu">
            <summary className="ui-button ui-button-outline ui-button-size-default" aria-label="Sort columns">Sort columns</summary>
            <div className="toolbar-menu-panel">
              {(detail?.table?.schema?.fields || []).map((field) => (
                <label className="toolbar-menu-item" key={field.name}>
                  <input type="checkbox" checked={orderBy.includes(field.name || "")}
                    onChange={(event) => onOrderByChange(event.target.checked
                      ? [...orderBy, field.name || ""] : orderBy.filter((name) => name !== field.name))} />
                  <span>{field.name}</span>
                </label>
              ))}
            </div>
          </details>
          <select className="select-box" aria-label="Sort direction" value={String(descending)}
            disabled={!orderBy.length} onChange={(event) => onDescendingChange(event.target.value === "true")}>
            <option value="false">Ascending</option><option value="true">Descending</option>
          </select>
        </div>

        <div className="data-toolbar-group" aria-label="Refresh controls">
          <Button
            aria-label="Refresh data"
            className="toolbar-icon-button"
            disabled={disabled}
            onClick={onRefresh}
            size="icon"
            title="Refresh"
            variant="outline"
          >
            <RefreshCw size={16} />
          </Button>
          <Button
            aria-label={autoRefresh ? "Pause auto refresh" : "Start auto refresh"}
            className={cn("toolbar-icon-button", autoRefresh && "active")}
            disabled={disabled}
            onClick={() => onAutoRefreshChange(!autoRefresh)}
            size="icon"
            title={autoRefresh ? "Pause auto refresh" : "Start auto refresh"}
            variant="outline"
          >
            {autoRefresh ? <Pause size={16} /> : <Play size={16} />}
          </Button>
        </div>

        <div className="data-toolbar-group data-toolbar-grow" aria-label="Table view controls">
          <Button
            aria-label="Toggle search"
            className={cn("toolbar-icon-button", searchOpen && "active")}
            disabled={!data?.ok}
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
              placeholder="Search rows"
              value={searchQuery}
            />
          )}
          <details className="toolbar-menu">
            <summary
              aria-label="Manage columns"
              className="ui-button ui-button-outline ui-button-size-icon toolbar-icon-button"
              title="Columns"
            >
              <Columns3 size={16} />
            </summary>
            <div className="toolbar-menu-panel">
              {columns.map((column) => (
                <label className="toolbar-menu-item" key={column} title={column}>
                  <input
                    checked={!hiddenColumns.has(column)}
                    disabled={!hiddenColumns.has(column) && visibleColumns.length <= 1}
                    onChange={() => toggleColumn(column)}
                    type="checkbox"
                  />
                  <span>{column}</span>
                </label>
              ))}
            </div>
          </details>
          <Button
            aria-label="Toggle text wrap"
            className={cn("toolbar-icon-button", wrapCells && "active")}
            disabled={!data?.ok}
            onClick={() => setWrapCells((current) => !current)}
            size="icon"
            title="Wrap text"
            variant="outline"
          >
            <WrapText size={16} />
          </Button>
          <Button
            aria-label={fullscreen ? "Exit fullscreen" : "Fullscreen"}
            className={cn("toolbar-icon-button", fullscreen && "active")}
            onClick={() => setFullscreen((current) => !current)}
            size="icon"
            title={fullscreen ? "Exit fullscreen" : "Fullscreen"}
            variant="outline"
          >
            {fullscreen ? <Minimize2 size={16} /> : <Maximize2 size={16} />}
          </Button>
        </div>

        <div className="data-toolbar-group" aria-label="Export controls">
          <Button
            aria-label="Copy visible rows"
            className="toolbar-icon-button"
            disabled={!canUseRows}
            onClick={() => void copyRows(rows, visibleColumns)}
            size="icon"
            title="Copy visible rows"
            variant="outline"
          >
            <Copy size={16} />
          </Button>
          <Button
            aria-label="Copy column names"
            className="toolbar-icon-button"
            disabled={!visibleColumns.length}
            onClick={() => void copyColumns(visibleColumns)}
            size="icon"
            title="Copy column names"
            variant="outline"
          >
            <ClipboardList size={16} />
          </Button>
          <details className="toolbar-menu">
            <summary
              aria-label="Export data"
              className="ui-button ui-button-outline ui-button-size-icon toolbar-icon-button"
              title="Export"
            >
              <Download size={16} />
            </summary>
            <div className="toolbar-menu-panel compact">
              <button
                disabled={!canUseRows}
                onClick={() => exportRows(data, rows, visibleColumns, "csv")}
                type="button"
              >
                Export CSV
              </button>
              <button
                disabled={!canUseRows}
                onClick={() => exportRows(data, rows, visibleColumns, "json")}
                type="button"
              >
                Export JSON
              </button>
            </div>
          </details>
        </div>
      </div>
      <div className="data-preview-note">
        {orderBy.length ? `Sort: ${orderBy.join(", ")} (${descending ? "descending" : "ascending"})`
          : "Unsorted preview. Choose sort columns for ordered head or tail rows."}
        {data?.ok && ` · ${data.row_count} rows${data.truncated ? " (preview limited)" : ""}`}
      </div>
      <div className="data-scroll">
        <DataTable className={cn("data-table", wrapCells && "wrap-cells")}>
          <thead>
            <tr>
              {visibleColumns.map((column) => (
                <th key={column}>
                  <div className="data-column-name">{column}</div>
                  <div className="data-column-type">{columnTypes.get(column) || "unknown"}</div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {renderDataRows(data, visibleColumns, rows)}
          </tbody>
        </DataTable>
      </div>
    </CardContent>
  );
}

function dataTypeByColumn(data: TableDataPayload | null): Map<string, string> {
  const fields = data?.schema?.fields || [];
  return new Map(
    fields
      .filter((field): field is SchemaField & { name: string } => Boolean(field.name))
      .map((field) => [field.name, displayColumnType(field)]),
  );
}

function displayColumnType(field: SchemaField): string {
  const dataType = displayValue(field.data_type);
  if (dataType === "-") {
    return "unknown";
  }
  return dataType;
}

function visibleRows(
  data: TableDataPayload | null,
  columns: string[],
  searchQuery: string,
): Array<Record<string, StatusValue>> {
  if (!data?.ok) {
    return [];
  }
  const query = searchQuery.trim().toLowerCase();
  if (!query) {
    return data.rows;
  }
  return data.rows.filter((row) =>
    columns.some((column) => displayValue(row[column]).toLowerCase().includes(query)),
  );
}

async function copyRows(
  rows: Array<Record<string, StatusValue>>,
  columns: string[],
): Promise<void> {
  await copyText(toTsv(rows, columns));
}

async function copyColumns(columns: string[]): Promise<void> {
  await copyText(columns.join("\t"));
}

async function copyText(text: string): Promise<void> {
  if (navigator.clipboard) {
    await navigator.clipboard.writeText(text);
  }
}

function exportRows(
  data: TableDataPayload | null,
  rows: Array<Record<string, StatusValue>>,
  columns: string[],
  format: "csv" | "json",
): void {
  const tableName = data ? `${data.namespace}-${data.table_name}` : "table";
  if (format === "csv") {
    downloadTextFile(`${tableName}.csv`, toCsv(rows, columns), "text/csv;charset=utf-8");
    return;
  }
  downloadTextFile(
    `${tableName}.json`,
    JSON.stringify(rows.map((row) => pickColumns(row, columns)), null, 2),
    "application/json;charset=utf-8",
  );
}

function downloadTextFile(filename: string, text: string, type: string): void {
  const blob = new Blob([text], { type });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function toCsv(rows: Array<Record<string, StatusValue>>, columns: string[]): string {
  return [columns, ...rows.map((row) => columns.map((column) => row[column]))]
    .map((values) => values.map(csvCell).join(","))
    .join("\n");
}

function toTsv(rows: Array<Record<string, StatusValue>>, columns: string[]): string {
  return [columns, ...rows.map((row) => columns.map((column) => row[column]))]
    .map((values) => values.map((value) => displayValue(value)).join("\t"))
    .join("\n");
}

function csvCell(value: StatusValue): string {
  const text = displayValue(value);
  if (/[",\n\r]/.test(text)) {
    return `"${text.replaceAll('"', '""')}"`;
  }
  return text;
}

function pickColumns(
  row: Record<string, StatusValue>,
  columns: string[],
): Record<string, StatusValue> {
  return Object.fromEntries(columns.map((column) => [column, row[column]]));
}

function renderDataRows(
  data: TableDataPayload | null,
  columns: string[],
  rows: Array<Record<string, StatusValue>>,
): JSX.Element {
  if (!data) {
    return (
      <tr>
        <td>
          <EmptyState title="No data loaded" />
        </td>
      </tr>
    );
  }
  if (!data.ok) {
    return (
      <tr>
        <td>
          <EmptyState title={data.error || "Failed to load data"} />
        </td>
      </tr>
    );
  }
  if (!rows.length) {
    return (
      <tr>
        <td colSpan={Math.max(columns.length, 1)}>
          <EmptyState title="No rows" />
        </td>
      </tr>
    );
  }
  return (
    <>
      {rows.map((row, index) => (
        <tr key={index}>
          {columns.map((column) => (
            <td key={column}>
              <div className="data-cell" title={displayValue(row[column])}>
                {displayNumber(row[column])}
              </div>
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}
