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
  type DashboardPayload,
  type SchemaField,
  type StatusValue,
  type TableDataPayload,
  type TableDetailPayload,
  type TableSummary,
} from "../api/client";
import { displayNumber, displayValue, StatusBadge, statusTone } from "../components/status";
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
  const [dataMode, setDataMode] = useState<DataMode>("tail");
  const [dataLimit, setDataLimit] = useState(100);
  const [autoRefresh, setAutoRefresh] = useState(false);

  const selectedSummary = useMemo(
    () => data.tables.find((table) => table.stream_name === selectedTable) || null,
    [data.tables, selectedTable],
  );

  useEffect(() => {
    if (!selectedTable) {
      setDetail(null);
      setTableData(null);
      return;
    }
    setDetail(null);
    setTableData(null);
    void loadDetail(selectedTable);
    if (tab === "data") {
      void loadData(selectedTable);
    }
  }, [selectedTable]);

  useEffect(() => {
    if (!selectedTable || tab !== "data" || !autoRefresh) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      void loadData(selectedTable);
    }, 5000);
    return () => window.clearInterval(timer);
  }, [autoRefresh, dataLimit, dataMode, selectedTable, tab]);

  async function loadDetail(tableName = selectedTable): Promise<void> {
    if (!tableName) {
      return;
    }
    const payload = await fetchTableDetail(tableName);
    if (tableName === selectedTable) {
      setDetail(payload);
    }
  }

  async function loadData(tableName = selectedTable): Promise<void> {
    if (!tableName) {
      return;
    }
    const payload = await fetchTableData(tableName, dataMode, dataLimit);
    if (tableName === selectedTable) {
      setTableData(payload);
    }
  }

  function handleTabChange(value: string): void {
    const nextTab = value === "data" ? "data" : "summary";
    setTab(nextTab);
    if (nextTab === "data" && selectedTable && !tableData) {
      void loadData(selectedTable);
    }
  }

  function handleModeChange(mode: DataMode): void {
    setDataMode(mode);
    setTableData(null);
    if (selectedTable) {
      void fetchTableData(selectedTable, mode, dataLimit).then(setTableData);
    }
  }

  function handleLimitChange(value: string): void {
    const nextLimit = Math.min(Math.max(Number(value) || 100, 1), 1000);
    setDataLimit(nextLimit);
    setTableData(null);
    if (selectedTable) {
      void fetchTableData(selectedTable, dataMode, nextLimit).then(setTableData);
    }
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
                key={table.stream_name}
                selected={table.stream_name === selectedTable}
                table={table}
                onSelect={onSelectedTableChange}
              />
            ))}
          </ul>
        </CardContent>
      </Card>

      <Card className="table-inspector">
        <CardHeader>
          <div className="inspector-name">{selectedSummary?.stream_name || "Select a table"}</div>
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
            onModeChange={handleModeChange}
            onRefresh={() => void loadData()}
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
        onClick={() => onSelect(table.stream_name)}
        title={table.stream_name}
        type="button"
      >
        <span className={cn("dot", statusTone(table.health_status || table.status))} />
        <span className="table-name">{table.stream_name}</span>
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
  const fields = table.schema?.fields || table.fields || [];
  const items: Array<[string, unknown]> = [
    ["Status", table.status],
    ["Health", detail.health?.status],
    ["Schema Hash", table.schema_hash],
    ["Writer", table.writer_process_id],
    ["Writer Epoch", table.writer_epoch],
    ["Descriptor", table.descriptor_generation],
    ["Rows", table.row_count],
    ["Active Rows", table.active_rows],
    ["Readers", table.reader_count],
    ["Sealed", table.sealed_count],
    ["Persisted", table.persisted_count],
    ["Persist Events", table.persist_event_count],
  ];

  return (
    <CardContent className="tab-panel">
      <div className="info-grid">
        {items.map(([label, value]) => (
          <div className="info-item" key={label}>
            <div className="info-label">{label}</div>
            <div className="info-value" title={displayValue(value)}>
              {label === "Status" || label === "Health" ? (
                <StatusBadge value={value} />
              ) : (
                displayValue(value)
              )}
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
            <th>Timezone</th>
          </tr>
        </thead>
        <tbody>
          {fields.length ? (
            fields.map((field) => (
              <tr key={field.name || "-"}>
                <td className="strong">{displayValue(field.name)}</td>
                <td title={displayValue(field.data_type)}>{displayValue(field.data_type)}</td>
                <td>{displayValue(field.nullable)}</td>
                <td>{displayValue(field.timezone)}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={4}>
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
}): JSX.Element {
  const columns = data?.columns || [];
  const columnTypes = useMemo(() => dataTypeByColumn(detail), [detail]);
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
  }, [data?.table_name]);

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
            disabled={disabled}
            onClick={() => onModeChange("tail")}
            size="icon"
            title="Tail"
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

function dataTypeByColumn(detail: TableDetailPayload | null): Map<string, string> {
  const fields = detail?.table?.schema?.fields || detail?.table?.fields || [];
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
  const tableName = data?.table_name || "table";
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
