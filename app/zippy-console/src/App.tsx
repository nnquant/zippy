import { AlertTriangle, Clock, X } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { fetchDashboard, type DashboardPayload, type TableSummary } from "./api/client";
import { Sidebar, type ConsoleView } from "./components/Sidebar";
import { statusTone } from "./components/status";
import { Button } from "./components/ui/button";
import { DashboardView } from "./views/DashboardView";
import { LogsView } from "./views/LogsView";
import { TablesView } from "./views/TablesView";

export function App(): JSX.Element {
  const [data, setData] = useState<DashboardPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<ConsoleView>("dashboard");
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [bannerHidden, setBannerHidden] = useState(false);

  async function refresh(): Promise<void> {
    try {
      const payload = await fetchDashboard();
      setData(payload);
      setError(null);
      setLastRefresh(new Date());
      setSelectedTable((current) => {
        if (current && payload.tables.some((table) => table.stream_name === current)) {
          return current;
        }
        return payload.tables[0]?.stream_name || null;
      });
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : String(requestError));
    }
  }

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => {
      void refresh();
    }, 5000);
    return () => window.clearInterval(timer);
  }, []);

  const issue = useMemo(() => (data ? issueSummary(data, error) : null), [data, error]);

  return (
    <div className="app-shell">
      <Sidebar activeView={view} onViewChange={setView} />
      <main className="main-shell">
        <div className="topline">
          <div className="top-left">
            <StatusChip label="Master" status={data?.master.status || (error ? "error" : "unknown")} />
            <StatusChip
              label="Gateway"
              status={data?.gateway?.status || data?.gateway_status || "unknown"}
            />
          </div>
          <div className="top-right">
            <div className="chip">
              <Clock size={15} />
              <span>Last refresh {lastRefresh ? lastRefresh.toLocaleTimeString() : "--:--:--"}</span>
            </div>
          </div>
        </div>

        {issue && !bannerHidden && view === "dashboard" && (
          <div className={`problem-banner show ${issue.level}`}>
            <div className="problem-icon">
              <AlertTriangle size={22} />
            </div>
            <div className="problem-content">
              <div className="problem-message">{issue.message}</div>
            </div>
            <div className="problem-actions">
              <Button
                aria-label="Close alert banner"
                className="problem-close-button"
                onClick={() => setBannerHidden(true)}
                size="icon"
                variant="ghost"
              >
                <X size={18} />
              </Button>
            </div>
          </div>
        )}

        {!data ? (
          error ? (
            <div className="screen-empty">{error}</div>
          ) : (
            <DashboardLoading />
          )
        ) : view === "tables" ? (
          <TablesView
            data={data}
            onSelectedTableChange={setSelectedTable}
            selectedTable={selectedTable}
          />
        ) : view === "logs" ? (
          <LogsView data={data} />
        ) : (
          <DashboardView
            data={data}
            onSelectTable={(tableName) => {
              setSelectedTable(tableName);
              setView("tables");
            }}
          />
        )}
      </main>
    </div>
  );
}

function StatusChip({ label, status }: { label: string; status: unknown }): JSX.Element {
  return (
    <div className="chip">
      <span className={`dot ${statusTone(status)}`} />
      <span>{label}</span>
    </div>
  );
}

function DashboardLoading(): JSX.Element {
  return (
    <div aria-label="Loading dashboard" className="dashboard-loading" role="status">
      <div className="dashboard-loading-ring" />
    </div>
  );
}

function issueSummary(
  data: DashboardPayload,
  requestError: string | null,
): { level: "warning" | "error" | "critical"; message: string } | null {
  if (requestError) {
    return {
      level: "critical",
      message: requestError,
    };
  }
  if (data.master.error) {
    return {
      level: "critical",
      message:
        "master client initialization failed, table registration and read/write requests may fail.",
    };
  }
  const bad = data.stale_error_tables || [];
  if (bad.length) {
    const severity = bad.some((table) => tableSeverity(table) === "error") ? "error" : "warning";
    return {
      level: severity,
      message: `${bad.length} table(s) have ${severity} status, check writer ownership, descriptor_generation, and persist events.`,
    };
  }
  return null;
}

function tableSeverity(table: TableSummary): "warning" | "error" | null {
  const statuses = [table.health_status, table.status].map((status) => String(status || "").toLowerCase());
  const alerts = table.alerts || [];
  if (
    statuses.some((status) => ["error", "failed"].includes(status)) ||
    alerts.some((alert) => String(alert.severity || "").toLowerCase() === "error")
  ) {
    return "error";
  }
  if (
    statuses.some((status) => ["warning", "stale"].includes(status)) ||
    alerts.some((alert) => String(alert.severity || "").toLowerCase() === "warning")
  ) {
    return "warning";
  }
  return null;
}
