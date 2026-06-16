import { LayoutDashboard, ScrollText, Table2 } from "lucide-react";

import { cn } from "../lib/utils";

export type ConsoleView = "dashboard" | "tables" | "logs";

interface SidebarProps {
  activeView: ConsoleView;
  onViewChange: (view: ConsoleView) => void;
}

export function Sidebar({ activeView, onViewChange }: SidebarProps): JSX.Element {
  return (
    <aside className="sidebar">
      <div className="brand">
        <img className="brand-logo" src="/assets/zippy-logo.png" alt="zippy" />
      </div>
      <button
        className={cn("nav-item", activeView === "dashboard" && "active")}
        onClick={() => onViewChange("dashboard")}
        type="button"
      >
        <LayoutDashboard className="nav-icon" />
        <span>Dashboard</span>
      </button>
      <button
        className={cn("nav-item", activeView === "tables" && "active")}
        onClick={() => onViewChange("tables")}
        type="button"
      >
        <Table2 className="nav-icon" />
        <span>Tables</span>
      </button>
      <button
        className={cn("nav-item", activeView === "logs" && "active")}
        onClick={() => onViewChange("logs")}
        type="button"
      >
        <ScrollText className="nav-icon" />
        <span>Logs</span>
      </button>
    </aside>
  );
}
