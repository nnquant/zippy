import { useState } from "react";

import type { TableSummary } from "../api/client";

export function tableIssueMessage(table: TableSummary): string {
  const alerts = table.alerts || [];
  const alert =
    alerts.find((item) => ["error", "warning"].includes(String(item.severity || "").toLowerCase())) ||
    alerts[0];
  if (alert) {
    return [alert.kind, alert.message || alert.summary || alert.reason].filter(Boolean).join(": ");
  }
  const health = String(table.health_status || "").toLowerCase();
  const status = String(table.status || "").toLowerCase();
  if (["warning", "stale", "error", "failed"].includes(health) || ["warning", "stale", "error", "failed"].includes(status)) {
    return `health_status=[${table.health_status || "-"}] status=[${table.status || "-"}]`;
  }
  return "";
}

export function MessageCell({ table }: { table: TableSummary }): JSX.Element {
  const [expanded, setExpanded] = useState(false);
  const message = tableIssueMessage(table);
  if (!message) {
    return <span className="message-empty">-</span>;
  }
  const truncated = message.length > 120;
  const text = expanded || !truncated ? message : `${message.slice(0, 117)}...`;
  return (
    <span className={`message-detail ${expanded ? "expanded" : ""}`} title={message}>
      {text}
      {truncated && (
        <button className="status-more" onClick={() => setExpanded((value) => !value)} type="button">
          {expanded ? "Less" : "More"}
        </button>
      )}
    </span>
  );
}
