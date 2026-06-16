import type { LucideIcon } from "lucide-react";

interface MetricCardProps {
  label: string;
  value: string;
  icon: LucideIcon;
  tone?: "default" | "ok" | "warning" | "error" | "muted";
}

export function MetricCard({
  icon: Icon,
  label,
  tone = "default",
  value,
}: MetricCardProps): JSX.Element {
  return (
    <div className="metric">
      <div className="metric-copy">
        <div className="metric-label">{label}</div>
        <div className={`metric-value ${tone}`}>{value}</div>
      </div>
      <div className={`metric-icon ${tone}`}>
        <Icon size={34} />
      </div>
    </div>
  );
}
