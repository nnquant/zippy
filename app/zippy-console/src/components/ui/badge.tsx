import * as React from "react";

import { cn } from "../../lib/utils";

type BadgeTone = "default" | "ok" | "warning" | "error" | "muted";

export interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone;
}

export function Badge({ className, tone = "default", ...props }: BadgeProps): JSX.Element {
  return <span className={cn("ui-badge", `ui-badge-${tone}`, className)} {...props} />;
}
