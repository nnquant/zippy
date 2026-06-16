import * as React from "react";

import { cn } from "../../lib/utils";

export function DataTable({
  className,
  ...props
}: React.TableHTMLAttributes<HTMLTableElement>): JSX.Element {
  return <table className={cn("ui-table", className)} {...props} />;
}
