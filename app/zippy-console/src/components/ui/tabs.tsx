import * as React from "react";

import { cn } from "../../lib/utils";

interface TabsProps {
  value: string;
  onValueChange: (value: string) => void;
  children: React.ReactNode;
}

interface TabTriggerProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  value: string;
  activeValue: string;
}

export function Tabs({ children }: TabsProps): JSX.Element {
  return <div className="ui-tabs">{children}</div>;
}

export function TabsList({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>): JSX.Element {
  return <div className={cn("ui-tabs-list", className)} {...props} />;
}

export function TabsTrigger({
  activeValue,
  className,
  value,
  ...props
}: TabTriggerProps): JSX.Element {
  return (
    <button
      className={cn("ui-tabs-trigger", activeValue === value && "active", className)}
      type="button"
      {...props}
    />
  );
}
