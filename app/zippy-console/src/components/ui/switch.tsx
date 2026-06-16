import * as React from "react";

export interface SwitchProps
  extends Omit<React.InputHTMLAttributes<HTMLInputElement>, "type"> {
  label: string;
}

export function Switch({ label, ...props }: SwitchProps): JSX.Element {
  return (
    <label className="ui-switch">
      <input type="checkbox" {...props} />
      <span className="ui-switch-track" aria-hidden="true">
        <span className="ui-switch-thumb" />
      </span>
      <span>{label}</span>
    </label>
  );
}
