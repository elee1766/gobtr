import type { Component, JSX } from "solid-js";
import { Match, Switch } from "solid-js";

type StatusType = "success" | "error" | "warning" | "running" | "neutral";

type StatusBadgeProps = {
  status: StatusType;
  children: JSX.Element;
};

const statusClasses: Record<StatusType, string> = {
  success: "text-success",
  error: "text-error",
  warning: "text-warning",
  running: "text-primary",
  neutral: "text-text-muted",
};

export const StatusBadge: Component<StatusBadgeProps> = (props) => {
  return (
    <span class={statusClasses[props.status]}>
      {props.children}
    </span>
  );
};

// Convenience component for common scrub/status patterns
type StatusDisplayProps = {
  running?: boolean;
  status?: string;
  hasErrors?: boolean;
};

export const ScrubStatus: Component<StatusDisplayProps> = (props) => {
  return (
    <Switch>
      <Match when={props.running}>
        <StatusBadge status="running">running</StatusBadge>
      </Match>
      <Match when={!props.status}>
        <StatusBadge status="neutral">-</StatusBadge>
      </Match>
      <Match when={props.status === "finished" && !props.hasErrors}>
        <StatusBadge status="success">ok</StatusBadge>
      </Match>
      <Match when={props.status === "finished" && props.hasErrors}>
        <StatusBadge status="error">errors found</StatusBadge>
      </Match>
      <Match when={props.status === "aborted" || props.status === "canceled"}>
        <StatusBadge status="warning">{props.status}</StatusBadge>
      </Match>
      <Match when={true}>
        <span class="text-text-tertiary">{props.status || "unknown"}</span>
      </Match>
    </Switch>
  );
};
