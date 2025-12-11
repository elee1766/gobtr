import type { Component } from "solid-js";
import { cn, formatNumber } from "@/lib/utils";

export const ErrorSpan: Component<{
  label: string;
  count: number;
  critical?: boolean;
}> = (props) => {
  if (props.count === 0) return null;
  return (
    <span class={cn("text-error", props.critical && "font-semibold")}>
      {props.label}:{formatNumber(props.count)}
    </span>
  );
};

export const WarnSpan: Component<{
  label: string;
  count: number;
}> = (props) => {
  if (props.count === 0) return null;
  return <span class="text-warning">{props.label}:{formatNumber(props.count)}</span>;
};
