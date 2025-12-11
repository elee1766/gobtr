import type { Component } from "solid-js";

type FlagProps = {
  active: boolean;
  label: string;
  tooltip: string;
  activeClass?: string;
};

export const Flag: Component<FlagProps> = (props) => {
  return (
    <span
      class={`px-1 rounded text-[10px] font-medium cursor-help ${
        props.active
          ? props.activeClass || "bg-warning-subtle text-warning-muted"
          : "bg-bg-base text-text-muted"
      }`}
      title={props.tooltip}
    >
      {props.label}
    </span>
  );
};
