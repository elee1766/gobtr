import { Component, JSX, splitProps } from "solid-js";

export interface StatRowProps {
  label: string;
  children: JSX.Element;
  align?: "top" | "middle";
}

export const StatRow: Component<StatRowProps> = (props) => {
  const [local, others] = splitProps(props, ["label", "children", "align"]);
  const align = () => local.align || "middle";

  return (
    <tr class="border-b border-border-subtle" {...others}>
      <td class={`px-2 py-1 text-text-tertiary w-20 align-${align()}`}>{local.label}</td>
      <td class={`px-2 py-1 text-text-default align-${align()}`}>{local.children}</td>
    </tr>
  );
};
