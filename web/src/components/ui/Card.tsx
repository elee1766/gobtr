import type { Component, JSX } from "solid-js";
import { Show, splitProps } from "solid-js";
import { cn } from "@/lib/utils";

type CardProps = {
  header?: string;
  headerRight?: JSX.Element;
  children: JSX.Element;
  divided?: boolean;
  class?: string;
};

export const Card: Component<CardProps> = (props) => {
  const [local] = splitProps(props, ["header", "headerRight", "children", "divided", "class"]);

  return (
    <div class={cn("bg-bg-surface border border-border-default", local.class)}>
      <Show when={local.header || local.headerRight}>
        <div class="px-2 py-1 bg-bg-surface-raised border-b border-border-subtle flex justify-between items-center">
          <Show when={local.header}>
            <span class="text-xs text-text-tertiary">{local.header}</span>
          </Show>
          <Show when={local.headerRight}>
            {local.headerRight}
          </Show>
        </div>
      </Show>
      <div classList={{ "divide-y divide-border-subtle": local.divided }}>
        {local.children}
      </div>
    </div>
  );
};
