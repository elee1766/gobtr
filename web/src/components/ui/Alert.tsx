import type { Component, JSX } from "solid-js";
import { Show, splitProps } from "solid-js";
import { cn } from "@/lib/utils";

type AlertType = "error" | "success" | "warning" | "info";

type AlertProps = {
  type: AlertType;
  children: JSX.Element;
  class?: string;
  onDismiss?: () => void;
};

const typeClasses: Record<AlertType, string> = {
  error: "text-error bg-error-subtle",
  success: "bg-success-subtle border border-success-muted text-success",
  warning: "text-warning bg-warning-subtle",
  info: "text-primary bg-primary-subtle",
};

export const Alert: Component<AlertProps> = (props) => {
  const [local] = splitProps(props, ["type", "children", "class", "onDismiss"]);

  return (
    <div class={cn("text-xs px-2 py-1", typeClasses[local.type], local.class)}>
      <div class="flex items-center justify-between">
        <span>{local.children}</span>
        <Show when={local.onDismiss}>
          <button
            class="ml-2 text-current opacity-60 hover:opacity-100 cursor-pointer"
            onClick={local.onDismiss}
          >
            ×
          </button>
        </Show>
      </div>
    </div>
  );
};
