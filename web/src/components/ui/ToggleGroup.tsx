import { For, splitProps } from "solid-js";
import { cn } from "@/lib/utils";

interface ToggleOption<T> {
  label: string;
  value: T;
}

interface ToggleGroupProps<T> {
  options: ToggleOption<T>[];
  value: T;
  onChange: (value: T) => void;
  disabled?: boolean;
  class?: string;
}

export function ToggleGroup<T>(props: ToggleGroupProps<T>) {
  const [local] = splitProps(props, ["options", "value", "onChange", "disabled", "class"]);

  return (
    <div class={cn("inline-flex border border-border-strong rounded overflow-hidden", local.class)}>
      <For each={local.options}>
        {(option) => (
          <button
            class={cn(
              "px-2 py-1 text-xs cursor-pointer",
              local.value === option.value
                ? "bg-interactive text-text-inverse"
                : "bg-bg-surface text-text-tertiary hover:bg-bg-surface-raised",
              local.disabled && "opacity-50 cursor-not-allowed"
            )}
            disabled={local.disabled}
            onClick={() => local.onChange(option.value)}
          >
            {option.label}
          </button>
        )}
      </For>
    </div>
  );
}
