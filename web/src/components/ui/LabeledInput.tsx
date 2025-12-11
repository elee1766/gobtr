import type { Component, JSX } from "solid-js";
import { Show, splitProps } from "solid-js";
import { cn } from "@/lib/utils";

type LabeledInputProps = {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  type?: string;
  mono?: boolean;
  hint?: string;
  class?: string;
  disabled?: boolean;
} & Omit<JSX.InputHTMLAttributes<HTMLInputElement>, "onChange" | "value" | "class">;

export const LabeledInput: Component<LabeledInputProps> = (props) => {
  const [local, others] = splitProps(props, [
    "label", "value", "onChange", "placeholder", "type", "mono", "hint", "class", "disabled"
  ]);

  return (
    <div class={cn("space-y-1", local.class)}>
      <label class="block text-xs text-text-tertiary">{local.label}</label>
      <input
        type={local.type ?? "text"}
        class={cn(
          "w-full px-2 py-1 text-xs border border-border-strong",
          local.mono && "font-mono",
          local.disabled && "opacity-50"
        )}
        placeholder={local.placeholder}
        value={local.value}
        disabled={local.disabled}
        onInput={(e) => local.onChange(e.currentTarget.value)}
        {...others}
      />
      <Show when={local.hint}>
        <div class="text-xs text-text-muted">{local.hint}</div>
      </Show>
    </div>
  );
};
