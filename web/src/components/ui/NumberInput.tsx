import type { Component } from "solid-js";
import { splitProps } from "solid-js";
import { cn } from "@/lib/utils";

type NumberInputProps = {
  value: number;
  onChange: (value: number) => void;
  min?: number;
  max?: number;
  step?: number;
  suffix?: string;
  class?: string;
  disabled?: boolean;
};

export const NumberInput: Component<NumberInputProps> = (props) => {
  const [local] = splitProps(props, ["value", "onChange", "min", "max", "step", "suffix", "class", "disabled"]);

  const step = () => local.step ?? 1;
  const canDecrement = () => local.min === undefined || local.value > local.min;
  const canIncrement = () => local.max === undefined || local.value < local.max;

  const increment = () => {
    if (canIncrement()) {
      const newVal = local.value + step();
      local.onChange(local.max !== undefined ? Math.min(newVal, local.max) : newVal);
    }
  };

  const decrement = () => {
    if (canDecrement()) {
      const newVal = local.value - step();
      local.onChange(local.min !== undefined ? Math.max(newVal, local.min) : newVal);
    }
  };

  const handleInput = (e: InputEvent & { currentTarget: HTMLInputElement }) => {
    const val = parseInt(e.currentTarget.value);
    if (!isNaN(val)) {
      let clamped = val;
      if (local.min !== undefined) clamped = Math.max(clamped, local.min);
      if (local.max !== undefined) clamped = Math.min(clamped, local.max);
      local.onChange(clamped);
    }
  };

  const handleBlur = (e: FocusEvent & { currentTarget: HTMLInputElement }) => {
    const val = parseInt(e.currentTarget.value);
    if (isNaN(val)) {
      e.currentTarget.value = String(local.value);
    }
  };

  const buttonClass = "px-2 py-1 text-xs cursor-pointer select-none disabled:opacity-30 disabled:cursor-not-allowed";

  return (
    <div class={cn("inline-flex items-center border border-border-strong bg-bg-surface", local.class)}>
      <button
        type="button"
        class={cn(buttonClass, "text-text-tertiary hover:bg-bg-surface-raised border-r border-border-strong")}
        disabled={local.disabled || !canDecrement()}
        onClick={decrement}
      >
        −
      </button>
      <input
        type="number"
        min={local.min}
        max={local.max}
        step={local.step}
        class="w-12 px-1 py-1 text-xs text-center bg-transparent border-none outline-none [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
        value={local.value}
        disabled={local.disabled}
        onInput={handleInput}
        onBlur={handleBlur}
      />
      {local.suffix && (
        <span class="text-xs text-text-tertiary pr-1">{local.suffix}</span>
      )}
      <button
        type="button"
        class={cn(buttonClass, "text-text-tertiary hover:bg-bg-surface-raised border-l border-border-strong")}
        disabled={local.disabled || !canIncrement()}
        onClick={increment}
      >
        +
      </button>
    </div>
  );
};
