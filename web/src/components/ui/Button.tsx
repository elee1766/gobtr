import type { Component, JSX } from "solid-js";
import { splitProps } from "solid-js";
import { cn } from "@/lib/utils";

type ButtonVariant = "primary" | "danger" | "soft" | "ghost";

type ButtonProps = {
  variant?: ButtonVariant;
  children: JSX.Element;
} & JSX.ButtonHTMLAttributes<HTMLButtonElement>;

const variantClasses: Record<ButtonVariant, string> = {
  primary: "bg-primary text-text-inverse hover:bg-primary-hover",
  danger: "bg-error text-text-inverse hover:bg-error-hover",
  soft: "bg-interactive-soft text-text-inverse hover:bg-interactive",
  ghost: "text-text-tertiary hover:bg-bg-surface-raised",
};

export const Button: Component<ButtonProps> = (props) => {
  const [local, others] = splitProps(props, ["variant", "children", "class"]);

  return (
    <button
      class={cn(
        "px-2 py-1 text-xs cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed",
        variantClasses[local.variant ?? "primary"],
        local.class
      )}
      {...others}
    >
      {local.children}
    </button>
  );
};
