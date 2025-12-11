import type { Component } from "solid-js";
import { splitProps } from "solid-js";
import { cn } from "@/lib/utils";

type ProgressSize = "sm" | "md" | "lg";

type ProgressBarProps = {
  value: number; // 0-100
  size?: ProgressSize;
  /** Use threshold-based coloring (red > 90%, yellow > 75%, blue otherwise) */
  thresholdColors?: boolean;
  /** Custom color class for the fill */
  colorClass?: string;
  class?: string;
};

const sizeClasses: Record<ProgressSize, string> = {
  sm: "h-1",
  md: "h-1.5",
  lg: "h-2.5",
};

export const ProgressBar: Component<ProgressBarProps> = (props) => {
  const [local] = splitProps(props, ["value", "size", "thresholdColors", "colorClass", "class"]);

  const pct = () => Math.min(100, Math.max(0, local.value));

  const fillColor = () => {
    if (local.colorClass) return local.colorClass;
    if (local.thresholdColors) {
      if (pct() > 90) return "bg-error-soft";
      if (pct() > 75) return "bg-warning-soft";
      return "bg-primary-soft";
    }
    return "bg-primary-soft";
  };

  return (
    <div class={cn("bg-bg-muted rounded overflow-hidden", sizeClasses[local.size ?? "md"], local.class)}>
      <div
        class={cn("h-full", fillColor())}
        style={{ width: `${pct()}%` }}
      />
    </div>
  );
};
