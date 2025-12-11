import { Show } from "solid-js";
import type { DeviceStats } from "%/v1/filesystem_pb";
import { FormattedBytes } from "@/components/ui";
import { formatNumber } from "@/lib/utils";

export function DeviceRow(props: { device: DeviceStats; showRaw?: boolean }) {
  const dev = () => props.device;

  const usedPct = () => {
    if (dev().totalBytes === 0n) return 0;
    return Number((dev().usedBytes * 100n) / dev().totalBytes);
  };

  const totalErrors = () =>
    dev().writeErrors + dev().readErrors + dev().flushErrors +
    dev().corruptionErrors + dev().generationErrors;

  const barColor = () => {
    const pct = usedPct();
    if (pct > 90) return "bg-error-soft";
    if (pct > 75) return "bg-warning-soft";
    return "bg-primary-soft";
  };

  return (
    <div class="px-2 py-2">
      {/* Device path and ID */}
      <div class="flex items-center justify-between text-xs mb-1">
        <span class="font-mono text-text-default">{dev().devicePath}</span>
        <Show when={dev().deviceId}>
          <span class="text-text-muted">devid {dev().deviceId}</span>
        </Show>
      </div>

      {/* Usage bar */}
      <div class="h-2 bg-bg-muted rounded overflow-hidden mb-1">
        <div class={`${barColor()} h-full`} style={{ width: `${usedPct()}%` }} />
      </div>

      {/* Usage stats */}
      <div class="flex items-center justify-between text-xs">
        <div class="flex items-center space-x-3 text-text-secondary">
          <span>
            <span class="text-text-muted">used </span>
            <span class="font-mono"><FormattedBytes bytes={dev().usedBytes} showRaw={props.showRaw || false} /></span>
          </span>
          <span>
            <span class="text-text-muted">total </span>
            <span class="font-mono"><FormattedBytes bytes={dev().totalBytes} showRaw={props.showRaw || false} /></span>
          </span>
        </div>
        <Show when={totalErrors() > 0n} fallback={
          <span class="text-success">no errors</span>
        }>
          <span class="text-error">{formatNumber(totalErrors())} errors</span>
        </Show>
      </div>

      {/* Error breakdown if any */}
      <Show when={totalErrors() > 0n}>
        <div class="mt-1 text-xs text-text-tertiary flex flex-wrap gap-x-3">
          <Show when={dev().readErrors > 0n}>
            <span class="text-error">read:{formatNumber(dev().readErrors)}</span>
          </Show>
          <Show when={dev().writeErrors > 0n}>
            <span class="text-error">write:{formatNumber(dev().writeErrors)}</span>
          </Show>
          <Show when={dev().flushErrors > 0n}>
            <span class="text-error">flush:{formatNumber(dev().flushErrors)}</span>
          </Show>
          <Show when={dev().corruptionErrors > 0n}>
            <span class="text-error">corruption:{formatNumber(dev().corruptionErrors)}</span>
          </Show>
          <Show when={dev().generationErrors > 0n}>
            <span class="text-error">generation:{formatNumber(dev().generationErrors)}</span>
          </Show>
        </div>
      </Show>
    </div>
  );
}
