import { Show, createMemo, createSignal, createResource, createEffect, onMount, onCleanup, Suspense } from "solid-js";
import type { TrackedFilesystem } from "%/v1/filesystem_pb";
import type { ScrubProgress } from "%/v1/scrub_pb";
import type { BalanceProgress } from "%/v1/balance_pb";
import { scrubClient, balanceClient } from "@/api/client";
import { formatNumber } from "@/lib/utils";
import { uiSettings } from "@/stores/ui";
import { Button, ProgressBar, ErrorSpan, WarnSpan, StatRow, FormattedBytes } from "@/components/ui";
import { ScrubConfirmModal, type ScrubOptions } from "@/components/ScrubConfirmModal";
import { BalanceConfirmModal, type BalanceOptions } from "@/components/BalanceConfirmModal";

export function MaintenanceTab(props: {
  fs: TrackedFilesystem;
  setError: (e: string) => void;
  onRefresh: () => void;
}) {
  // Separate resources for scrub and balance - they load independently
  const [scrubData, { refetch: refetchScrub }] = createResource(
    () => props.fs.path,
    (path) => scrubClient.getScrubStatus({ devicePath: path })
  );
  const [balanceData, { refetch: refetchBalance }] = createResource(
    () => props.fs.path,
    (path) => balanceClient.getBalanceStatus({ devicePath: path })
  );

  // Listen for refresh events
  onMount(() => {
    const handler = () => {
      refetchScrub();
      refetchBalance();
    };
    window.addEventListener("fs-refresh", handler);
    onCleanup(() => window.removeEventListener("fs-refresh", handler));
  });

  // Track polling state
  const [pollInterval, setPollInterval] = createSignal<ReturnType<typeof setInterval> | null>(null);
  const [pollingCountdown, setPollingCountdown] = createSignal(2);
  const [pollRetries, setPollRetries] = createSignal(0); // Retries before giving up if not running

  // Running state derived from resource data
  const scrubRunning = () => scrubData.latest?.isRunning ?? false;
  const balanceRunning = () => balanceData.latest?.isRunning ?? false;

  // Start polling if already running when data first loads
  // Use a flag to ensure this only triggers on initial load, not on poll updates
  let initialCheckDone = false;
  createEffect(() => {
    // Track the resources to react to their changes
    const scrub = scrubData();
    const balance = balanceData();
    // Only check on initial load (first time both have data)
    if (!initialCheckDone && scrub && balance) {
      initialCheckDone = true;
      if (scrub.isRunning || balance.isRunning) {
        startPolling();
      }
    }
  });

  // Start polling with countdown
  const startPolling = () => {
    if (pollInterval()) return; // Already polling

    // Refetch immediately
    refetchScrub();
    refetchBalance();
    setPollingCountdown(2);
    setPollRetries(5); // Poll at least 5 times before giving up

    // Start countdown and polling interval
    const interval = setInterval(() => {
      setPollingCountdown((c) => {
        if (c <= 1) {
          // Time to poll
          const stillRunning = scrubRunning() || balanceRunning();
          const retriesLeft = pollRetries();

          if (stillRunning || retriesLeft > 0) {
            refetchScrub();
            refetchBalance();
            if (!stillRunning) {
              setPollRetries(retriesLeft - 1);
            }
            return 2; // Reset countdown
          } else {
            // Stop polling - not running and no retries left
            clearInterval(interval);
            setPollInterval(null);
            return 2;
          }
        }
        return c - 1;
      });
    }, 1000);
    setPollInterval(interval);
  };

  const stopPolling = () => {
    const interval = pollInterval();
    if (interval) {
      clearInterval(interval);
      setPollInterval(null);
    }
  };

  // Cleanup on unmount
  onCleanup(() => stopPolling());

  const isPolling = () => pollInterval() !== null;

  const startScrub = async (opts: ScrubOptions) => {
    try {
      await scrubClient.startScrub({
        devicePath: props.fs.path,
        readonly: opts.readonly,
        limitBytesPerSec: opts.limitBytesPerSec,
        force: opts.force,
      });
      // Start polling immediately after scrub starts
      startPolling();
    } catch (e) {
      props.setError(String(e));
    }
  };

  const cancelScrub = async () => {
    try {
      await scrubClient.cancelScrub({ devicePath: props.fs.path });
      refetchScrub();
    } catch (e) {
      props.setError(String(e));
    }
  };

  const startBalance = async (opts: BalanceOptions) => {
    try {
      await balanceClient.startBalance({
        devicePath: props.fs.path,
        filters: opts.filters,
        limitPercent: opts.limitPercent,
        background: opts.background,
        dryRun: opts.dryRun,
        force: opts.force,
      });
      // Start polling immediately after balance starts
      startPolling();
    } catch (e) {
      props.setError(String(e));
    }
  };

  const cancelBalance = async () => {
    try {
      await balanceClient.cancelBalance({ devicePath: props.fs.path });
      refetchBalance();
    } catch (e) {
      props.setError(String(e));
    }
  };

  return (
    <div class="space-y-4">
      {/* Scrub section - loads independently */}
      <ScrubSection
        scrubData={scrubData}
        scrubRunning={scrubRunning()}
        onStart={startScrub}
        onCancel={cancelScrub}
        isPolling={isPolling()}
        pollingCountdown={pollingCountdown()}
      />

      {/* Balance section - loads independently */}
      <BalanceSection
        balanceData={balanceData}
        balanceRunning={balanceRunning()}
        onStart={startBalance}
        onCancel={cancelBalance}
        isPolling={isPolling()}
        pollingCountdown={pollingCountdown()}
      />
    </div>
  );
}

// Circular countdown indicator
function CountdownCircle(props: { countdown: number; total: number }) {
  const radius = 5;
  const circumference = 2 * Math.PI * radius;
  const progress = () => (props.countdown / props.total) * circumference;

  return (
    <svg width="14" height="14" class="inline-block" style={{ transform: "rotate(-90deg)" }}>
      <circle
        cx="7"
        cy="7"
        r={radius}
        fill="none"
        stroke="currentColor"
        stroke-width="2"
        class="text-border-default"
      />
      <circle
        cx="7"
        cy="7"
        r={radius}
        fill="none"
        stroke="currentColor"
        stroke-width="2"
        stroke-dasharray={circumference}
        stroke-dashoffset={circumference - progress()}
        class="text-primary"
      />
    </svg>
  );
}

function ScrubSection(props: {
  scrubData: ReturnType<typeof createResource<Awaited<ReturnType<typeof scrubClient.getScrubStatus>>>>[0];
  scrubRunning: boolean;
  onStart: (opts: ScrubOptions) => void;
  onCancel: () => void;
  isPolling: boolean;
  pollingCountdown: number;
}) {
  const showRawBytes = () => uiSettings().showRawBytes;
  const [scrubModalOpen, setScrubModalOpen] = createSignal(false);
  const [initialReadonly, setInitialReadonly] = createSignal(false);

  // Use .latest to avoid suspending when data exists
  const scrubStatus = () => props.scrubData.latest?.progress;

  const hasErrors = createMemo(() => {
    const s = scrubStatus();
    if (!s) return false;
    return s.readErrors > 0 || s.csumErrors > 0 || s.verifyErrors > 0 ||
      s.superErrors > 0 || s.mallocErrors > 0 || s.unverifiedErrors > 0 ||
      s.dataErrors > 0 || s.treeErrors > 0 || s.uncorrectableErrors > 0;
  });

  const scrubStatusBadge = createMemo(() => {
    const s = scrubStatus();
    if (!s) return { class: "text-text-muted", text: "never run" };
    if (props.scrubRunning) {
      return { class: "text-primary", text: `running ${s.progressPercent.toFixed(1)}%` };
    }
    if (s.status === "finished") {
      if (hasErrors()) {
        return { class: "text-error", text: "finished with errors" };
      }
      return { class: "text-success", text: "ok" };
    }
    if (s.status === "aborted" || s.status === "canceled") {
      return { class: "text-warning", text: s.status };
    }
    return { class: "text-text-tertiary", text: s.status || "unknown" };
  });

  return (
    <section>
      <div class="flex items-center justify-between mb-2">
        <div class="flex items-center gap-2">
          <h3 class="text-xs font-medium text-text-secondary">scrub</h3>
          <Show when={props.isPolling && props.scrubRunning}>
            <span class="flex items-center gap-1 text-[10px] text-text-muted">
              <CountdownCircle countdown={props.pollingCountdown} total={2} />
            </span>
          </Show>
        </div>
        <div class="flex space-x-1">
          <Show when={props.scrubRunning}>
            <Button variant="danger" onClick={props.onCancel}>cancel</Button>
          </Show>
          <Show when={!props.scrubRunning}>
            <Button variant="primary" onClick={() => { setInitialReadonly(false); setScrubModalOpen(true); }}>scrub</Button>
            <Button variant="soft" title="read-only (no repair)" onClick={() => { setInitialReadonly(true); setScrubModalOpen(true); }}>r/o</Button>
          </Show>
        </div>
      </div>

      {/* Progress bar if running */}
      <Show when={props.scrubRunning && scrubStatus()}>
        <ProgressBar value={scrubStatus()!.progressPercent} size="sm" colorClass="bg-primary" class="mb-2" />
      </Show>

      {/* Stats table - use .latest to avoid full re-render */}
      <Suspense fallback={<div class="text-xs text-text-tertiary">loading scrub status...</div>}>
        <Show when={props.scrubData.latest}>
          <ScrubStats
            scrubStatus={scrubStatus()}
            scrubRunning={props.scrubRunning}
            showRawBytes={showRawBytes()}
            statusBadge={scrubStatusBadge()}
            hasErrors={hasErrors()}
          />
        </Show>
      </Suspense>

      {/* Scrub confirmation modal */}
      <ScrubConfirmModal
        open={scrubModalOpen()}
        onClose={() => setScrubModalOpen(false)}
        onConfirm={(opts) => {
          setScrubModalOpen(false);
          props.onStart(opts);
        }}
        initialReadonly={initialReadonly()}
      />
    </section>
  );
}

function ScrubStats(props: {
  scrubStatus: ScrubProgress | undefined;
  scrubRunning: boolean;
  showRawBytes: boolean;
  statusBadge: { class: string; text: string };
  hasErrors: boolean;
}) {
  return (
    <table class="w-full text-xs">
      <tbody>
        {/* Status row */}
        <StatRow label="status">
          <span class={props.statusBadge.class}>{props.statusBadge.text}</span>
        </StatRow>
        <Show when={props.scrubStatus}>
          {(scrub) => (
            <>
              {/* Time row */}
              <StatRow label="time">
                <Show when={scrub().startedAt > 0n} fallback="-">
                  <span>{new Date(Number(scrub().startedAt) * 1000).toLocaleString("sv-SE", { dateStyle: "short", timeStyle: "short" })}</span>
                  <Show when={scrub().finishedAt > 0n && !props.scrubRunning}>
                    <span class="text-text-muted"> → {new Date(Number(scrub().finishedAt) * 1000).toLocaleTimeString("sv-SE", { timeStyle: "short" })}</span>
                  </Show>
                  <Show when={scrub().duration}>
                    <span class="text-text-muted ml-2">({scrub().duration})</span>
                  </Show>
                </Show>
              </StatRow>
              {/* Scrubbed data row */}
              <StatRow label="scrubbed">
                <span class="font-mono">
                  <FormattedBytes bytes={scrub().bytesScrubbed} showRaw={props.showRawBytes} />
                  <Show when={scrub().totalBytes > 0n}>
                    <span class="text-text-muted mx-1">/</span>
                    <FormattedBytes bytes={scrub().totalBytes} showRaw={props.showRawBytes} />
                  </Show>
                </span>
              </StatRow>
              {/* Data extents row */}
              <StatRow label="data">
                <span class="font-mono">
                  <FormattedBytes bytes={scrub().dataBytesScrubbed} showRaw={props.showRawBytes} />
                  <span class="text-text-muted ml-2">(</span>
                  <span>{formatNumber(scrub().dataExtentsScrubbed)}</span>
                  <span class="text-text-muted ml-0.5">extents)</span>
                </span>
              </StatRow>
              {/* Tree extents row */}
              <StatRow label="tree">
                <span class="font-mono">
                  <FormattedBytes bytes={scrub().treeBytesScrubbed} showRaw={props.showRawBytes} />
                  <span class="text-text-muted ml-2">(</span>
                  <span>{formatNumber(scrub().treeExtentsScrubbed)}</span>
                  <span class="text-text-muted ml-0.5">extents)</span>
                </span>
              </StatRow>
              {/* Errors row */}
              <StatRow label="errors">
                <Show when={!props.hasErrors} fallback={
                  <div class="flex flex-wrap gap-x-3 gap-y-1">
                    <ErrorSpan label="data" count={scrub().dataErrors} />
                    <ErrorSpan label="tree" count={scrub().treeErrors} />
                    <ErrorSpan label="read" count={scrub().readErrors} />
                    <ErrorSpan label="csum" count={scrub().csumErrors} />
                    <ErrorSpan label="verify" count={scrub().verifyErrors} />
                    <ErrorSpan label="super" count={scrub().superErrors} />
                    <ErrorSpan label="malloc" count={scrub().mallocErrors} />
                    <ErrorSpan label="unverified" count={scrub().unverifiedErrors} />
                    <ErrorSpan label="uncorrectable" count={scrub().uncorrectableErrors} critical />
                    <WarnSpan label="corrected" count={scrub().correctedErrors} />
                  </div>
                }>
                  <span class="text-success">none</span>
                </Show>
              </StatRow>
              {/* Checksums row */}
              <StatRow label="csum">
                <span class="font-mono">
                  <span class="text-text-tertiary">no_csum:</span>
                  <span>{formatNumber(scrub().noCsum)}</span>
                  <span class="text-text-tertiary ml-3">discards:</span>
                  <span>{formatNumber(scrub().csumDiscards)}</span>
                </span>
              </StatRow>
              {/* Last physical row */}
              <Show when={scrub().lastPhysical > 0n}>
                <StatRow label="position">
                  <span class="font-mono">
                    <FormattedBytes bytes={scrub().lastPhysical} showRaw={props.showRawBytes} />
                  </span>
                </StatRow>
              </Show>
            </>
          )}
        </Show>
      </tbody>
    </table>
  );
}

function BalanceSection(props: {
  balanceData: ReturnType<typeof createResource<Awaited<ReturnType<typeof balanceClient.getBalanceStatus>>>>[0];
  balanceRunning: boolean;
  onStart: (opts: BalanceOptions) => void;
  onCancel: () => void;
  isPolling: boolean;
  pollingCountdown: number;
}) {
  const showRawBytes = () => uiSettings().showRawBytes;
  const [balanceModalOpen, setBalanceModalOpen] = createSignal(false);

  // Use .latest to avoid suspending when data exists
  const balanceStatus = () => props.balanceData.latest?.progress;

  const balanceStatusBadge = createMemo(() => {
    const s = balanceStatus();
    if (!s || s.status === "idle" || s.status === "never_run") {
      return { class: "text-text-muted", text: "idle" };
    }
    if (props.balanceRunning) {
      const pct = s.progressPercent?.toFixed(1) || "0";
      return { class: "text-primary", text: `running ${pct}%` };
    }
    if (s.status === "paused") {
      return { class: "text-warning", text: "paused" };
    }
    if (s.status === "finished") {
      return { class: "text-success", text: "finished" };
    }
    if (s.status === "cancelled") {
      return { class: "text-warning", text: "cancelled" };
    }
    return { class: "text-text-tertiary", text: s.status || "unknown" };
  });

  return (
    <section>
      <div class="flex items-center justify-between mb-2">
        <div class="flex items-center gap-2">
          <h3 class="text-xs font-medium text-text-secondary">balance</h3>
          <Show when={props.isPolling && props.balanceRunning}>
            <span class="flex items-center gap-1 text-[10px] text-text-muted">
              <CountdownCircle countdown={props.pollingCountdown} total={2} />
            </span>
          </Show>
        </div>
        <div class="flex space-x-1">
          <Show when={props.balanceRunning}>
            <Button variant="danger" onClick={props.onCancel}>cancel</Button>
          </Show>
          <Show when={!props.balanceRunning}>
            <Button variant="primary" onClick={() => setBalanceModalOpen(true)}>balance</Button>
          </Show>
        </div>
      </div>

      {/* Progress bar if running */}
      <Show when={props.balanceRunning && balanceStatus()}>
        <ProgressBar value={balanceStatus()!.progressPercent} size="sm" colorClass="bg-primary" class="mb-2" />
      </Show>

      {/* Stats table - use .latest to avoid full re-render */}
      <Suspense fallback={<div class="text-xs text-text-tertiary">loading balance status...</div>}>
        <Show when={props.balanceData.latest}>
          <BalanceStats
            balanceStatus={balanceStatus()}
            balanceRunning={props.balanceRunning}
            showRawBytes={showRawBytes()}
            statusBadge={balanceStatusBadge()}
          />
        </Show>
      </Suspense>

      {/* Balance confirmation modal */}
      <BalanceConfirmModal
        open={balanceModalOpen()}
        onClose={() => setBalanceModalOpen(false)}
        onConfirm={(opts) => {
          setBalanceModalOpen(false);
          props.onStart(opts);
        }}
      />
    </section>
  );
}

function BalanceStats(props: {
  balanceStatus: BalanceProgress | undefined;
  balanceRunning: boolean;
  showRawBytes: boolean;
  statusBadge: { class: string; text: string };
}) {
  return (
    <table class="w-full text-xs">
      <tbody>
        {/* Status row */}
        <StatRow label="status">
          <span class={props.statusBadge.class}>{props.statusBadge.text}</span>
        </StatRow>
        <Show when={props.balanceStatus && props.balanceStatus.status !== "idle"}>
          {(bal) => (
            <>
              {/* Progress row */}
              <Show when={bal().totalChunks > 0n}>
                <StatRow label="progress">
                  <span class="font-mono">
                    <span>{formatNumber(bal().relocated)}</span>
                    <span class="text-text-muted mx-1">/</span>
                    <span>{formatNumber(bal().totalChunks)}</span>
                    <span class="text-text-muted ml-1">chunks</span>
                    <Show when={bal().left > 0n}>
                      <span class="text-text-muted ml-2">({formatNumber(bal().left)} left)</span>
                    </Show>
                  </span>
                </StatRow>
              </Show>
              {/* Considered row */}
              <Show when={bal().considered > 0n}>
                <StatRow label="considered">
                  <span class="font-mono">{formatNumber(bal().considered)} chunks</span>
                </StatRow>
              </Show>
              {/* Size relocated row */}
              <Show when={bal().sizeRelocated > 0n}>
                <StatRow label="relocated">
                  <span class="font-mono">
                    <FormattedBytes bytes={bal().sizeRelocated} showRaw={props.showRawBytes} />
                  </span>
                </StatRow>
              </Show>
              {/* Errors row */}
              <Show when={bal().softErrors > 0}>
                <StatRow label="errors">
                  <WarnSpan label="soft" count={bal().softErrors} />
                </StatRow>
              </Show>
              {/* Duration row */}
              <Show when={bal().duration}>
                <StatRow label="duration">
                  <span class="font-mono">{bal().duration}</span>
                </StatRow>
              </Show>
            </>
          )}
        </Show>
      </tbody>
    </table>
  );
}
