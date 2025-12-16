import { Show, For, createMemo, createResource, onMount, onCleanup, Suspense } from "solid-js";
import { A } from "@solidjs/router";
import { Poline } from "poline";
import type { TrackedFilesystem } from "%/v1/filesystem_pb";
import { filesystemClient, subvolumeClient, scrubClient } from "@/api/client";
import { formatBytes, countBackups, formatNumber } from "@/lib/utils";
import { Tooltip, FormattedBytes, StatRow } from "@/components/ui";
import { uiSettings } from "@/stores/ui";

// Anchor colors for each allocation type
const ALLOC_TYPE_ANCHORS: Record<string, [[number, number, number], [number, number, number]]> = {
  Data: [[210, 0.80, 0.40], [220, 0.55, 0.65]],
  Metadata: [[145, 0.70, 0.35], [155, 0.50, 0.60]],
  System: [[30, 0.85, 0.45], [40, 0.60, 0.60]],
  Unallocated: [[280, 0.50, 0.45], [290, 0.35, 0.65]],
};

function shuffleForContrast<T>(arr: T[]): T[] {
  if (arr.length <= 2) return arr;
  const result: T[] = [];
  const mid = Math.ceil(arr.length / 2);
  for (let i = 0; i < mid; i++) {
    result.push(arr[i]);
    if (i + mid < arr.length) result.push(arr[i + mid]);
  }
  return result;
}

function getUsageBarColor(allocType: string): string {
  const anchors = ALLOC_TYPE_ANCHORS[allocType] || ALLOC_TYPE_ANCHORS.Data;
  const [h, s, l] = anchors[1];
  return `hsl(${h}, ${s * 100}%, ${l * 100}%)`;
}

function generateTypeColors(devicePaths: string[], allocType: string): Map<string, string> {
  const colors = new Map<string, string>();
  const count = devicePaths.length;
  if (count === 0) return colors;
  const anchors = ALLOC_TYPE_ANCHORS[allocType] || ALLOC_TYPE_ANCHORS.Data;
  if (count === 1) {
    const [h, s, l] = anchors[0];
    colors.set(devicePaths[0], `hsl(${h}, ${s * 100}%, ${l * 100}%)`);
    return colors;
  }
  const poline = new Poline({ anchorColors: anchors, numPoints: count });
  const palette = shuffleForContrast(poline.colorsCSS);
  devicePaths.forEach((path, i) => colors.set(path, palette[i] || "#6b7280"));
  return colors;
}

export function OverviewTab(props: { fs: TrackedFilesystem }) {
  // Each data type is its own resource - they load independently
  const [usage, { refetch: refetchUsage }] = createResource(
    () => props.fs.path,
    (path) => filesystemClient.getFilesystemUsage({ devicePath: path }).then(r => r.usage)
  );
  const [subvolumes, { refetch: refetchSubvols }] = createResource(
    () => props.fs.path,
    (path) => subvolumeClient.listSubvolumes({ mountPath: path }).then(r => r.subvolumes)
  );
  const [scrubData, { refetch: refetchScrub }] = createResource(
    () => props.fs.path,
    (path) => scrubClient.getScrubStatus({ devicePath: path })
  );
  const [errors, { refetch: refetchErrors }] = createResource(
    () => props.fs.path,
    (path) => filesystemClient.getErrors({ device: path, limit: 1000 }).then(r => r.errors)
  );
  const [deviceStats, { refetch: refetchStats }] = createResource(
    () => props.fs.path,
    (path) => filesystemClient.getDeviceStats({ devicePath: path }).then(r => r.devices)
  );

  onMount(() => {
    const handler = () => {
      refetchUsage();
      refetchSubvols();
      refetchScrub();
      refetchErrors();
      refetchStats();
    };
    window.addEventListener("fs-refresh", handler);
    onCleanup(() => window.removeEventListener("fs-refresh", handler));
  });

  const showRaw = () => uiSettings().showRawBytes;

  // Use .latest to avoid suspending - shows stale data while refreshing
  const subvolCount = createMemo(() => {
    const subs = subvolumes.latest;
    if (!subs) return 0;
    return subs.length - countBackups(subs, props.fs.btrbkSnapshotDir);
  });
  const backupCount = createMemo(() => {
    const subs = subvolumes.latest;
    if (!subs) return 0;
    return countBackups(subs, props.fs.btrbkSnapshotDir);
  });
  // Combine database errors and device stats errors
  const errorCount = createMemo(() => {
    const dbErrors = errors.latest?.length ?? 0;
    // Also count device stats errors (excluding "total" entry)
    const stats = deviceStats.latest;
    let deviceErrors = 0n;
    if (stats) {
      for (const dev of stats) {
        if (dev.devicePath === "total") continue;
        deviceErrors += dev.writeErrors + dev.readErrors + dev.flushErrors + dev.corruptionErrors + dev.generationErrors;
      }
    }
    return dbErrors + Number(deviceErrors);
  });

  const scrubInfo = createMemo(() => {
    const data = scrubData.latest;
    if (!data) return { text: "...", class: "text-text-muted" };
    if (data.isRunning) return { text: "running", class: "text-primary" };
    if (!data.progress) return { text: "never run", class: "text-text-muted" };
    const s = data.progress;
    const hasErrors = s.uncorrectableErrors > 0 || s.readErrors > 0 || s.csumErrors > 0;
    if (s.status === "finished") {
      return hasErrors
        ? { text: "errors found", class: "text-error" }
        : { text: "ok", class: "text-success" };
    }
    return { text: s.status || "unknown", class: "text-text-tertiary" };
  });

  const deviceCount = createMemo(() => {
    const stats = deviceStats.latest;
    const u = usage.latest;
    // Filter out the "total" entry from device count
    const statsCount = stats?.filter((d: any) => d.devicePath !== "total").length || 0;
    let allocDeviceCount = 0;
    if (u) {
      const paths = new Set<string>();
      for (const alloc of u.allocations) {
        for (const dev of alloc.devices) paths.add(dev.devicePath);
      }
      allocDeviceCount = paths.size;
    }
    return Math.max(statsCount, allocDeviceCount);
  });

  return (
    <div class="space-y-3">
      {/* Summary Cards - use .latest for instant render */}
      <div class="grid grid-cols-4 gap-2">
        <A href={`/fs/${props.fs.id}/subvolumes`} class="block">
          <div class="bg-bg-surface border border-border-default p-2 text-center hover:bg-bg-surface-raised">
            <div class="text-xs text-text-tertiary">subvolumes</div>
            <div class="text-sm font-medium text-text-default">{formatNumber(subvolCount())}</div>
          </div>
        </A>
        <Show when={props.fs.btrbkSnapshotDir} fallback={
          <div class="bg-bg-surface border border-border-default p-2 text-center">
            <div class="text-xs text-text-tertiary">backups</div>
            <div class="text-sm font-medium text-text-muted">-</div>
          </div>
        }>
          <A href={`/fs/${props.fs.id}/subvolumes`} class="block">
            <div class="bg-bg-surface border border-border-default p-2 text-center hover:bg-bg-surface-raised">
              <div class="text-xs text-text-tertiary">backups</div>
              <div class="text-sm font-medium text-text-default">{formatNumber(backupCount())}</div>
            </div>
          </A>
        </Show>
        <A href={`/fs/${props.fs.id}/errors`} class="block">
          <div class="bg-bg-surface border border-border-default p-2 text-center hover:bg-bg-surface-raised">
            <div class="text-xs text-text-tertiary">errors</div>
            <div class={`text-sm font-medium ${errorCount() > 0 ? "text-error" : "text-success"}`}>
              {formatNumber(errorCount())}
            </div>
          </div>
        </A>
        <A href={`/fs/${props.fs.id}/maintenance`} class="block">
          <div class="bg-bg-surface border border-border-default p-2 text-center hover:bg-bg-surface-raised">
            <div class="text-xs text-text-tertiary">scrub</div>
            <div class={`text-sm font-medium ${scrubInfo().class}`}>{scrubInfo().text}</div>
          </div>
        </A>
      </div>

      {/* Filesystem Info + Last Scrub */}
      <div class="grid grid-cols-2 gap-2">
        {/* Filesystem Info */}
        <div class="bg-bg-surface border border-border-default">
          <div class="px-2 py-1 bg-bg-surface-raised border-b border-border-subtle text-xs text-text-tertiary">
            filesystem
          </div>
          <table class="w-full text-xs">
            <tbody>
              <StatRow label="path" align="top">
                <span class="font-mono">{props.fs.path}</span>
              </StatRow>
              <Show when={props.fs.label}>
                <StatRow label="label" align="top">{props.fs.label}</StatRow>
              </Show>
              <Show when={props.fs.btrbkSnapshotDir}>
                <StatRow label="btrbk" align="top">
                  <span class="font-mono">{props.fs.btrbkSnapshotDir}</span>
                </StatRow>
              </Show>
              <StatRow label="devices" align="top">{formatNumber(deviceCount())}</StatRow>
            </tbody>
          </table>
        </div>

        {/* Last Scrub */}
        <div class="bg-bg-surface border border-border-default">
          <div class="px-2 py-1 bg-bg-surface-raised border-b border-border-subtle text-xs text-text-tertiary">
            last scrub
          </div>
          <Suspense fallback={<div class="px-2 py-1 text-xs text-text-muted">loading...</div>}>
            <Show when={scrubData()} fallback={<div class="px-2 py-1 text-xs text-text-muted">loading...</div>}>
              {(data) => (
                <Show when={data().progress} fallback={<div class="px-2 py-1 text-xs text-text-muted">never run</div>}>
                  {(s) => {
                    const scrub = () => s();
                    return (
                      <table class="w-full text-xs">
                        <tbody>
                          <StatRow label="status" align="top">
                            <span class={scrubInfo().class}>{scrubInfo().text}</span>
                          </StatRow>
                          <Show when={scrub().startedAt > 0n}>
                            <StatRow label="started" align="top">
                              {new Date(Number(scrub().startedAt) * 1000).toLocaleString()}
                            </StatRow>
                          </Show>
                          <Show when={scrub().duration}>
                            <StatRow label="duration" align="top">{scrub().duration}</StatRow>
                          </Show>
                          <StatRow label="errors" align="top">
                            <Show when={scrub().uncorrectableErrors > 0 || scrub().readErrors > 0 || scrub().csumErrors > 0} fallback={
                              <span class="text-success">none</span>
                            }>
                              <span class="text-error">
                                {scrub().uncorrectableErrors > 0 && `uncorrectable:${formatNumber(scrub().uncorrectableErrors)} `}
                                {scrub().readErrors > 0 && `read:${formatNumber(scrub().readErrors)} `}
                                {scrub().csumErrors > 0 && `csum:${formatNumber(scrub().csumErrors)}`}
                              </span>
                            </Show>
                          </StatRow>
                        </tbody>
                      </table>
                    );
                  }}
                </Show>
              )}
            </Show>
          </Suspense>
        </div>
      </div>

      {/* Space & Allocations - separate Suspense */}
      <Suspense fallback={<div class="bg-bg-surface border border-border-default p-2 text-xs text-text-tertiary">loading usage...</div>}>
        <Show when={usage()}>
          {(u) => <SpaceAllocationsCard usage={u()} showRaw={showRaw()} />}
        </Show>
      </Suspense>

      {/* Device Stats - separate Suspense */}
      <Suspense fallback={<div class="bg-bg-surface border border-border-default p-2 text-xs text-text-tertiary">loading devices...</div>}>
        <Show when={deviceStats() && usage()}>
          <DevicesCard usage={usage()!} deviceStats={deviceStats()!} showRaw={showRaw()} />
        </Show>
      </Suspense>
    </div>
  );
}

function SpaceAllocationsCard(props: { usage: any; showRaw: boolean }) {
  const deviceColors = createMemo(() => {
    const colors = new Map<string, string>();
    for (const alloc of props.usage.allocations) {
      if (!alloc.devices || alloc.devices.length === 0) continue;
      const paths = alloc.devices.map((d: any) => d.devicePath);
      const typeColors = generateTypeColors(paths, alloc.type);
      for (const [path, color] of typeColors) {
        colors.set(`${alloc.type}:${path}`, color);
      }
    }
    return colors;
  });

  const getDeviceColor = (allocType: string, devicePath: string) =>
    deviceColors().get(`${allocType}:${devicePath}`) || "#6b7280";

  const totalSpace = createMemo(() => ({
    total: props.usage.deviceSize,
    allocated: props.usage.deviceAllocated,
    unallocated: props.usage.deviceUnallocated,
    used: props.usage.used,
  }));

  const allocatedPct = createMemo(() => {
    const space = totalSpace();
    if (space.total === 0n) return 0;
    return Number((space.allocated * 100n) / space.total);
  });

  const allocations = createMemo(() =>
    props.usage.allocations.filter((a: any) => a.type !== "GlobalReserve" && !a.type.toLowerCase().includes("free"))
  );

  const globalReserve = createMemo(() =>
    props.usage.allocations.find((a: any) => a.type === "GlobalReserve")
  );

  return (
    <div class="bg-bg-surface border border-border-default">
      <div class="px-2 py-1 bg-bg-surface-raised border-b border-border-subtle text-xs text-text-tertiary">
        space & allocations
      </div>
      <div class="flex">
        {/* Vertical meters */}
        <div class="p-2 border-r border-border-subtle">
          <div class="flex gap-2" style={{ height: "120px" }}>
            <For each={allocations().filter((a: any) => a.type !== "Unallocated")}>
              {(alloc) => {
                const pct = () => alloc.size > 0n ? Number((alloc.used * 100n) / alloc.size) : 0;
                const deviceTooltip = () => alloc.devices.map((d: any) => `${d.devicePath.split("/").pop()}: ${formatBytes(d.size)}`).join(" • ");
                return (
                  <Tooltip text={`${alloc.type} (${alloc.profile}): ${formatBytes(alloc.used)} / ${formatBytes(alloc.size)} (${pct()}%)\n${deviceTooltip()}`}>
                    <div class="flex flex-col items-center h-full">
                      <div class="w-5 flex-1 rounded overflow-hidden relative bg-bg-muted">
                        <div class="absolute bottom-0 left-0 right-0 flex flex-col" style={{ height: `${pct()}%`, "min-height": pct() > 0 ? "2px" : "0" }}>
                          <Show when={alloc.devices.length > 0} fallback={<div class="w-full h-full" style={{ "background-color": getUsageBarColor(alloc.type) }} />}>
                            <For each={alloc.devices}>
                              {(dev: any) => (
                                <div style={{ "flex-grow": Number(dev.size), "background-color": getDeviceColor(alloc.type, dev.devicePath) }} class="w-full min-h-[1px]" />
                              )}
                            </For>
                          </Show>
                        </div>
                      </div>
                      <div class="text-[10px] text-text-tertiary mt-1 text-center leading-tight">
                        <div class="font-medium text-text-default">{pct()}%</div>
                        <div>{alloc.type.slice(0, 4)}</div>
                      </div>
                    </div>
                  </Tooltip>
                );
              }}
            </For>
            {/* Unallocated */}
            {(() => {
              const unalloc = props.usage.allocations.find((a: any) => a.type === "Unallocated");
              if (!unalloc) return null;
              const rawTotal = () => unalloc.devices.reduce((sum: bigint, d: any) => sum + d.size, 0n);
              const unallocPct = () => totalSpace().total > 0n ? Number((rawTotal() * 100n) / totalSpace().total) : 0;
              return (
                <Tooltip text={`Unallocated: ${formatBytes(rawTotal())} (${unallocPct()}% of total)`}>
                  <div class="flex flex-col items-center h-full">
                    <div class="w-5 flex-1 rounded overflow-hidden relative bg-bg-muted">
                      <div class="absolute bottom-0 left-0 right-0" style={{ height: `${unallocPct()}%`, "min-height": unallocPct() > 0 ? "2px" : "0", "background-color": getUsageBarColor("Unallocated") }} />
                    </div>
                    <div class="text-[10px] text-text-tertiary mt-1 text-center leading-tight">
                      <div class="font-medium text-text-default">{unallocPct()}%</div>
                      <div>Free</div>
                    </div>
                  </div>
                </Tooltip>
              );
            })()}
          </div>
        </div>
        {/* Stats */}
        <div class="flex-1 flex">
          <table class="text-xs flex-1">
            <tbody>
              <tr class="border-b border-border-subtle">
                <td class="px-1.5 py-0.5 text-text-tertiary">total</td>
                <td class="px-1.5 py-0.5 font-mono text-text-default text-right"><FormattedBytes bytes={totalSpace().total} showRaw={props.showRaw} /></td>
              </tr>
              <tr class="border-b border-border-subtle">
                <td class="px-1.5 py-0.5 text-text-tertiary">allocated <span class="text-text-muted">({allocatedPct()}%)</span></td>
                <td class="px-1.5 py-0.5 font-mono text-text-default text-right"><FormattedBytes bytes={totalSpace().allocated} showRaw={props.showRaw} /></td>
              </tr>
              <tr class="border-b border-border-subtle">
                <td class="px-1.5 py-0.5 text-text-tertiary">unallocated</td>
                <td class="px-1.5 py-0.5 font-mono text-text-default text-right"><FormattedBytes bytes={totalSpace().unallocated} showRaw={props.showRaw} /></td>
              </tr>
              <tr class="border-b border-border-subtle">
                <td class="px-1.5 py-0.5 text-text-tertiary">used</td>
                <td class="px-1.5 py-0.5 font-mono text-text-default text-right"><FormattedBytes bytes={totalSpace().used} showRaw={props.showRaw} /></td>
              </tr>
              <Show when={props.usage.deviceSlack > 0n}>
                <tr class="border-b border-border-subtle">
                  <td class="px-1.5 py-0.5 text-text-tertiary">slack</td>
                  <td class="px-1.5 py-0.5 font-mono text-text-default text-right"><FormattedBytes bytes={props.usage.deviceSlack} showRaw={props.showRaw} /></td>
                </tr>
              </Show>
              <Show when={globalReserve()}>
                {(gr) => (
                  <tr>
                    <td class="px-1.5 py-0.5 text-text-tertiary">reserve</td>
                    <td class="px-1.5 py-0.5 font-mono text-text-default text-right"><FormattedBytes bytes={gr().used} showRaw={props.showRaw} />/<FormattedBytes bytes={gr().size} showRaw={props.showRaw} /></td>
                  </tr>
                )}
              </Show>
            </tbody>
          </table>
          <table class="text-xs flex-1 border-l border-border-subtle">
            <tbody>
              <For each={allocations().filter((a: any) => a.type !== "Unallocated")}>
                {(alloc) => {
                  const ratio = () => alloc.type === "Data" ? props.usage.dataRatio : props.usage.metadataRatio;
                  return (
                    <>
                      <tr class="border-b border-border-subtle">
                        <td class="px-1.5 py-0.5 text-text-tertiary">{alloc.type.toLowerCase()}</td>
                        <td class="px-1.5 py-0.5 font-mono text-text-default text-right"><FormattedBytes bytes={alloc.used} showRaw={props.showRaw} /><span class="text-text-muted">/</span><FormattedBytes bytes={alloc.size} showRaw={props.showRaw} /></td>
                      </tr>
                      <tr class="border-b border-border-subtle">
                        <td class="px-1.5 py-0.5 text-text-muted pl-3">profile</td>
                        <td class="px-1.5 py-0.5 text-text-tertiary text-right">{alloc.profile} <span class="text-text-muted">({ratio()}x)</span></td>
                      </tr>
                    </>
                  );
                }}
              </For>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function DevicesCard(props: { usage: any; deviceStats: any; showRaw: boolean }) {
  const deviceList = createMemo(() => {
    // Filter out "total" entry from device stats
    const realDevices = props.deviceStats.filter((d: any) => d.devicePath && d.devicePath !== "total");
    if (realDevices.length > 0) {
      return realDevices.map((d: any) => ({
        path: d.devicePath,
        totalBytes: d.totalBytes,
        deviceId: d.deviceId,
        writeErrors: d.writeErrors,
        readErrors: d.readErrors,
        flushErrors: d.flushErrors,
        corruptionErrors: d.corruptionErrors,
        generationErrors: d.generationErrors,
      }));
    }
    const devMap = new Map<string, bigint>();
    for (const alloc of props.usage.allocations) {
      for (const dev of alloc.devices) {
        if (dev.devicePath) devMap.set(dev.devicePath, (devMap.get(dev.devicePath) || 0n) + dev.size);
      }
    }
    if (devMap.size === 0) {
      return [{ path: "all devices", totalBytes: props.usage.deviceSize, deviceId: undefined, writeErrors: 0n, readErrors: 0n, flushErrors: 0n, corruptionErrors: 0n, generationErrors: 0n }];
    }
    return Array.from(devMap.entries()).map(([path, total]) => ({
      path, totalBytes: total, deviceId: undefined, writeErrors: 0n, readErrors: 0n, flushErrors: 0n, corruptionErrors: 0n, generationErrors: 0n,
    }));
  });

  const getDeviceAllocations = (devPath: string) => {
    if (devPath === "all devices") {
      return props.usage.allocations.filter((a: any) => a.type !== "GlobalReserve").map((alloc: any) => ({ type: alloc.type, profile: alloc.profile, size: alloc.size }));
    }
    return props.usage.allocations
      .map((alloc: any) => {
        if (!alloc.devices || alloc.devices.length === 0) return null;
        const devAlloc = alloc.devices.find((d: any) => d.devicePath === devPath);
        if (!devAlloc) return null;
        return { type: alloc.type, profile: alloc.profile, size: devAlloc.size };
      })
      .filter(Boolean);
  };

  return (
    <div class="bg-bg-surface border border-border-default">
      <div class="px-2 py-1 bg-bg-surface-raised border-b border-border-subtle text-xs text-text-tertiary">devices</div>
      <div class="divide-y divide-border-subtle">
        <For each={deviceList()}>
          {(dev) => {
            const deviceAllocations = () => getDeviceAllocations(dev.path).filter((a: any) => a.type !== "Unallocated");
            const totalErrors = () => dev.writeErrors + dev.readErrors + dev.flushErrors + dev.corruptionErrors + dev.generationErrors;
            const allocTotal = () => deviceAllocations().reduce((sum: bigint, a: any) => sum + a.size, 0n);
            // Unallocated = device total - sum of allocations
            const unallocated = () => {
              const total = dev.totalBytes;
              const alloc = allocTotal();
              return total > alloc ? total - alloc : 0n;
            };
            // Use device totalBytes as denominator to show full capacity
            const pctWidth = (size: bigint) => {
              const total = dev.totalBytes;
              if (total === 0n) return 0;
              return Number((size * 10000n) / total) / 100;
            };
            return (
              <div class="p-2">
                <div class="flex items-center justify-between text-xs mb-1">
                  <div class="flex items-center gap-2">
                    <span class="font-mono text-text-default">{dev.path}</span>
                    <Show when={dev.deviceId}><span class="text-text-muted text-[10px]">id:{dev.deviceId}</span></Show>
                  </div>
                  <div class="flex items-center gap-2">
                    <Show when={totalErrors() > 0n} fallback={<span class="text-success text-[10px]">0 errors</span>}>
                      <Tooltip text={`write:${formatNumber(dev.writeErrors)} read:${formatNumber(dev.readErrors)} flush:${formatNumber(dev.flushErrors)} corruption:${formatNumber(dev.corruptionErrors)} generation:${formatNumber(dev.generationErrors)}`}>
                        <span class="text-error text-[10px]">{formatNumber(totalErrors())} errors</span>
                      </Tooltip>
                    </Show>
                    <span class="text-text-tertiary"><FormattedBytes bytes={dev.totalBytes} showRaw={props.showRaw} /></span>
                  </div>
                </div>
                <div class="h-2 bg-bg-muted rounded overflow-hidden flex mb-1">
                  <For each={deviceAllocations()}>
                    {(alloc: any) => (
                      <div title={`${alloc.type}: ${formatBytes(alloc.size)}`} style={{ width: `${pctWidth(alloc.size)}%`, "background-color": getUsageBarColor(alloc.type) }} class="h-full border-r border-bg-surface/50 last:border-r-0 hover:brightness-110 transition-all cursor-pointer" />
                    )}
                  </For>
                  {/* Unallocated space shown relative to device capacity */}
                  <Show when={unallocated() > 0n}>
                    <div title={`Unallocated: ${formatBytes(unallocated())}`} style={{ width: `${pctWidth(unallocated())}%`, "background-color": getUsageBarColor("Unallocated") }} class="h-full hover:brightness-110 transition-all cursor-pointer" />
                  </Show>
                </div>
                <div class="flex flex-wrap gap-x-3 text-[10px] text-text-tertiary">
                  <For each={deviceAllocations()}>
                    {(alloc: any) => (
                      <span><span class="inline-block w-2 h-2 rounded-sm mr-0.5" style={{ "background-color": getUsageBarColor(alloc.type) }} />{alloc.type.slice(0, 4)} <FormattedBytes bytes={alloc.size} showRaw={props.showRaw} /></span>
                    )}
                  </For>
                  <Show when={unallocated() > 0n}>
                    <span><span class="inline-block w-2 h-2 rounded-sm mr-0.5" style={{ "background-color": getUsageBarColor("Unallocated") }} />Free <FormattedBytes bytes={unallocated()} showRaw={props.showRaw} /></span>
                  </Show>
                </div>
              </div>
            );
          }}
        </For>
      </div>
    </div>
  );
}
