import { Show, For, createMemo, createSignal, createResource, onMount, onCleanup, Suspense } from "solid-js";
import type { TrackedFilesystem } from "%/v1/filesystem_pb";
import type { Subvolume } from "%/v1/subvolume_pb";
import { subvolumeClient } from "@/api/client";
import { isInBtrbkDir, formatRelativeTimeShort, truncateUuid, copyToClipboard, formatNumber } from "@/lib/utils";
import { Flag } from "@/components/ui";

interface BackupInfo {
  date: Date;
  path: string;
  name: string;
}

interface TimeSlot {
  label: string;
  start: number;
  end: number;
  backups: BackupInfo[];
}

async function loadSubvolumes(fsPath: string) {
  const resp = await subvolumeClient.listSubvolumes({ mountPath: fsPath });
  return resp.subvolumes;
}

export function SubvolumesTab(props: {
  fs: TrackedFilesystem;
  expandedNodes: Set<bigint>;
  setExpandedNodes: (fn: (prev: Set<bigint>) => Set<bigint>) => void;
}) {
  const [subvolumes, { refetch }] = createResource(() => props.fs.path, loadSubvolumes);

  // Listen for refresh events
  onMount(() => {
    const handler = () => refetch();
    window.addEventListener("fs-refresh", handler);
    onCleanup(() => window.removeEventListener("fs-refresh", handler));
  });

  return (
    <Suspense fallback={<div class="text-xs text-text-tertiary">loading subvolumes...</div>}>
      <Show when={subvolumes()} keyed>
        {(subs) => <SubvolumesContent fs={props.fs} subvolumes={subs} expandedNodes={props.expandedNodes} setExpandedNodes={props.setExpandedNodes} />}
      </Show>
    </Suspense>
  );
}

function SubvolumesContent(props: {
  fs: TrackedFilesystem;
  subvolumes: Subvolume[];
  expandedNodes: Set<bigint>;
  setExpandedNodes: (fn: (prev: Set<bigint>) => Set<bigint>) => void;
}) {
  // Filter out btrbk backups for subvolume tree
  const subvolumes = createMemo(() =>
    props.subvolumes.filter(sv => !isInBtrbkDir(sv.path, props.fs.btrbkSnapshotDir))
  );

  // Backups tooltip state
  const [tooltip, setTooltip] = createSignal<{ x: number; y: number; slot: TimeSlot } | null>(null);

  const toggleExpand = (id: bigint) => {
    props.setExpandedNodes(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  // Build tree structure
  const tree = createMemo(() => {
    const byParent = new Map<bigint, Subvolume[]>();
    for (const sv of subvolumes()) {
      const parentId = sv.parentId ?? 0n;
      if (!byParent.has(parentId)) {
        byParent.set(parentId, []);
      }
      byParent.get(parentId)!.push(sv);
    }
    return byParent;
  });

  // Format date for display
  const formatDate = (timestamp: bigint) => {
    // Go's zero time (0001-01-01) has Unix timestamp of -62135596800
    // Also handle 0 and undefined
    if (!timestamp || timestamp === 0n || timestamp < 0n) return "-";
    const date = new Date(Number(timestamp) * 1000);
    // Check for invalid/very old dates (before 1990)
    if (date.getFullYear() < 1990) return "-";
    // Show date in compact format: YYYY-MM-DD
    return date.toISOString().split("T")[0];
  };

  const renderRow = (sv: Subvolume, depth: number): any => {
    const children = tree().get(sv.id) || [];
    const hasChildren = children.length > 0;
    const isExpanded = props.expandedNodes.has(sv.id);
    const name = sv.path.split("/").pop() || sv.path || "/";
    const isSnapshot = !!sv.parentUuid;

    return (
      <>
        <tr class="hover:bg-bg-surface-raised text-xs">
          {/* Name with tree indent */}
          <td class="py-1 pr-2">
            <div
              class="flex items-center cursor-pointer"
              style={{ "padding-left": `${depth * 16}px` }}
              onClick={() => hasChildren && toggleExpand(sv.id)}
            >
              <span class="w-4 text-text-muted flex-shrink-0">
                {hasChildren ? (isExpanded ? "▼" : "▶") : ""}
              </span>
              <span class="font-mono text-text-default truncate" title={sv.path}>
                {name}
              </span>
            </div>
          </td>
          {/* ID */}
          <td class="py-1 px-2 text-right font-mono text-text-tertiary" title={`Subvolume ID: ${sv.id}`}>
            {formatNumber(sv.id)}
          </td>
          {/* Gen */}
          <td class="py-1 px-2 text-right font-mono text-text-muted" title={`Generation number: ${sv.gen}\nIncremented on each transaction that modifies this subvolume`}>
            {formatNumber(sv.gen)}
          </td>
          {/* Flags */}
          <td class="py-1 px-2">
            <div class="flex gap-1">
              <Flag
                active={sv.isReadonly}
                label="ro"
                tooltip={sv.isReadonly
                  ? "Read-only: This subvolume cannot be modified. Snapshots are typically read-only."
                  : "Read-write: This subvolume can be modified."}
                activeClass="bg-warning-subtle text-warning-muted"
              />
              <Flag
                active={isSnapshot}
                label="snap"
                tooltip={isSnapshot
                  ? `Snapshot: Created from parent ${sv.parentUuid}`
                  : "Not a snapshot: This is an original subvolume, not created from another."}
                activeClass="bg-primary-muted text-primary-hover"
              />
            </div>
          </td>
          {/* UUID */}
          <td class="py-1 px-2 font-mono text-text-muted text-[10px]">
            <Show when={sv.uuid} fallback="-">
              <span
                class="cursor-pointer hover:text-text-secondary hover:underline"
                title={`${sv.uuid}\nClick to copy`}
                onClick={() => copyToClipboard(sv.uuid, "UUID copied")}
              >
                {truncateUuid(sv.uuid)}
              </span>
            </Show>
          </td>
          {/* Created */}
          <td class="py-1 px-2 text-right text-text-muted">
            {formatDate(sv.createdAt)}
          </td>
        </tr>
        <Show when={isExpanded}>
          <For each={children}>
            {(child) => renderRow(child, depth + 1)}
          </For>
        </Show>
      </>
    );
  };

  const roots = createMemo(() => tree().get(0n) || tree().get(5n) || []);

  // === Backups section ===
  const hasBtrbkDir = () => !!props.fs.btrbkSnapshotDir;

  // Group backups by source
  const backupGroups = createMemo(() => {
    if (!hasBtrbkDir()) return [];

    const result = new Map<string, BackupInfo[]>();

    for (const sv of props.subvolumes) {
      if (!isInBtrbkDir(sv.path, props.fs.btrbkSnapshotDir)) continue;

      const name = sv.path.split("/").pop() || "";
      const lastDot = name.lastIndexOf(".");
      if (lastDot === -1) continue;

      const source = name.substring(0, lastDot);
      const dateStr = name.substring(lastDot + 1);

      let date: Date;
      if (dateStr.length >= 15 && dateStr[8] === "T") {
        date = new Date(
          parseInt(dateStr.substring(0, 4)),
          parseInt(dateStr.substring(4, 6)) - 1,
          parseInt(dateStr.substring(6, 8)),
          parseInt(dateStr.substring(9, 11)),
          parseInt(dateStr.substring(11, 13)),
          parseInt(dateStr.substring(13, 15))
        );
      } else if (dateStr.length >= 8) {
        date = new Date(
          parseInt(dateStr.substring(0, 4)),
          parseInt(dateStr.substring(4, 6)) - 1,
          parseInt(dateStr.substring(6, 8))
        );
      } else {
        continue;
      }

      if (!result.has(source)) {
        result.set(source, []);
      }
      result.get(source)!.push({ date, path: sv.path, name });
    }

    // Sort by date within each group (newest first for display)
    for (const backups of result.values()) {
      backups.sort((a, b) => b.date.getTime() - a.date.getTime());
    }

    return Array.from(result.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  });

  // Generate time slots with non-linear scale
  const timeSlots = createMemo(() => {
    const now = new Date();
    const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
    const DAY = 24 * 60 * 60 * 1000;
    const WEEK = 7 * DAY;
    const MONTH = 30 * DAY;

    const slots: { label: string; start: number; end: number }[] = [];
    let cursor = startOfToday + DAY;

    // 14 daily slots
    for (let i = 0; i < 14; i++) {
      const end = cursor;
      const start = end - DAY;
      cursor = start;
      const label = i === 0 ? "today" : `${i}d`;
      slots.push({ label, start, end });
    }

    // 6 weekly slots
    for (let i = 0; i < 6; i++) {
      const end = cursor;
      const start = end - WEEK;
      cursor = start;
      slots.push({ label: `${i + 2}w`, start, end });
    }

    // 8 monthly slots
    for (let i = 0; i < 8; i++) {
      const end = cursor;
      const start = end - MONTH;
      cursor = start;
      slots.push({ label: `${i + 2}mo`, start, end });
    }

    // 4 quarterly slots
    for (let i = 0; i < 4; i++) {
      const end = cursor;
      const start = end - 3 * MONTH;
      cursor = start;
      slots.push({ label: `${10 + i * 3}mo`, start, end });
    }

    // 3 yearly slots
    for (let i = 0; i < 3; i++) {
      const end = cursor;
      const start = end - 365 * DAY;
      cursor = start;
      slots.push({ label: `${i + 2}y`, start, end });
    }

    return slots;
  });

  const getSlotsWithBackups = (backups: BackupInfo[]): TimeSlot[] => {
    return timeSlots().map(slot => ({
      ...slot,
      backups: backups.filter(b => {
        const t = b.date.getTime();
        return t >= slot.start && t < slot.end;
      }),
    }));
  };

  const totalBackups = createMemo(() => backupGroups().reduce((sum, [, b]) => sum + b.length, 0));

  const handleMouseEnter = (e: MouseEvent, slot: TimeSlot) => {
    if (slot.backups.length === 0) return;
    const rect = (e.target as HTMLElement).getBoundingClientRect();
    setTooltip({
      x: rect.left + rect.width / 2,
      y: rect.top - 8,
      slot,
    });
  };

  const handleMouseLeave = () => {
    setTooltip(null);
  };

  const latestBackup = (backups: BackupInfo[]) => backups[0];

  return (
    <div class="space-y-2">
      {/* Subvolumes table */}
      <div class="bg-bg-surface border border-border-default">
        <div class="px-2 py-1 bg-bg-surface-raised border-b border-border-subtle flex justify-between items-center">
          <span class="text-xs text-text-tertiary">subvolumes</span>
          <span class="text-xs text-text-muted">{formatNumber(subvolumes().length)}</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full">
            <thead>
              <tr class="text-[10px] text-text-tertiary border-b border-border-subtle bg-bg-surface-raised">
                <th class="py-1 px-2 text-left font-medium">name</th>
                <th class="py-1 px-2 text-right font-medium" title="Subvolume ID - unique numeric identifier">id</th>
                <th class="py-1 px-2 text-right font-medium" title="Generation number - incremented on each modification">gen</th>
                <th class="py-1 px-2 text-left font-medium" title="Subvolume flags and properties">props</th>
                <th class="py-1 px-2 text-left font-medium" title="UUID - unique identifier (click to copy)">uuid</th>
                <th class="py-1 px-2 text-right font-medium" title="Creation time">created</th>
              </tr>
            </thead>
            <tbody class="font-mono">
              <For each={roots()}>
                {(sv) => renderRow(sv, 0)}
              </For>
              <Show when={roots().length === 0}>
                <tr>
                  <td colspan="6" class="py-2 px-2 text-xs text-text-tertiary text-center">
                    No subvolumes found
                  </td>
                </tr>
              </Show>
            </tbody>
          </table>
        </div>
      </div>

      {/* Backups timeline */}
      <Show when={hasBtrbkDir() && backupGroups().length > 0}>
        <div class="bg-bg-surface border border-border-default relative">
          {/* Header */}
          <div class="px-2 py-1 bg-bg-surface-raised border-b border-border-subtle flex justify-between items-center">
            <span class="text-xs text-text-tertiary">backups</span>
            <span class="text-xs text-text-muted">{totalBackups()} snapshots</span>
          </div>

          {/* Scrollable timeline container */}
          <div class="overflow-x-auto">
            {/* Legend row */}
            <div class="flex items-center border-b border-border-subtle bg-bg-surface-raised min-w-max">
              <div class="w-40 flex-shrink-0 px-2 py-1" />
              <div class="flex gap-px mx-2 py-1">
                <For each={timeSlots()}>
                  {(slot) => (
                    <div
                      class="w-4 h-3 flex items-center justify-center text-[8px] text-text-muted flex-shrink-0"
                      title={slot.label}
                    >
                      {slot.label.length <= 3 ? slot.label : ""}
                    </div>
                  )}
                </For>
              </div>
            </div>

            {/* Backup rows */}
            <div class="divide-y divide-border-subtle">
              <For each={backupGroups()}>
                {([source, backups]) => {
                  const latest = latestBackup(backups);
                  const latestRelative = formatRelativeTimeShort(latest.date);
                  const slots = getSlotsWithBackups(backups);

                  return (
                    <div class="flex items-center hover:bg-bg-surface-raised min-w-max">
                      <div class="w-40 flex-shrink-0 px-2 py-2 text-xs truncate" title={source}>
                        <div class="font-medium text-text-default">{source}</div>
                        <div class="text-text-muted text-[10px]">
                          {formatNumber(backups.length)} · latest {latestRelative}
                        </div>
                      </div>
                      <div class="flex gap-px mx-2 py-1">
                        <For each={slots}>
                          {(slot) => {
                            const hasBackup = slot.backups.length > 0;
                            const count = slot.backups.length;
                            return (
                              <div
                                class={`w-4 h-4 rounded-sm flex-shrink-0 transition-colors ${
                                  hasBackup
                                    ? count > 1
                                      ? "bg-primary hover:bg-primary-strong cursor-pointer"
                                      : "bg-primary-softer hover:bg-primary cursor-pointer"
                                    : "bg-bg-base"
                                }`}
                                onMouseEnter={(e) => handleMouseEnter(e, slot)}
                                onMouseLeave={handleMouseLeave}
                              />
                            );
                          }}
                        </For>
                      </div>
                    </div>
                  );
                }}
              </For>
            </div>
          </div>

          {/* Tooltip */}
          <Show when={tooltip()}>
            {(t) => {
              const showBelow = t().y < 150;
              return (
                <div
                  class="fixed z-50 bg-bg-overlay text-text-inverse text-xs rounded px-2 py-1.5 pointer-events-none shadow-lg max-w-xs"
                  style={{
                    left: `${t().x}px`,
                    top: showBelow ? `${t().y + 30}px` : `${t().y}px`,
                    transform: showBelow ? "translateX(-50%)" : "translate(-50%, -100%)",
                  }}
                >
                  <div class="font-medium mb-1">{t().slot.label}: {formatNumber(t().slot.backups.length)} backup{t().slot.backups.length !== 1 ? "s" : ""}</div>
                  <div class="space-y-0.5 max-h-24 overflow-y-auto">
                    <For each={t().slot.backups.slice(0, 10)}>
                      {(backup) => (
                        <div class="flex justify-between gap-3 text-text-faint">
                          <span>{backup.date.toLocaleDateString()}</span>
                          <span class="text-text-muted">{formatRelativeTimeShort(backup.date)}</span>
                        </div>
                      )}
                    </For>
                    <Show when={t().slot.backups.length > 10}>
                      <div class="text-text-tertiary">+{formatNumber(t().slot.backups.length - 10)} more</div>
                    </Show>
                  </div>
                </div>
              );
            }}
          </Show>
        </div>
      </Show>
    </div>
  );
}
