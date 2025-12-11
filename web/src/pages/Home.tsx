import { createSignal, createResource, For, Show, onMount, onCleanup, batch, Suspense } from "solid-js";
import { A } from "@solidjs/router";
import { filesystemClient, subvolumeClient, scrubClient } from "@/api/client";
import { cacheFsData } from "@/stores/filesystems";
import { cn, formatBytes, countBackups, formatNumber } from "@/lib/utils";
import { Button, Alert, ProgressBar, ScrubStatus, LabeledInput } from "@/components/ui";

interface FilesystemSummary {
  id: bigint;
  path: string;
  label: string;
  subvolumeCount: number;
  backupCount: number;
  errorCount: number;
  scrubStatus: string;
  scrubRunning: boolean;
  btrbkConfigured: boolean;
  totalBytes: bigint;
  usedBytes: bigint;
  freeBytes: bigint;
}

async function loadFilesystems(): Promise<FilesystemSummary[]> {
  const [fsResp, subvolResp, errorResp, scrubResp, statsResp] = await Promise.all([
    filesystemClient.listTrackedFilesystems({}),
    subvolumeClient.listAllSubvolumes({}),
    filesystemClient.getAllErrors({ limit: 1000 }),
    scrubClient.getAllScrubStatus({}),
    filesystemClient.getAllDeviceStats({}),
  ]);

  const summaries: FilesystemSummary[] = [];

  for (const fs of fsResp.filesystems) {
    cacheFsData(fs);

    const summary: FilesystemSummary = {
      id: fs.id,
      path: fs.path,
      label: fs.label,
      subvolumeCount: 0,
      backupCount: 0,
      errorCount: 0,
      scrubStatus: "",
      scrubRunning: false,
      btrbkConfigured: fs.btrbkSnapshotDir !== "",
      totalBytes: 0n,
      usedBytes: 0n,
      freeBytes: 0n,
    };

    const subvolFs = subvolResp.filesystems.find(s => s.path === fs.path);
    if (subvolFs) {
      const backups = fs.btrbkSnapshotDir
        ? countBackups(subvolFs.subvolumes, fs.btrbkSnapshotDir)
        : 0;
      summary.backupCount = backups;
      summary.subvolumeCount = subvolFs.subvolumes.length - backups;
    }

    const errorFs = errorResp.filesystems.find(e => e.path === fs.path);
    if (errorFs) {
      summary.errorCount = errorFs.errors.length;
    }

    const scrubFs = scrubResp.filesystems.find(s => s.path === fs.path);
    if (scrubFs) {
      summary.scrubRunning = scrubFs.isRunning;
      summary.scrubStatus = scrubFs.progress?.status || "";
    }

    const statsFs = statsResp.filesystems.find(s => s.path === fs.path);
    if (statsFs && statsFs.devices) {
      for (const dev of statsFs.devices) {
        summary.totalBytes += dev.totalBytes;
        summary.usedBytes += dev.usedBytes;
        summary.freeBytes += dev.freeBytes;
      }
    }

    summaries.push(summary);
  }

  return summaries;
}

export default function Home() {
  const [filesystems, { refetch }] = createResource(loadFilesystems);
  const [showAddForm, setShowAddForm] = createSignal(false);
  const [newPath, setNewPath] = createSignal("");
  const [newLabel, setNewLabel] = createSignal("");
  const [newBtrbkDir, setNewBtrbkDir] = createSignal("");
  const [adding, setAdding] = createSignal(false);
  const [error, setError] = createSignal("");

  onMount(() => {
    const handler = () => refetch();
    window.addEventListener("refresh", handler);
    onCleanup(() => window.removeEventListener("refresh", handler));
  });

  const addFilesystem = async () => {
    if (!newPath()) return;
    setAdding(true);
    setError("");

    try {
      await filesystemClient.addFilesystem({
        path: newPath(),
        label: newLabel(),
        btrbkSnapshotDir: newBtrbkDir(),
      });
      batch(() => {
        setNewPath("");
        setNewLabel("");
        setNewBtrbkDir("");
        setShowAddForm(false);
      });
      refetch();
    } catch (e) {
      setError(String(e));
    } finally {
      setAdding(false);
    }
  };

  const removeFilesystem = async (id: bigint) => {
    try {
      await filesystemClient.removeFilesystem({ id });
      refetch();
    } catch (e) {
      setError(String(e));
    }
  };

  return (
    <div class="space-y-2">
      {/* Error */}
      <Show when={error()}>
        <Alert type="error">{error()}</Alert>
      </Show>

      {/* Main content with Suspense */}
      <Suspense fallback={<div class="text-xs text-text-tertiary p-2">loading...</div>}>
        {/* Filesystem table with title bar */}
        <div class="border border-border-default bg-bg-surface">
          {/* Title bar */}
          <div class="flex items-center justify-end px-2 py-1 bg-bg-surface-raised border-b border-border-default">
            <button
              class="text-xs text-text-tertiary hover:text-text-default cursor-pointer"
              onClick={() => setShowAddForm(!showAddForm())}
            >
              + add
            </button>
          </div>

          {/* Add form */}
          <Show when={showAddForm()}>
            <div class="bg-bg-surface-raised border-b border-border-default p-2">
              <div class="flex flex-wrap gap-2 items-end">
                <LabeledInput
                  label="path"
                  value={newPath()}
                  onChange={setNewPath}
                  placeholder="/mnt/btrfs"
                  mono
                  class="flex-1 min-w-[150px]"
                />
                <LabeledInput
                  label="label"
                  value={newLabel()}
                  onChange={setNewLabel}
                  placeholder="optional"
                  class="w-24"
                />
                <LabeledInput
                  label="btrbk snapshot dir"
                  value={newBtrbkDir()}
                  onChange={setNewBtrbkDir}
                  placeholder=".snapshots"
                  mono
                  class="flex-1 min-w-[150px]"
                />
                <Button
                  variant="primary"
                  disabled={adding() || !newPath()}
                  onClick={addFilesystem}
                >
                  add
                </Button>
                <Button
                  variant="ghost"
                  onClick={() => {
                    batch(() => {
                      setShowAddForm(false);
                      setNewPath("");
                      setNewLabel("");
                      setNewBtrbkDir("");
                    });
                  }}
                >
                  cancel
                </Button>
              </div>
            </div>
          </Show>

          {/* Empty state */}
          <Show when={filesystems()?.length === 0}>
            <div class="text-xs text-text-tertiary p-4 text-center">
              no filesystems tracked. click + add to add one.
            </div>
          </Show>

          {/* Table content */}
          <Show when={(filesystems()?.length ?? 0) > 0}>
            <table class="w-full text-xs">
              <thead class="bg-bg-muted border-b border-border-subtle">
                <tr>
                  <th class="px-2 py-1 text-left text-text-tertiary font-normal">path</th>
                  <th class="px-2 py-1 text-left text-text-tertiary font-normal">usage</th>
                  <th class="px-2 py-1 text-right text-text-tertiary font-normal">subvols</th>
                  <th class="px-2 py-1 text-right text-text-tertiary font-normal">errors</th>
                  <th class="px-2 py-1 text-left text-text-tertiary font-normal">scrub</th>
                  <th class="px-2 py-1 text-right text-text-tertiary font-normal"></th>
                </tr>
              </thead>
              <tbody>
                <For each={filesystems()}>
                  {(fs) => {
                    const usedPct = () => fs.totalBytes > 0n ? Number((fs.usedBytes * 100n) / fs.totalBytes) : 0;
                    return (
                      <tr class="border-b border-border-subtle hover:bg-bg-surface-raised">
                        <td class="px-2 py-2">
                          <A
                            href={`/fs/${fs.id}`}
                            class="text-primary hover:text-primary-strong hover:underline"
                          >
                            <span>{fs.label || fs.path}</span>
                            <Show when={fs.label}>
                              <span class="text-text-muted font-mono ml-1">{fs.path}</span>
                            </Show>
                          </A>
                        </td>
                        <td class="px-2 py-2">
                          <Show when={fs.totalBytes > 0n} fallback={<span class="text-text-muted">-</span>}>
                            <div class="flex items-center space-x-2">
                              <ProgressBar
                                value={usedPct()}
                                thresholdColors
                                size="md"
                                class="w-16"
                              />
                              <span class="text-text-secondary font-mono">{formatBytes(fs.usedBytes)}</span>
                              <span class="text-text-muted">/</span>
                              <span class="text-text-tertiary font-mono">{formatBytes(fs.totalBytes)}</span>
                            </div>
                          </Show>
                        </td>
                        <td class="px-2 py-2 text-right text-text-secondary">
                          {formatNumber(fs.subvolumeCount)}
                          <Show when={fs.backupCount > 0}>
                            <span class="text-text-muted">+{formatNumber(fs.backupCount)}</span>
                          </Show>
                        </td>
                        <td class="px-2 py-2 text-right">
                          <span class={cn(fs.errorCount > 0 ? "text-error" : "text-text-muted")}>
                            {formatNumber(fs.errorCount)}
                          </span>
                        </td>
                        <td class="px-2 py-2">
                          <ScrubStatus running={fs.scrubRunning} status={fs.scrubStatus} />
                        </td>
                        <td class="px-2 py-2 text-right">
                          <button
                            class="text-error-soft hover:text-error-hover hover:bg-error-subtle px-1 cursor-pointer"
                            title="Remove filesystem"
                            onClick={() => removeFilesystem(fs.id)}
                          >
                            ×
                          </button>
                        </td>
                      </tr>
                    );
                  }}
                </For>
              </tbody>
            </table>
          </Show>
        </div>
      </Suspense>
    </div>
  );
}
