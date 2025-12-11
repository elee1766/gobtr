import { createSignal, createResource, createMemo, For, Show, onMount, onCleanup, Suspense } from "solid-js";
import { useParams, A } from "@solidjs/router";
import { filesystemClient } from "@/api/client";
import { cacheFsData } from "@/stores/filesystems";
import { uiSettings, setShowRawBytes } from "@/stores/ui";
import type { TrackedFilesystem } from "%/v1/filesystem_pb";
import { cn } from "@/lib/utils";
import { ToggleGroup, Alert } from "@/components/ui";

// Tab components
import { OverviewTab } from "./fs/OverviewTab";
import { UsageTab } from "./fs/UsageTab";
import { SubvolumesTab } from "./fs/SubvolumesTab";
import { MaintenanceTab } from "./fs/MaintenanceTab";
import { VisualizeTab } from "./fs/VisualizeTab";
import { ErrorsTab } from "./fs/ErrorsTab";
import { SettingsTab } from "./fs/SettingsTab";

async function loadFs(id: string): Promise<TrackedFilesystem | null> {
  const fsId = BigInt(id);
  const fsResp = await filesystemClient.listTrackedFilesystems({});
  const fs = fsResp.filesystems.find(f => f.id === fsId);
  if (fs) cacheFsData(fs);
  return fs ?? null;
}

export default function FilesystemDetail() {
  const params = useParams();
  const [fs, { refetch }] = createResource(() => params.id, loadFs);
  const [expandedNodes, setExpandedNodes] = createSignal<Set<bigint>>(new Set());
  const [error, setError] = createSignal("");

  // Listen for refresh events
  onMount(() => {
    const handler = () => {
      refetch();
      // Dispatch a custom event that tabs can listen to
      window.dispatchEvent(new CustomEvent("fs-refresh"));
    };
    window.addEventListener("refresh", handler);
    onCleanup(() => window.removeEventListener("refresh", handler));
  });

  const activeTab = createMemo(() => params.tab || "overview");

  const tabs = [
    { id: "overview", label: "overview" },
    { id: "usage", label: "usage" },
    { id: "subvolumes", label: "subvolumes" },
    { id: "maintenance", label: "maintenance" },
    { id: "visualize", label: "visualize" },
    { id: "errors", label: "errors" },
    { id: "config", label: "config" },
  ];

  return (
    <div>
      {/* Tabs row */}
      <div class="flex items-end justify-between">
        <div class="flex">
          <For each={tabs}>
            {(tab) => (
              <A
                href={`/fs/${params.id}/${tab.id}`}
                class={cn(
                  "px-3 py-1 text-xs select-none border-t border-x -mb-px",
                  activeTab() === tab.id
                    ? "border-border-default bg-bg-surface text-text-default"
                    : "border-transparent text-text-tertiary hover:text-text-default hover:bg-bg-surface-raised"
                )}
                draggable={false}
              >
                {tab.label}
              </A>
            )}
          </For>
        </div>
        <ToggleGroup
          options={[
            { label: "human", value: false },
            { label: "raw", value: true },
          ]}
          value={uiSettings().showRawBytes}
          onChange={setShowRawBytes}
          class="mb-1"
        />
      </div>

      {/* Content panel */}
      <div class="border border-border-default bg-bg-surface p-3">
        {/* Error */}
        <Show when={error()}>
          <Alert type="error" class="mb-2">{error()}</Alert>
        </Show>

        {/* Loading fs */}
        <Suspense fallback={<div class="text-xs text-text-tertiary">loading...</div>}>
          <Show when={fs()} keyed>
            {(f) => (
              <>
                <Show when={activeTab() === "overview"}>
                  <OverviewTab fs={f} />
                </Show>
                <Show when={activeTab() === "usage"}>
                  <UsageTab fs={f} />
                </Show>
                <Show when={activeTab() === "subvolumes"}>
                  <SubvolumesTab
                    fs={f}
                    expandedNodes={expandedNodes()}
                    setExpandedNodes={setExpandedNodes}
                  />
                </Show>
                <Show when={activeTab() === "maintenance"}>
                  <MaintenanceTab fs={f} setError={setError} onRefresh={refetch} />
                </Show>
                <Show when={activeTab() === "visualize"}>
                  <VisualizeTab fsPath={f.path} />
                </Show>
                <Show when={activeTab() === "errors"}>
                  <ErrorsTab fsPath={f.path} />
                </Show>
                <Show when={activeTab() === "config"}>
                  <SettingsTab fs={f} onSave={refetch} />
                </Show>
              </>
            )}
          </Show>
        </Suspense>
      </div>
    </div>
  );
}
