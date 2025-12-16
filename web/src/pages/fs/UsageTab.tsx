import { createSignal, createMemo, For, Show, onMount, onCleanup } from "solid-js";
import { createStore, reconcile, produce } from "solid-js/store";
import type { TrackedFilesystem } from "%/v1/filesystem_pb";
import { formatBytes } from "@/lib/utils";
import { uiSettings } from "@/stores/ui";
import { usageClient } from "@/api/client";

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

interface TreeNode {
  name: string;
  fullPath: string;
  isDir: boolean;
  samples: bigint;
  estimatedSize: bigint;
  percentage: number;
  childCount: number;
  children?: TreeNode[];
  expanded?: boolean;
  loading?: boolean;
}

export function UsageTab(props: { fs: TrackedFilesystem }) {
  const fsPath = () => props.fs.path;

  // Sampling state
  const [sortBy, setSortBy] = createSignal<"size" | "name" | "samples">("size");
  const [sortDesc, setSortDesc] = createSignal(true);
  const [autoRefreshPaused, setAutoRefreshPaused] = createSignal(false);
  const [treeData, setTreeData] = createStore<TreeNode[]>([]);
  const [expandedPaths, setExpandedPaths] = createSignal<Set<string>>(new Set());
  let prevSampleCount = 0;
  let prevSessionId = "";

  // Status - use signal instead of resource to avoid loading state flicker
  const [statusData, setStatusData] = createSignal<Awaited<ReturnType<typeof usageClient.getSamplingStatus>> | null>(null);
  const [initialLoading, setInitialLoading] = createSignal(true);
  const [treeLoading, setTreeLoading] = createSignal(false);

  const fetchStatus = async () => {
    try {
      const resp = await usageClient.getSamplingStatus({ fsPath: fsPath() });
      setStatusData(resp);
      return resp;
    } catch (e) {
      console.error("Failed to fetch status:", e);
      return null;
    } finally {
      setInitialLoading(false);
    }
  };

  // Total size for percentage calculation
  const [rootTotalSize, setRootTotalSize] = createSignal(0n);
  const [rootTotalSamples, setRootTotalSamples] = createSignal(0n);

  // Fetch children for a path
  const fetchChildren = async (path: string): Promise<TreeNode[]> => {
    const resp = await usageClient.getUsageTree({
      fsPath: fsPath(),
      path: path,
      sortBy: sortBy(),
      sortDesc: sortDesc(),
      limit: 100,
    });

    // Track root totals for percentage calculations
    if (path === "/") {
      if (resp.totalSize) setRootTotalSize(resp.totalSize);
      if (resp.totalSamples) setRootTotalSamples(resp.totalSamples);
    }

    const totalSamples = rootTotalSamples() || resp.totalSamples || 1n;

    return (resp.children ?? []).map(n => ({
        name: n.name,
        fullPath: n.fullPath,
        isDir: n.isDir,
        samples: n.samples,
        estimatedSize: n.estimatedSize,
        percentage: totalSamples > 0n ? Number((n.samples * 10000n) / totalSamples) / 100 : 0,
        childCount: n.childCount,
      }));
  };

  // Load root children
  const loadRoot = async () => {
    if (!statusData()?.hasSession) {
      setTreeData(reconcile([]));
      return;
    }

    setTreeLoading(true);
    try {
      const children = await fetchChildren("/");
      setTreeData(reconcile(children));
    } finally {
      setTreeLoading(false);
    }
  };

  // Poll for status updates - separate status and tree refresh for performance
  let statusInterval: number | undefined;
  let treeRefreshInterval: number | undefined;
  let countdownInterval: number | undefined;
  const TREE_REFRESH_SECS = 3; // Only refresh tree every 3 seconds
  const [treeRefreshCountdown, setTreeRefreshCountdown] = createSignal(TREE_REFRESH_SECS);

  onMount(() => {
    // Initial fetch
    fetchStatus().then(() => loadRoot());

    // Fast status polling (1s) - just updates stats display, no tree refresh
    statusInterval = setInterval(async () => {
      if (autoRefreshPaused()) return;
      const result = await fetchStatus();
      if (!result) return;

      const currentSessionId = result.sessionId ?? "";

      // Session changed - immediate full refresh needed
      if (currentSessionId !== prevSessionId) {
        prevSessionId = currentSessionId;
        prevSampleCount = Number(result.progress?.sampleCount ?? 0n);
        await refreshTree();
        setTreeRefreshCountdown(TREE_REFRESH_SECS);
      } else {
        // Just track sample count for display
        prevSampleCount = Number(result.progress?.sampleCount ?? 0n);
      }
    }, 1000) as unknown as number;

    // Countdown ticker - updates every second
    countdownInterval = setInterval(() => {
      if (autoRefreshPaused()) return;
      setTreeRefreshCountdown((c) => (c <= 1 ? TREE_REFRESH_SECS : c - 1));
    }, 1000) as unknown as number;

    // Slower tree refresh (3s) - reduces network load during active sampling
    treeRefreshInterval = setInterval(async () => {
      if (autoRefreshPaused()) return;
      const status = statusData();
      if (!status) return;

      const running = status.progress?.isRunning ?? false;
      const hasSamples = (status.progress?.sampleCount ?? 0n) > 0n;

      // Auto-refresh tree during active sampling OR when there's data to show
      if (running || (hasSamples && treeData.length === 0)) {
        if (treeData.length > 0) {
          await refreshTree();
        } else {
          // Initial load when tree is empty but we have samples
          await loadRoot();
        }
      }
    }, TREE_REFRESH_SECS * 1000) as unknown as number;
  });

  onCleanup(() => {
    if (statusInterval) clearInterval(statusInterval);
    if (treeRefreshInterval) clearInterval(treeRefreshInterval);
    if (countdownInterval) clearInterval(countdownInterval);
  });

  // Refresh entire tree while preserving expanded state
  const refreshTree = async () => {
    const expanded = expandedPaths();

    // Fetch root children
    const rootChildren = await fetchChildren("/");

    // Recursively refresh expanded nodes
    const refreshedChildren = await refreshNodeChildren(rootChildren, expanded);
    setTreeData(reconcile(refreshedChildren));
  };

  // Recursively refresh expanded child nodes
  const refreshNodeChildren = async (nodes: TreeNode[], expanded: Set<string>): Promise<TreeNode[]> => {
    return Promise.all(nodes.map(async (node) => {
      if (expanded.has(node.fullPath) && node.childCount > 0) {
        const children = await fetchChildren(node.fullPath);
        return {
          ...node,
          expanded: true,
          children: await refreshNodeChildren(children, expanded),
        };
      }
      return node;
    }));
  };

  // Cached status values - only update when we have actual new data (not loading states)
  const [cachedStatus, setCachedStatus] = createSignal<{
    sampleCount: number;
    totalSize: bigint;
    isRunning: boolean;
    hasSession: boolean;
    samplesPerSec: number;
    currentPath: string;
    sessionId: string;
    runningTimeSeconds: bigint;
  }>({
    sampleCount: 0,
    totalSize: 0n,
    isRunning: false,
    hasSession: false,
    samplesPerSec: 0,
    currentPath: "",
    sessionId: "",
    runningTimeSeconds: 0n,
  });

  // Recent paths for animation
  const [recentPaths, setRecentPaths] = createSignal<string[]>([]);
  const [pathAnimIndex, setPathAnimIndex] = createSignal(0);

  // Animation interval for cycling through recent paths (stops at end, doesn't loop)
  let pathAnimInterval: number | undefined;
  onMount(() => {
    pathAnimInterval = setInterval(() => {
      const paths = recentPaths();
      if (paths.length > 0) {
        setPathAnimIndex((prev) => {
          // Stop at the last path, don't loop
          if (prev >= paths.length - 1) return prev;
          return prev + 1;
        });
      }
    }, 60) as unknown as number; // Cycle every 60ms for smooth animation
  });
  onCleanup(() => {
    if (pathAnimInterval) clearInterval(pathAnimInterval);
  });

  // Update cached values only when values actually changed
  createMemo(() => {
    const s = statusData();
    if (s) {
      const newValues = {
        sampleCount: Number(s.progress?.sampleCount ?? 0n),
        totalSize: s.progress?.totalSize ?? 0n,
        isRunning: s.progress?.isRunning ?? false,
        hasSession: s.hasSession ?? false,
        samplesPerSec: s.progress?.samplesPerSecond ?? 0,
        currentPath: s.progress?.currentPath ?? "",
        sessionId: s.sessionId ?? "",
        runningTimeSeconds: s.progress?.runningTimeSeconds ?? 0n,
      };
      // Only update if values actually changed
      const prev = cachedStatus();
      if (
        newValues.sampleCount !== prev.sampleCount ||
        newValues.isRunning !== prev.isRunning ||
        newValues.hasSession !== prev.hasSession ||
        newValues.sessionId !== prev.sessionId ||
        newValues.totalSize !== prev.totalSize ||
        newValues.runningTimeSeconds !== prev.runningTimeSeconds
      ) {
        setCachedStatus(newValues);
      }

      // Update recent paths for animation - reset index when new paths arrive
      const paths = s.progress?.recentPaths ?? [];
      if (paths.length > 0) {
        setRecentPaths(paths);
        setPathAnimIndex(0); // Start from beginning of new batch
      }
    }
  });

  // Use cached values for display - these won't flicker during loading
  const sampleCount = () => cachedStatus().sampleCount;
  const totalSize = () => cachedStatus().totalSize;
  const isRunning = () => cachedStatus().isRunning;
  const hasSession = () => cachedStatus().hasSession;
  const samplesPerSec = () => cachedStatus().samplesPerSec;

  // Animated current path - cycles through recent paths
  const currentSamplingPath = () => {
    const paths = recentPaths();
    if (paths.length === 0) return cachedStatus().currentPath;
    return paths[pathAnimIndex() % paths.length] || cachedStatus().currentPath;
  };

  // Elapsed time - uses cached running time (cumulative active sampling time)
  const elapsed = createMemo(() => {
    const cached = cachedStatus();
    const seconds = Number(cached.runningTimeSeconds);
    if (seconds <= 0) return "0s";

    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
  });

  // Max samples for bar scaling (at root level)
  const maxSamples = createMemo(() => {
    if (treeData.length === 0) return 1n;
    return treeData.reduce((max, n) => n.samples > max ? n.samples : max, 1n);
  });

  const handleSortClick = (id: "size" | "name" | "samples") => {
    if (sortBy() === id) {
      setSortDesc(!sortDesc());
    } else {
      setSortBy(id);
      setSortDesc(true);
    }
    // Re-fetch with new sort
    refreshTree();
  };

  const handleStartSampling = async () => {
    try {
      await usageClient.startSampling({ fsPath: fsPath(), resume: true });
      fetchStatus();
      setTimeout(loadRoot, 500);
    } catch (e) {
      console.error("Failed to start sampling:", e);
    }
  };

  const handleStopSampling = async () => {
    try {
      await usageClient.stopSampling({ fsPath: fsPath() });
      fetchStatus();
    } catch (e) {
      console.error("Failed to stop sampling:", e);
    }
  };

  const handleClear = async () => {
    try {
      await usageClient.clearSampling({ fsPath: fsPath() });
      setTreeData(reconcile([]));
      setExpandedPaths(new Set());
      setRootTotalSize(0n);
      setRootTotalSamples(0n);
      fetchStatus();
    } catch (e) {
      console.error("Failed to clear sampling:", e);
    }
  };

  // Toggle expand/collapse for a node
  const toggleNode = async (path: number[], node: TreeNode) => {
    if (node.childCount === 0) return;

    const newExpanded = new Set(expandedPaths());

    if (node.expanded) {
      // Collapse
      newExpanded.delete(node.fullPath);
      setExpandedPaths(newExpanded);
      updateNodeInTree(path, { ...node, expanded: false, children: undefined });
    } else {
      // Expand - fetch children
      newExpanded.add(node.fullPath);
      setExpandedPaths(newExpanded);
      updateNodeInTree(path, { ...node, loading: true });

      try {
        const children = await fetchChildren(node.fullPath);
        updateNodeInTree(path, { ...node, expanded: true, loading: false, children });
      } catch (e) {
        console.error("Failed to load children:", e);
        updateNodeInTree(path, { ...node, loading: false });
        newExpanded.delete(node.fullPath);
        setExpandedPaths(newExpanded);
      }
    }
  };

  // Update a node in the tree by path
  const updateNodeInTree = (path: number[], newNode: TreeNode) => {
    setTreeData(produce((nodes) => {
      let current: TreeNode[] = nodes;
      for (let i = 0; i < path.length - 1; i++) {
        const node = current[path[i]];
        if (node?.children) {
          current = node.children;
        } else {
          return;
        }
      }
      const lastIdx = path[path.length - 1];
      if (current[lastIdx]) {
        Object.assign(current[lastIdx], newNode);
      }
    }));
  };

  // Recursive tree node component
  const TreeNodeRow = (props: { node: TreeNode; depth: number; path: number[]; maxSamples: bigint }) => {
    const node = () => props.node;
    const barPct = () => Number((node().samples * 100n) / props.maxSamples);
    const hasChildren = () => node().childCount > 0;

    return (
      <>
        <div class="relative hover:bg-bg-surface-raised group">
          {/* Background bar */}
          <div
            class="absolute inset-y-0 left-0 bg-primary-subtle"
            style={{ width: `${barPct()}%` }}
          />
          {/* Content */}
          <div
            class={`relative flex items-center py-1 pr-2 ${hasChildren() ? "cursor-pointer" : ""}`}
            style={{ "padding-left": `${props.depth * 16 + 8}px` }}
            onClick={() => hasChildren() && toggleNode(props.path, node())}
          >
            {/* Expand/collapse indicator */}
            <span class="w-4 text-text-muted text-xs flex-shrink-0">
              <Show when={hasChildren()} fallback={<span class="w-4" />}>
                <Show when={node().loading} fallback={
                  <span class="inline-block transition-transform" classList={{ "rotate-90": node().expanded }}>
                    ▶
                  </span>
                }>
                  <span class="animate-spin">◌</span>
                </Show>
              </Show>
            </span>
            {/* Icon */}
            <span class="w-5 text-text-muted text-xs flex-shrink-0">
              {hasChildren() ? "📁" : "📄"}
            </span>
            {/* Path */}
            <span class="flex-1 text-xs font-mono text-text-default truncate" title={node().fullPath}>
              {node().fullPath}
            </span>
            {/* Stats */}
            <div class="flex items-center space-x-3 text-xs flex-shrink-0 whitespace-nowrap">
              {/* Percentage */}
              <span class="w-12 text-right text-text-secondary">
                {node().percentage.toFixed(1)}%
              </span>
              {/* Size */}
              <span class="w-20 text-right font-mono text-text-tertiary">
                {formatBytes(node().estimatedSize, uiSettings().showRawBytes)}
              </span>
              {/* Samples */}
              <span class="w-16 text-right font-mono text-text-muted">
                {Number(node().samples).toLocaleString()}
              </span>
            </div>
          </div>
        </div>
        {/* Children */}
        <Show when={node().expanded && node().children}>
          <For each={node().children}>
            {(child, i) => (
              <TreeNodeRow
                node={child}
                depth={props.depth + 1}
                path={[...props.path, i()]}
                maxSamples={props.maxSamples}
              />
            )}
          </For>
        </Show>
      </>
    );
  };

  return (
    <div class="space-y-2">
      {/* Header with stats */}
      <div class="bg-bg-surface border border-border-default">
        {/* Top bar with controls */}
        <div class="flex items-center justify-between px-3 py-2 border-b border-border-subtle">
          {/* Stats */}
          <div class="flex items-center space-x-4 text-xs">
            <Show when={!initialLoading()} fallback={
              <>
                <div class="flex items-center">
                  <span class="text-text-tertiary">samples </span>
                  <span class="w-12 h-3 bg-bg-muted rounded animate-pulse ml-1" />
                </div>
                <div class="flex items-center">
                  <span class="text-text-tertiary">size </span>
                  <span class="w-14 h-3 bg-bg-muted rounded animate-pulse ml-1" />
                </div>
                <div class="flex items-center">
                  <span class="text-text-tertiary">time </span>
                  <span class="w-10 h-3 bg-bg-muted rounded animate-pulse ml-1" />
                </div>
              </>
            }>
              <div>
                <span class="text-text-tertiary">samples </span>
                <span class="font-mono text-text-default">{sampleCount().toLocaleString()}</span>
              </div>
              <div>
                <span class="text-text-tertiary">size </span>
                <span class="font-mono text-text-default">{formatBytes(totalSize(), uiSettings().showRawBytes)}</span>
              </div>
              <div>
                <span class="text-text-tertiary">time </span>
                <span class="font-mono text-text-default">{elapsed()}</span>
              </div>
              <Show when={isRunning()}>
                <div>
                  <span class="text-text-tertiary">rate </span>
                  <span class="font-mono text-text-default">{samplesPerSec().toFixed(0)}/s</span>
                </div>
              </Show>
            </Show>
          </div>
          {/* Controls */}
          <div class="flex items-center space-x-2">
            {/* Auto-refresh pause toggle with countdown */}
            <div class="flex items-center gap-1">
              <Show when={!autoRefreshPaused() && !initialLoading()}>
                <CountdownCircle countdown={treeRefreshCountdown()} total={TREE_REFRESH_SECS} />
              </Show>
              <button
                class={`px-2 py-1 text-xs cursor-pointer ${
                  autoRefreshPaused()
                    ? "bg-warning-soft text-text-inverse hover:bg-warning"
                    : "bg-bg-muted text-text-secondary hover:bg-border-strong"
                }`}
                onClick={() => setAutoRefreshPaused(!autoRefreshPaused())}
                title={autoRefreshPaused() ? "Resume auto-refresh" : "Pause auto-refresh"}
                disabled={initialLoading()}
              >
                {autoRefreshPaused() ? "paused" : "live"}
              </button>
            </div>
            <Show when={initialLoading()}>
              <span class="text-xs text-text-muted animate-pulse">loading...</span>
            </Show>
            <Show when={!initialLoading()}>
              <Show
                when={isRunning()}
                fallback={
                  <div class="flex space-x-1">
                    <button
                      class="px-2 py-1 text-xs cursor-pointer bg-primary text-text-inverse hover:bg-primary-hover"
                      onClick={handleStartSampling}
                    >
                      {hasSession() ? "resume" : "start"}
                    </button>
                    <Show when={hasSession()}>
                      <button
                        class="px-2 py-1 text-xs cursor-pointer bg-interactive-soft text-text-inverse hover:bg-interactive"
                        onClick={handleClear}
                      >
                        clear
                      </button>
                    </Show>
                  </div>
                }
              >
                <button
                  class="px-2 py-1 text-xs cursor-pointer bg-error text-text-inverse hover:bg-error-hover"
                  onClick={handleStopSampling}
                >
                  stop
                </button>
              </Show>
            </Show>
          </div>
        </div>
        {/* Progress bar (if sampling) */}
        <Show when={isRunning()}>
          <div class="h-1 bg-bg-base">
            <div class="h-1 bg-primary-soft animate-pulse w-full" />
          </div>
          <div class="px-3 py-1 text-xs text-text-tertiary font-mono truncate border-t border-border-subtle">
            {currentSamplingPath() || "sampling..."}
          </div>
        </Show>
      </div>

      {/* Tree view */}
      <Show
        when={treeData.length > 0}
        fallback={
          <div class="bg-bg-surface border border-border-default p-4 text-xs text-text-tertiary text-center">
            <Show when={!initialLoading() && !treeLoading()} fallback={
              <div class="flex items-center justify-center space-x-2">
                <span class="animate-spin">◌</span>
                <span>{initialLoading() ? "checking for existing session..." : "loading tree data..."}</span>
              </div>
            }>
              {hasSession() ? "no data yet" : "click start to begin sampling"}
            </Show>
          </div>
        }
      >
        <div class="bg-bg-surface border border-border-default">
          {/* Column headers */}
          <div class="flex items-center py-1 pr-2 border-b border-border-subtle bg-bg-surface-raised text-xs" style={{ "padding-left": "8px" }}>
            {/* Reset sort button */}
            <button
              class="w-4 flex-shrink-0 text-text-muted hover:text-text-default cursor-pointer"
              onClick={() => { setSortBy("size"); setSortDesc(true); refreshTree(); }}
              title="Reset sort to size descending"
            >
              ↺
            </button>
            {/* Spacer for folder icon */}
            <span class="w-5 flex-shrink-0" />
            {/* Path header */}
            <button
              class={`flex-1 text-left cursor-pointer hover:text-text-default ${
                sortBy() === "name" ? "text-text-default font-medium" : "text-text-tertiary"
              }`}
              onClick={() => handleSortClick("name")}
            >
              path {sortBy() === "name" && (sortDesc() ? "↓" : "↑")}
              <span class="text-text-muted ml-2">({treeData.length.toLocaleString()} items)</span>
            </button>
            {/* Stats headers */}
            <div class="flex items-center space-x-3 flex-shrink-0 whitespace-nowrap">
              <span class="w-12 text-right text-text-tertiary">%</span>
              <button
                class={`w-20 text-right cursor-pointer hover:text-text-default ${
                  sortBy() === "size" ? "text-text-default font-medium" : "text-text-tertiary"
                }`}
                onClick={() => handleSortClick("size")}
              >
                size {sortBy() === "size" && (sortDesc() ? "↓" : "↑")}
              </button>
              <button
                class={`w-16 text-right cursor-pointer hover:text-text-default ${
                  sortBy() === "samples" ? "text-text-default font-medium" : "text-text-tertiary"
                }`}
                onClick={() => handleSortClick("samples")}
              >
                samples {sortBy() === "samples" && (sortDesc() ? "↓" : "↑")}
              </button>
            </div>
          </div>
          {/* Tree items */}
          <div class="divide-y divide-border-subtle">
            <For each={treeData}>
              {(node, i) => (
                <TreeNodeRow
                  node={node}
                  depth={0}
                  path={[i()]}
                  maxSamples={maxSamples()}
                />
              )}
            </For>
          </div>
        </div>
      </Show>
    </div>
  );
}
