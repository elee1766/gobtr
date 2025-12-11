import { type ParentComponent, createMemo, createEffect, onCleanup, Show, Suspense } from "solid-js";
import { A, useLocation } from "@solidjs/router";
import { Toaster } from "solid-toast";
import { getCachedFs } from "@/stores/filesystems";
import { uiSettings } from "@/stores/ui";

const App: ParentComponent = (props) => {
  const location = useLocation();
  const isSettings = createMemo(() => location.pathname === "/settings");

  const handleRefresh = () => {
    window.dispatchEvent(new CustomEvent("refresh"));
  };

  // Auto-refresh timer (controlled via settings only)
  createEffect(() => {
    const settings = uiSettings();
    if (!settings.autoRefreshEnabled) {
      return;
    }

    const interval = setInterval(() => {
      window.dispatchEvent(new CustomEvent("refresh"));
    }, settings.autoRefreshInterval * 1000);

    onCleanup(() => clearInterval(interval));
  });

  return (
    <div class="min-h-screen bg-bg-base">
      <header class="bg-bg-surface border-b border-border-default select-none" draggable={false}>
        <div class="container mx-auto px-3">
          <div class="flex items-center h-8 text-xs">
            <Breadcrumb />
            <div class="flex-1" />
            {/* Refresh button (hidden on settings page) */}
            <Show when={!isSettings()}>
              <button
                class="px-2 py-1 text-text-tertiary hover:bg-bg-surface-raised cursor-pointer"
                onClick={handleRefresh}
              >
                refresh
              </button>
            </Show>
            <Show
              when={isSettings()}
              fallback={
                <A
                  href="/settings"
                  class="px-2 py-1 text-text-tertiary hover:bg-bg-surface-raised"
                  draggable={false}
                >
                  settings
                </A>
              }
            >
              <button
                class="px-2 py-1 text-text-tertiary hover:bg-bg-surface-raised cursor-pointer"
                onClick={() => history.back()}
              >
                ← back
              </button>
            </Show>
          </div>
        </div>
      </header>
      <main class="container mx-auto px-3 py-4">
        <div class="max-w-7xl mx-auto">
          <Suspense fallback={<div class="text-xs text-text-tertiary p-2">loading...</div>}>
            {props.children}
          </Suspense>
        </div>
      </main>
      <Toaster position="bottom-right" gutter={8} />
    </div>
  );
};

function Breadcrumb() {
  const location = useLocation();
  const path = createMemo(() => location.pathname);

  // Parse filesystem route: /fs/:id or /fs/:id/:tab
  const fsInfo = createMemo(() => {
    const match = path().match(/^\/fs\/(\d+)(\/(.+))?$/);
    if (!match) return null;
    return { id: match[1], tab: match[3] };
  });

  const isHome = createMemo(() => path() === "/" || path() === "");
  const isSettings = createMemo(() => path() === "/settings");

  return (
    <div class="flex items-baseline">
      <Show when={isHome()}>
        <span class="text-text-secondary">filesystems</span>
      </Show>
      <Show when={isSettings()}>
        <span class="text-text-secondary">filesystems</span>
      </Show>
      <Show when={!isHome() && !isSettings()}>
        <A href="/" class="text-text-muted hover:text-text-secondary" draggable={false}>filesystems</A>
        <Show when={fsInfo()}>
          {(info) => (
            <>
              <span class="text-text-faint mx-1">/</span>
              <FsBreadcrumbPart id={info().id} tab={info().tab} />
            </>
          )}
        </Show>
      </Show>
    </div>
  );
}

function FsBreadcrumbPart(props: { id: string; tab?: string }) {
  const fs = createMemo(() => getCachedFs(props.id));
  const displayName = createMemo(() => {
    const f = fs();
    if (f?.label) return f.label;
    if (f?.path) {
      const parts = f.path.replace(/\/$/, "").split("/");
      return parts[parts.length - 1] || f.path;
    }
    return props.id;
  });

  return (
    <>
      <A href={`/fs/${props.id}`} class="text-text-secondary hover:text-text-default" draggable={false}>
        {displayName()}
      </A>
      <Show when={props.tab}>
        <span class="text-text-faint mx-1">/</span>
        <span class="text-text-secondary">{props.tab}</span>
      </Show>
    </>
  );
}

export default App;
