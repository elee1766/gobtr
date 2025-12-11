import { Show, For, createResource, onMount, onCleanup, Suspense } from "solid-js";
import { filesystemClient } from "@/api/client";

async function loadErrors(fsPath: string) {
  const resp = await filesystemClient.getErrors({ device: fsPath, limit: 1000 });
  return resp.errors;
}

export function ErrorsTab(props: { fsPath: string }) {
  const [errors, { refetch }] = createResource(() => props.fsPath, loadErrors);

  // Listen for refresh events
  onMount(() => {
    const handler = () => refetch();
    window.addEventListener("fs-refresh", handler);
    onCleanup(() => window.removeEventListener("fs-refresh", handler));
  });

  const errorTypeClass = (errType: string) => {
    if (errType === "corruption_err") return "text-error";
    if (errType === "generation_err") return "text-warning-soft";
    if (errType.includes("io_err")) return "text-warning";
    return "text-text-tertiary";
  };

  return (
    <Suspense fallback={<div class="text-xs text-text-tertiary">loading errors...</div>}>
      <Show when={errors()} keyed>
        {(errs) => (
          <div class="bg-bg-surface border border-border-default">
            <Show when={errs.length === 0}>
              <div class="text-xs text-success p-3">no errors</div>
            </Show>
            <Show when={errs.length > 0}>
              <For each={errs}>
                {(err) => (
                  <div class="px-2 py-1 border-b border-border-subtle text-xs hover:bg-bg-surface-raised">
                    <div class="flex items-center space-x-2">
                      <span class={errorTypeClass(err.errorType)}>{err.errorType}</span>
                      <span class="text-text-muted">
                        {new Date(Number(err.timestamp) * 1000).toLocaleString("sv-SE", { dateStyle: "short", timeStyle: "short" })}
                      </span>
                      <span class="text-text-secondary font-mono">{err.device}</span>
                      <Show when={err.message}>
                        <span class="text-text-default">{err.message}</span>
                      </Show>
                    </div>
                    <Show when={err.path}>
                      <div class="text-text-tertiary font-mono pl-4 truncate">{err.path}</div>
                    </Show>
                  </div>
                )}
              </For>
            </Show>
          </div>
        )}
      </Show>
    </Suspense>
  );
}
