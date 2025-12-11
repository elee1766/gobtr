import { Show, type JSX, type Component, onMount, onCleanup } from "solid-js";
import { Portal } from "solid-js/web";

export interface ModalProps {
  open: boolean;
  onClose: () => void;
  title?: string;
  children: JSX.Element;
}

export const Modal: Component<ModalProps> = (props) => {
  // Handle escape key
  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Escape" && props.open) {
      props.onClose();
    }
  };

  onMount(() => {
    document.addEventListener("keydown", handleKeyDown);
  });

  onCleanup(() => {
    document.removeEventListener("keydown", handleKeyDown);
  });

  return (
    <Show when={props.open}>
      <Portal>
        <div
          class="fixed inset-0 z-50 flex items-center justify-center"
          onClick={(e) => {
            if (e.target === e.currentTarget) props.onClose();
          }}
        >
          {/* Backdrop */}
          <div class="absolute inset-0 bg-black/50" />

          {/* Modal content */}
          <div class="relative bg-bg-surface border border-border-default rounded shadow-lg max-w-md w-full mx-4">
            <Show when={props.title}>
              <div class="px-4 py-2 border-b border-border-subtle bg-bg-surface-raised">
                <h2 class="text-sm font-medium text-text-default">{props.title}</h2>
              </div>
            </Show>
            <div class="p-4">
              {props.children}
            </div>
          </div>
        </div>
      </Portal>
    </Show>
  );
};
