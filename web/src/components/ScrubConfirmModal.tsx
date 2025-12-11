import { createSignal, createEffect } from "solid-js";
import { Modal, Button } from "@/components/ui";

export interface ScrubOptions {
  readonly: boolean;
  limitBytesPerSec: bigint;
  force: boolean;
}

export interface ScrubConfirmModalProps {
  open: boolean;
  onClose: () => void;
  onConfirm: (opts: ScrubOptions) => void;
  initialReadonly?: boolean;
}

export function ScrubConfirmModal(props: ScrubConfirmModalProps) {
  const [readonly, setReadonly] = createSignal(props.initialReadonly ?? false);
  const [limitMB, setLimitMB] = createSignal("");
  const [force, setForce] = createSignal(false);

  // Sync readonly state when modal opens with new initialReadonly value
  createEffect(() => {
    if (props.open) {
      setReadonly(props.initialReadonly ?? false);
    }
  });

  const handleConfirm = () => {
    const limitVal = parseFloat(limitMB()) || 0;
    props.onConfirm({
      readonly: readonly(),
      limitBytesPerSec: BigInt(Math.floor(limitVal * 1024 * 1024)),
      force: force(),
    });
    // Reset fields
    setLimitMB("");
    setForce(false);
  };

  const handleClose = () => {
    // Reset fields
    setLimitMB("");
    setForce(false);
    props.onClose();
  };

  return (
    <Modal open={props.open} onClose={handleClose} title="Start Scrub">
      <div class="space-y-4">
        <p class="text-xs text-text-tertiary">
          Configure scrub options before starting.
        </p>

        <div class="space-y-3">
          {/* Readonly checkbox */}
          <label class="flex items-center space-x-2 cursor-pointer">
            <input
              type="checkbox"
              checked={readonly()}
              onChange={(e) => setReadonly(e.currentTarget.checked)}
              class="w-4 h-4 accent-primary"
            />
            <span class="text-sm text-text-default">Read-only mode</span>
            <span class="text-xs text-text-muted">(-r)</span>
          </label>
          <p class="text-xs text-text-muted ml-6">
            Don't try to repair anything, just verify checksums.
          </p>

          {/* Rate limit input */}
          <div class="space-y-1">
            <label class="text-sm text-text-default">
              Rate limit <span class="text-xs text-text-muted">(--limit)</span>
            </label>
            <div class="flex items-center space-x-2">
              <input
                type="number"
                min="0"
                step="1"
                value={limitMB()}
                onInput={(e) => setLimitMB(e.currentTarget.value)}
                placeholder="0"
                class="w-24 px-2 py-1 text-sm bg-bg-default border border-border-default rounded focus:outline-none focus:border-primary"
              />
              <span class="text-sm text-text-muted">MB/s (0 = unlimited)</span>
            </div>
            <p class="text-xs text-text-muted">
              Limit I/O per second to reduce system load.
            </p>
          </div>

          {/* Force checkbox */}
          <label class="flex items-center space-x-2 cursor-pointer">
            <input
              type="checkbox"
              checked={force()}
              onChange={(e) => setForce(e.currentTarget.checked)}
              class="w-4 h-4 accent-primary"
            />
            <span class="text-sm text-text-default">Force</span>
            <span class="text-xs text-text-muted">(-f)</span>
          </label>
          <p class="text-xs text-text-muted ml-6">
            Force starting even if a scrub is already running (only use if previous scrub crashed).
          </p>
        </div>

        {/* Buttons */}
        <div class="flex justify-end space-x-2 pt-2 border-t border-border-subtle">
          <Button variant="soft" onClick={handleClose}>
            Cancel
          </Button>
          <Button variant="primary" onClick={handleConfirm}>
            Start Scrub
          </Button>
        </div>
      </div>
    </Modal>
  );
}
