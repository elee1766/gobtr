import { createSignal } from "solid-js";
import { Modal, Button } from "@/components/ui";
import type { BalanceFilters } from "%/v1/balance_pb";

export interface BalanceOptions {
  filters?: BalanceFilters;
  limitPercent: number;
  background: boolean;
  dryRun: boolean;
  force: boolean;
}

export interface BalanceConfirmModalProps {
  open: boolean;
  onClose: () => void;
  onConfirm: (opts: BalanceOptions) => void;
}

export function BalanceConfirmModal(props: BalanceConfirmModalProps) {
  const [data, setData] = createSignal(true);
  const [metadata, setMetadata] = createSignal(true);
  const [system, setSystem] = createSignal(false);
  const [usagePercent, setUsagePercent] = createSignal("50");
  const [limitChunks, setLimitChunks] = createSignal("");
  const [dryRun, setDryRun] = createSignal(false);
  const [force, setForce] = createSignal(false);

  const handleConfirm = () => {
    const usage = parseInt(usagePercent()) || 0;
    const limit = parseInt(limitChunks()) || 0;

    props.onConfirm({
      filters: {
        $typeName: "api.v1.BalanceFilters",
        data: data(),
        metadata: metadata(),
        system: system(),
        usagePercent: usage,
        limitChunks: BigInt(limit),
      },
      limitPercent: 0, // Not commonly used
      background: false,
      dryRun: dryRun(),
      force: force(),
    });
    // Reset fields
    resetFields();
  };

  const resetFields = () => {
    setData(true);
    setMetadata(true);
    setSystem(false);
    setUsagePercent("50");
    setLimitChunks("");
    setDryRun(false);
    setForce(false);
  };

  const handleClose = () => {
    resetFields();
    props.onClose();
  };

  return (
    <Modal open={props.open} onClose={handleClose} title="Start Balance">
      <div class="space-y-4">
        <p class="text-xs text-text-tertiary">
          Balance redistributes data across devices to optimize space usage.
          This can take a long time depending on filesystem size.
        </p>

        <div class="space-y-3">
          {/* Data type filters */}
          <div class="space-y-2">
            <span class="text-sm text-text-default">Balance types</span>
            <div class="flex space-x-4">
              <label class="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={data()}
                  onChange={(e) => setData(e.currentTarget.checked)}
                  class="w-4 h-4 accent-primary"
                />
                <span class="text-sm text-text-secondary">Data</span>
              </label>
              <label class="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={metadata()}
                  onChange={(e) => setMetadata(e.currentTarget.checked)}
                  class="w-4 h-4 accent-primary"
                />
                <span class="text-sm text-text-secondary">Metadata</span>
              </label>
              <label class="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={system()}
                  onChange={(e) => setSystem(e.currentTarget.checked)}
                  class="w-4 h-4 accent-primary"
                />
                <span class="text-sm text-text-secondary">System</span>
              </label>
            </div>
          </div>

          {/* Usage filter */}
          <div class="space-y-1">
            <label class="text-sm text-text-default">
              Usage filter <span class="text-xs text-text-muted">(-dusage, -musage)</span>
            </label>
            <div class="flex items-center space-x-2">
              <input
                type="number"
                min="0"
                max="100"
                step="1"
                value={usagePercent()}
                onInput={(e) => setUsagePercent(e.currentTarget.value)}
                placeholder="0"
                class="w-20 px-2 py-1 text-sm bg-bg-default border border-border-default rounded focus:outline-none focus:border-primary"
              />
              <span class="text-sm text-text-muted">% (0 = all chunks)</span>
            </div>
            <p class="text-xs text-text-muted">
              Only balance chunks with usage at or below this percentage.
              50% is a safe default that targets half-empty chunks.
            </p>
          </div>

          {/* Chunk limit */}
          <div class="space-y-1">
            <label class="text-sm text-text-default">
              Chunk limit <span class="text-xs text-text-muted">(-dlimit, -mlimit)</span>
            </label>
            <div class="flex items-center space-x-2">
              <input
                type="number"
                min="0"
                step="1"
                value={limitChunks()}
                onInput={(e) => setLimitChunks(e.currentTarget.value)}
                placeholder="0"
                class="w-24 px-2 py-1 text-sm bg-bg-default border border-border-default rounded focus:outline-none focus:border-primary"
              />
              <span class="text-sm text-text-muted">chunks (0 = no limit)</span>
            </div>
            <p class="text-xs text-text-muted">
              Limit total number of chunks to process. Useful for incremental balancing.
            </p>
          </div>

          {/* Dry run checkbox */}
          <label class="flex items-center space-x-2 cursor-pointer">
            <input
              type="checkbox"
              checked={dryRun()}
              onChange={(e) => setDryRun(e.currentTarget.checked)}
              class="w-4 h-4 accent-primary"
            />
            <span class="text-sm text-text-default">Dry run</span>
          </label>
          <p class="text-xs text-text-muted ml-6">
            Show what would be done without actually moving data.
          </p>

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
            Force starting even if a balance is already running.
          </p>
        </div>

        {/* Buttons */}
        <div class="flex justify-end space-x-2 pt-2 border-t border-border-subtle">
          <Button variant="soft" onClick={handleClose}>
            Cancel
          </Button>
          <Button variant="primary" onClick={handleConfirm}>
            Start Balance
          </Button>
        </div>
      </div>
    </Modal>
  );
}
