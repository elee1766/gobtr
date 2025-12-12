import { JSX } from "solid-js";
import {
  uiSettings,
  setShowRawBytes,
  setAutoRefreshEnabled,
  setAutoRefreshInterval,
  setMaxByteUnit,
  setByteBase,
  type ByteUnit,
  type ByteBase,
} from "@/stores/ui";
import { ToggleGroup, NumberInput } from "@/components/ui";

// Setting row with label, description, and control
function SettingRow(props: { label: string; description: string; children: JSX.Element }) {
  return (
    <div class="flex items-center justify-between">
      <div>
        <div class="text-xs font-medium text-text-default">{props.label}</div>
        <div class="text-xs text-text-tertiary">{props.description}</div>
      </div>
      {props.children}
    </div>
  );
}

export default function Settings() {
  return (
    <div class="space-y-4">
      {/* UI Settings */}
      <section class="bg-bg-surface border border-border-default">
        <div class="px-3 py-2 bg-bg-surface-raised border-b border-border-subtle">
          <h2 class="text-sm font-medium text-text-default">UI Settings</h2>
          <p class="text-xs text-text-tertiary">Display preferences saved to your browser</p>
        </div>
        <div class="p-3 space-y-4">
          {/* Number format */}
          <SettingRow label="Number Format" description="How to display byte values by default">
            <ToggleGroup
              options={[
                { label: "human", value: false },
                { label: "raw", value: true },
              ]}
              value={uiSettings().showRawBytes}
              onChange={setShowRawBytes}
            />
          </SettingRow>

          {/* Byte units and max unit */}
          <SettingRow label="Byte Units" description="Binary (1024) or decimal (1000), and max unit to display">
            <div class="flex gap-2">
              <ToggleGroup
                options={[
                  { label: "binary", value: "binary" as ByteBase },
                  { label: "decimal", value: "decimal" as ByteBase },
                ]}
                value={uiSettings().byteBase}
                onChange={setByteBase}
              />
              <ToggleGroup
                options={[
                  { label: uiSettings().byteBase === "binary" ? "MiB" : "MB", value: "MB" as ByteUnit },
                  { label: uiSettings().byteBase === "binary" ? "GiB" : "GB", value: "GB" as ByteUnit },
                  { label: uiSettings().byteBase === "binary" ? "TiB" : "TB", value: "TB" as ByteUnit },
                  { label: uiSettings().byteBase === "binary" ? "PiB" : "PB", value: "PB" as ByteUnit },
                ]}
                value={uiSettings().maxByteUnit}
                onChange={setMaxByteUnit}
              />
            </div>
          </SettingRow>

          {/* Auto-refresh */}
          <SettingRow label="Auto Refresh" description="Automatically refresh data periodically">
            <ToggleGroup
              options={[
                { label: "off", value: false },
                { label: "on", value: true },
              ]}
              value={uiSettings().autoRefreshEnabled}
              onChange={setAutoRefreshEnabled}
            />
          </SettingRow>

          <SettingRow label="Refresh Interval" description="How often to refresh when auto-refresh is enabled">
            <NumberInput
              value={uiSettings().autoRefreshInterval}
              onChange={setAutoRefreshInterval}
              min={1}
              max={300}
              suffix="sec"
            />
          </SettingRow>
        </div>
      </section>
    </div>
  );
}
