import { createSignal, createEffect } from "solid-js";

// SI unit types
export type ByteUnit = "B" | "KB" | "MB" | "GB" | "TB" | "PB";
export type ByteBase = "binary" | "decimal"; // 1024 vs 1000

// UI Settings stored in localStorage
export interface UISettings {
  showRawBytes: boolean;
  autoRefreshEnabled: boolean;
  autoRefreshInterval: number; // seconds
  maxByteUnit: ByteUnit; // Maximum unit to display (e.g., "GB" means never show TB)
  byteBase: ByteBase; // binary (1024, KiB/MiB) or decimal (1000, KB/MB)
}

const STORAGE_KEY = "btrfsguid-ui-settings";

const defaultSettings: UISettings = {
  showRawBytes: false,
  autoRefreshEnabled: false,
  autoRefreshInterval: 30,
  maxByteUnit: "TB",
  byteBase: "binary",
};

function loadSettings(): UISettings {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      return { ...defaultSettings, ...JSON.parse(stored) };
    }
  } catch {
    // Ignore parse errors
  }
  return defaultSettings;
}

const [settings, setSettings] = createSignal<UISettings>(loadSettings());

// Persist to localStorage on change
createEffect(() => {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(settings()));
});

// Getters
export function getShowRawBytes(): boolean {
  return settings().showRawBytes;
}

// Setters
export function setShowRawBytes(value: boolean) {
  setSettings(prev => ({ ...prev, showRawBytes: value }));
}

export function setAutoRefreshEnabled(value: boolean) {
  setSettings(prev => ({ ...prev, autoRefreshEnabled: value }));
}

export function setAutoRefreshInterval(value: number) {
  setSettings(prev => ({ ...prev, autoRefreshInterval: Math.max(1, value) }));
}

export function setMaxByteUnit(value: ByteUnit) {
  setSettings(prev => ({ ...prev, maxByteUnit: value }));
}

export function setByteBase(value: ByteBase) {
  setSettings(prev => ({ ...prev, byteBase: value }));
}

// For reactive access in components
export function useUISettings() {
  return {
    get showRawBytes() {
      return settings().showRawBytes;
    },
    setShowRawBytes,
  };
}

// Get full settings signal for reactive updates
export { settings as uiSettings };
