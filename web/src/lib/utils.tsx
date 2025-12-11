import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";
import toast from "solid-toast";
import type { ByteUnit, ByteBase } from "@/stores/ui";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// Custom toast with Tailwind styling
export function showToast(message: string, type: "success" | "error" = "success") {
  toast.custom(
    (t) => (
      <div
        class={cn(
          "px-3 py-2 text-xs border rounded shadow-sm transition-opacity",
          "bg-bg-surface text-text-default border-border-default",
          t.visible ? "opacity-100" : "opacity-0"
        )}
      >
        <span class={type === "error" ? "text-error" : "text-text-default"}>
          {message}
        </span>
      </div>
    ),
    { duration: 2000 }
  );
}

// Copy to clipboard with toast notification
export async function copyToClipboard(text: string, label: string = "Copied") {
  try {
    await navigator.clipboard.writeText(text);
    const display = text.length > 30 ? text.slice(0, 30) + "..." : text;
    showToast(`${label}: ${display}`);
  } catch {
    showToast("Failed to copy", "error");
  }
}

// Truncate UUID to show first 4 and last 4 chars
export function truncateUuid(uuid: string): string {
  if (!uuid || uuid.length < 12) return uuid || "-";
  return `${uuid.slice(0, 4)}...${uuid.slice(-4)}`;
}

// Helper functions

export function isInBtrbkDir(path: string, btrbkDir: string): boolean {
  if (!btrbkDir) return false;
  const normalizedDir = btrbkDir.replace(/^\//, "");
  const normalizedPath = path.replace(/^\//, "");
  return normalizedPath.startsWith(normalizedDir + "/");
}

export function countBackups(subvolumes: { path: string }[], btrbkDir: string): number {
  return subvolumes.filter(sv => isInBtrbkDir(sv.path, btrbkDir)).length;
}

// Unit definitions for binary (1024) and decimal (1000) bases
const BINARY_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"] as const;
const DECIMAL_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"] as const;
const UNIT_INDEX: Record<ByteUnit, number> = { B: 0, KB: 1, MB: 2, GB: 3, TB: 4, PB: 5 };

export interface FormatBytesOptions {
  maxUnit?: ByteUnit;
  base?: ByteBase;
}

export function formatBytes(
  bytes: bigint | number,
  raw: boolean = false,
  options: FormatBytesOptions = {}
): string {
  const { value, unit } = formatBytesParts(bytes, options);
  if (raw) return Number(bytes).toLocaleString();
  return `${value} ${unit}`;
}

export function formatBytesParts(
  bytes: bigint | number,
  options: FormatBytesOptions = {}
): { value: string; unit: string } {
  const { maxUnit = "TB", base = "binary" } = options;
  const n = typeof bytes === "bigint" ? Number(bytes) : bytes;
  const divisor = base === "binary" ? 1024 : 1000;
  const units = base === "binary" ? BINARY_UNITS : DECIMAL_UNITS;
  const maxIndex = UNIT_INDEX[maxUnit];

  let i = 0;
  let size = n;
  while (size >= divisor && i < maxIndex && i < units.length - 1) {
    size /= divisor;
    i++;
  }

  return {
    value: i > 0 ? size.toFixed(1) : size.toString(),
    unit: units[i],
  };
}

export function formatRelativeTime(date: Date, now: Date = new Date()): string {
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  const diffMin = Math.floor(diffSec / 60);
  const diffHour = Math.floor(diffMin / 60);
  const diffDay = Math.floor(diffHour / 24);
  const diffWeek = Math.floor(diffDay / 7);
  const diffMonth = Math.floor(diffDay / 30);
  const diffYear = Math.floor(diffDay / 365);

  if (diffSec < 60) return "just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  if (diffHour < 24) return `${diffHour}h ago`;
  if (diffDay === 1) return "yesterday";
  if (diffDay < 7) return `${diffDay}d ago`;
  if (diffWeek === 1) return "1 week ago";
  if (diffWeek < 4) return `${diffWeek} weeks ago`;
  if (diffMonth === 1) return "1 month ago";
  if (diffMonth < 12) return `${diffMonth} months ago`;
  if (diffYear === 1) return "1 year ago";
  return `${diffYear} years ago`;
}

// Format numbers with commas (locale-aware thousands separators)
export function formatNumber(n: number | bigint): string {
  return Number(n).toLocaleString();
}

export function formatRelativeTimeShort(date: Date, now: Date = new Date()): string {
  const diffMs = now.getTime() - date.getTime();
  const diffHour = Math.floor(diffMs / (1000 * 60 * 60));
  const diffDay = Math.floor(diffHour / 24);
  const remainingHours = diffHour % 24;
  const diffWeek = Math.floor(diffDay / 7);
  const diffMonth = Math.floor(diffDay / 30);
  const diffYear = Math.floor(diffDay / 365);

  if (diffHour < 1) return "now";
  if (diffHour < 24) return `${diffHour}h`;
  if (diffDay < 7) {
    return remainingHours > 0 ? `${diffDay}d ${remainingHours}h` : `${diffDay}d`;
  }
  if (diffWeek < 4) return `${diffWeek}w`;
  if (diffMonth < 12) return `${diffMonth}mo`;
  return `${diffYear}y`;
}
