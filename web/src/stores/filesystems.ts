import { createSignal } from "solid-js";
import type { TrackedFilesystem } from "%/v1/filesystem_pb";

// Atom-style global state for filesystem metadata cache
// Used by breadcrumbs and other components that need fs info without refetching
const [fsCache, setFsCache] = createSignal<Map<string, TrackedFilesystem>>(new Map());

export function cacheFsData(fs: TrackedFilesystem) {
  setFsCache(prev => {
    const next = new Map(prev);
    next.set(String(fs.id), fs);
    return next;
  });
}

export function getCachedFs(id: string): TrackedFilesystem | undefined {
  return fsCache().get(id);
}

export function clearFsCache() {
  setFsCache(new Map());
}
