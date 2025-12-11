import { createSignal, createResource, Show, For, createMemo, onMount, onCleanup, createEffect, Suspense } from "solid-js";
import { fragmapClient } from "@/api/client";
import { formatBytes, formatNumber } from "@/lib/utils";
import { uiSettings } from "@/stores/ui";
import type { BlockMapEntry, FragStats, Device } from "%/v1/fragmap_pb";
import { Poline, positionFunctions } from "poline";

// Convert HSL to RGB
const hslToRgb = (h: number, s: number, l: number): [number, number, number] => {
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const x = c * (1 - Math.abs(((h / 60) % 2) - 1));
  const m = l - c / 2;
  let r = 0, g = 0, b = 0;
  if (h < 60) { r = c; g = x; }
  else if (h < 120) { r = x; g = c; }
  else if (h < 180) { g = c; b = x; }
  else if (h < 240) { g = x; b = c; }
  else if (h < 300) { r = x; b = c; }
  else { r = c; b = x; }
  return [
    Math.round((r + m) * 255),
    Math.round((g + m) * 255),
    Math.round((b + m) * 255),
  ];
};

// Generate device palette using poline - rainbow with consistent lightness
const generateDevicePalette = (numDevices: number): [number, number, number][] => {
  // Rainbow anchor colors starting from cyan/blue, consistent saturation & lightness
  const anchorColors: [number, number, number][] = [
    [180, 0.60, 0.42],  // cyan
    [220, 0.65, 0.45],  // blue
    [270, 0.60, 0.45],  // purple
    [320, 0.65, 0.45],  // magenta
    [0, 0.65, 0.45],    // red
    [30, 0.65, 0.45],   // orange
    [60, 0.65, 0.45],   // yellow
    [120, 0.60, 0.40],  // green
  ];

  const poline = new Poline({
    anchorColors,
    numPoints: Math.max(Math.ceil(numDevices / (anchorColors.length - 1)), 2),
    positionFunction: positionFunctions.linearPosition,
  });

  return poline.colors.slice(0, numDevices).map(([h, s, l]) => {
    // Clamp lightness to ensure consistent range for utilization display
    const clampedL = Math.max(0.38, Math.min(0.48, l));
    return hslToRgb(h, s, clampedL);
  });
};

interface Props {
  fsPath: string;
}

// Color mapping for block types - using semantic viz colors
const typeColors = {
  data: { bg: "bg-viz-data", hex: "var(--color-viz-data)", isFree: false, rgb: [59, 130, 246] as [number, number, number] },
  metadata: { bg: "bg-viz-metadata", hex: "var(--color-viz-metadata)", isFree: false, rgb: [168, 85, 247] as [number, number, number] },
  system: { bg: "bg-viz-system", hex: "var(--color-viz-system)", isFree: false, rgb: [249, 115, 22] as [number, number, number] },
  free: { bg: "bg-viz-free", hex: "var(--color-viz-free)", isFree: true, rgb: [229, 231, 235] as [number, number, number] },
};

function getBlockTypeColor(type: bigint): { bg: string; hex: string; isFree: boolean; rgb: [number, number, number] } {
  const t = Number(type);
  if (t & 1) return typeColors.data;
  if (t & 4) return typeColors.metadata;
  if (t & 2) return typeColors.system;
  return typeColors.free;
}

// Block type constants for filtering
type BlockType = "data" | "metadata" | "system" | "free";

function getBlockType(type: bigint, allocated: boolean): BlockType {
  if (!allocated) return "free";
  const t = Number(type);
  if (t & 1) return "data";
  if (t & 4) return "metadata";
  if (t & 2) return "system";
  return "free";
}

export function FragMapViz(props: Props) {
  const [viewMode, setViewMode] = createSignal<"heatmap" | "blockmap">("blockmap");

  // Legend filter state - which type is selected and which index we're on
  const [legendFilter, setLegendFilter] = createSignal<{ type: BlockType; index: number } | null>(null);

  // Single fetch for all block maps - includes device info, entries, and stats
  const [allBlockMaps] = createResource(
    () => props.fsPath,
    async (fsPath) => {
      const resp = await fragmapClient.getDeviceBlockMaps({ fsPath, deviceIds: [] });
      return resp.maps.map((m) => ({
        device: m.device!,
        entries: m.entries,
        totalSize: m.totalSize,
        stats: m.stats || null,
      }));
    }
  );

  // Extract devices from block maps for legend
  const devices = () => allBlockMaps()?.map(m => m.device) || [];

  // Handle legend click - cycle through extents of that type
  const handleLegendClick = (type: BlockType) => {
    const current = legendFilter();
    if (current?.type === type) {
      // Same type clicked - go to next extent
      setLegendFilter({ type, index: current.index + 1 });
    } else {
      // Different type - start at first extent
      setLegendFilter({ type, index: 0 });
    }
  };

  // Clear filter when switching views
  createEffect(() => {
    viewMode();
    setLegendFilter(null);
  });

  return (
    <div class="space-y-3">
      {/* Controls */}
      <div class="flex items-center justify-between">
          <div class="flex items-center space-x-2">
            {/* View mode toggle */}
            <div class="flex">
              <button
                class={`px-2 py-1 text-xs cursor-pointer ${viewMode() === "blockmap" ? "bg-interactive text-text-inverse" : "bg-bg-muted text-text-secondary"}`}
                onClick={() => setViewMode("blockmap")}
              >
                layout
              </button>
              <button
                class={`px-2 py-1 text-xs cursor-pointer ${viewMode() === "heatmap" ? "bg-interactive text-text-inverse" : "bg-bg-muted text-text-secondary"}`}
                onClick={() => setViewMode("heatmap")}
              >
                density
              </button>
            </div>
          </div>

          {/* Legend - changes based on view mode */}
          <Show when={viewMode() === "blockmap"}>
            <div class="flex items-center space-x-3 text-xs">
              <For each={["data", "metadata", "system", "free"] as BlockType[]}>
                {(type) => (
                  <button
                    class={`flex items-center cursor-pointer hover:underline ${legendFilter()?.type === type ? "font-bold" : ""}`}
                    onClick={() => handleLegendClick(type)}
                    title={`Click to cycle through ${type} extents`}
                  >
                    <span class={`w-3 h-3 ${typeColors[type].bg} mr-1`}></span>
                    {type}
                  </button>
                )}
              </For>
            </div>
          </Show>
          <Show when={viewMode() === "heatmap" && devices().length > 0}>
            {(() => {
              const devs = devices();
              const palette = generateDevicePalette(Math.max(devs.length, 2));
              return (
                <div class="flex items-center space-x-4 text-xs">
                  {/* Device colors */}
                  <div class="flex items-center space-x-2">
                    <For each={devs}>
                      {(device, idx) => {
                        const [r, g, b] = palette[idx() % palette.length];
                        const color = `rgb(${r}, ${g}, ${b})`;
                        const label = device.path ? device.path.split('/').pop() : `dev${device.id}`;
                        return (
                          <span class="flex items-center">
                            <span class="w-3 h-3 mr-1" style={{ "background-color": color }}></span>
                            {label}
                          </span>
                        );
                      }}
                    </For>
                  </div>
                  {/* Utilization gradient */}
                  <div class="flex items-center space-x-1">
                    <span class="text-text-muted">0%</span>
                    {(() => {
                      const [r, g, b] = palette[0];
                      return (
                        <div class="w-16 h-3 rounded-sm" style={{
                          background: `linear-gradient(to right, rgb(240, 245, 250), rgb(${r}, ${g}, ${b}))`
                        }}></div>
                      );
                    })()}
                    <span class="text-text-muted">100%</span>
                  </div>
                </div>
              );
            })()}
          </Show>
        </div>

      {/* All devices visualization */}
      <Suspense fallback={
        <div class="h-32 flex items-center justify-center text-text-muted text-xs">
          loading...
        </div>
      }>
        <div class="space-y-4 relative">
          {/* Loading overlay - only show when refreshing (has data but loading) */}
          <Show when={allBlockMaps.loading && allBlockMaps()}>
            <div class="absolute top-0 right-0 text-xs text-text-muted">
              refreshing...
            </div>
          </Show>

          {/* Block map view - combined layout for all devices */}
          <Show when={viewMode() === "blockmap" && allBlockMaps()}>
            {(maps) => {
              const totalSize = () => maps().reduce((sum, m) => sum + Number(m.totalSize), 0);
              const totalAllocated = () => maps().reduce((sum, m) => sum + Number(m.stats?.allocatedSize || 0n), 0);
              const totalExtents = () => maps().reduce((sum, m) => sum + (m.stats?.numExtents || 0), 0);
              const usedPct = () => totalSize() > 0 ? ((totalAllocated() / totalSize()) * 100).toFixed(1) : "0";
              return (
                <div>
                  <div class="flex items-center justify-between mb-2">
                    <span class="text-xs font-medium">
                      {maps().length} device{maps().length !== 1 ? "s" : ""}
                      <span class="text-text-muted ml-2">({formatBytes(BigInt(totalSize()), uiSettings().showRawBytes)} total)</span>
                    </span>
                    <span class="text-xs text-text-secondary">
                      {usedPct()}% allocated
                      <span class="text-text-muted ml-2">
                        {formatNumber(totalExtents())} extents
                      </span>
                    </span>
                  </div>
                  <BaseGridView
                    maps={maps()}
                    showRawBytes={uiSettings().showRawBytes}
                    colorMode="type"
                    selectByType={legendFilter()}
                  />
                </div>
              );
            }}
          </Show>

          {/* Grid map view - stays mounted once data exists */}
          <Show when={viewMode() === "heatmap" && allBlockMaps()}>
            {(maps) => {
              const totalSize = () => maps().reduce((sum, m) => sum + Number(m.totalSize), 0);
              const totalAllocated = () => maps().reduce((sum, m) => sum + Number(m.stats?.allocatedSize || 0n), 0);
              const totalExtents = () => maps().reduce((sum, m) => sum + (m.stats?.numExtents || 0), 0);
              const usedPct = () => totalSize() > 0 ? ((totalAllocated() / totalSize()) * 100).toFixed(1) : "0";
              return (
                <div>
                  <div class="flex items-center justify-between mb-2">
                    <span class="text-xs font-medium">
                      {maps().length} device{maps().length !== 1 ? "s" : ""}
                      <span class="text-text-muted ml-2">({formatBytes(BigInt(totalSize()), uiSettings().showRawBytes)} total)</span>
                    </span>
                    <span class="text-xs text-text-secondary">
                      {usedPct()}% used
                      <span class="text-text-muted ml-2">
                        {formatNumber(totalExtents())} extents
                      </span>
                    </span>
                  </div>
                  <BaseGridView
                    maps={maps()}
                    showRawBytes={uiSettings().showRawBytes}
                    colorMode="utilization"
                  />
                </div>
              );
            }}
          </Show>
        </div>
      </Suspense>
    </div>
  );
}

// Fragmentation heat map - shows utilization per region with serpentine layout
interface DeviceBlockMap {
  device: Device;
  entries: BlockMapEntry[];
  totalSize: bigint;
  stats: FragStats | null;
}

// Extent cell - represents a single extent with its physical size
interface ExtentCell {
  device: Device;
  offset: bigint;
  length: bigint;
  allocated: boolean;
  type: bigint;
  // Chunk utilization (bytes used / chunk length)
  chunkUsed: bigint;
  chunkLength: bigint;
  // Pixel position calculated during layout
  pixelStart: number;
  pixelEnd: number;
}

// Color modes for the grid view
type ColorMode = "type" | "utilization";

// Unified grid view component - used for both layout and density views
function BaseGridView(props: {
  maps: DeviceBlockMap[];
  showRawBytes: boolean;
  colorMode: ColorMode;
  selectByType?: { type: BlockType; index: number } | null;
}) {
  const [hoveredExtent, setHoveredExtent] = createSignal<ExtentCell | null>(null);
  const [lockedExtent, setLockedExtent] = createSignal<ExtentCell | null>(null);
  const [containerWidth, setContainerWidth] = createSignal(800);
  const [canvasDimensions, setCanvasDimensions] = createSignal({ width: 800, height: 200 });
  const [isDrawing, setIsDrawing] = createSignal(true);
  let canvasRef: HTMLCanvasElement | undefined;
  let containerRef: HTMLDivElement | undefined;

  const ROW_HEIGHT = 12;

  // Calculate highlight SVG path for the current extent
  // Creates a single continuous outline around multi-row extents
  const extentHighlightPath = createMemo(() => {
    const ext = lockedExtent() || hoveredExtent();
    if (!ext) return null;

    const width = containerWidth();
    const rows = Math.ceil(width / 1.618 / ROW_HEIGHT);

    // Calculate all the rows this extent spans
    const startRow = Math.floor(ext.pixelStart / width);
    const endRow = Math.floor((ext.pixelEnd - 1) / width);

    if (startRow > endRow || startRow >= rows) return null;

    // Build list of rectangles for each row (we need these for the path)
    const rowRects: { x: number; y: number; w: number; h: number; row: number }[] = [];

    for (let row = startRow; row <= endRow && row < rows; row++) {
      const rowPixelStart = row * width;
      const rowPixelEnd = (row + 1) * width;

      const extStartInRow = Math.max(ext.pixelStart, rowPixelStart);
      const extEndInRow = Math.min(ext.pixelEnd, rowPixelEnd);

      const startCol = extStartInRow - rowPixelStart;
      const endCol = extEndInRow - rowPixelStart;

      let x1: number, x2: number;
      if (row % 2 === 0) {
        x1 = startCol;
        x2 = endCol;
      } else {
        x1 = width - endCol;
        x2 = width - startCol;
      }

      rowRects.push({
        x: Math.min(x1, x2),
        y: row * ROW_HEIGHT,
        w: Math.abs(x2 - x1),
        h: ROW_HEIGHT,
        row,
      });
    }

    if (rowRects.length === 0) return null;

    // Single row - simple rectangle path
    if (rowRects.length === 1) {
      const r = rowRects[0];
      return `M ${r.x} ${r.y} h ${r.w} v ${r.h} h ${-r.w} Z`;
    }

    // Multiple rows - build a path that traces the outline
    // We go clockwise: top of first row, then down right side, bottom of last row, up left side
    const pathParts: string[] = [];

    // Start at top-left of first row
    const first = rowRects[0];
    pathParts.push(`M ${first.x} ${first.y}`);

    // Trace top edge of first row
    pathParts.push(`h ${first.w}`);

    // Go down the right side, handling serpentine connections
    for (let i = 0; i < rowRects.length; i++) {
      const curr = rowRects[i];
      const next = rowRects[i + 1];

      if (next) {
        // Move down to next row, adjusting for different x positions
        const currRight = curr.x + curr.w;
        const nextRight = next.x + next.w;

        pathParts.push(`v ${curr.h}`); // down to bottom of current row
        if (nextRight !== currRight) {
          pathParts.push(`h ${nextRight - currRight}`); // horizontal adjustment
        }
      } else {
        // Last row - go down and trace bottom
        pathParts.push(`v ${curr.h}`);
      }
    }

    // Trace bottom edge of last row (right to left)
    const last = rowRects[rowRects.length - 1];
    pathParts.push(`h ${-last.w}`);

    // Go up the left side
    for (let i = rowRects.length - 1; i >= 0; i--) {
      const curr = rowRects[i];
      const prev = rowRects[i - 1];

      if (prev) {
        const currLeft = curr.x;
        const prevLeft = prev.x;

        pathParts.push(`v ${-curr.h}`); // up to top of current row
        if (prevLeft !== currLeft) {
          pathParts.push(`h ${prevLeft - currLeft}`); // horizontal adjustment
        }
      } else {
        // First row - go up to close
        pathParts.push(`v ${-curr.h}`);
      }
    }

    pathParts.push('Z');

    return pathParts.join(' ');
  });

  // Map device ID to color index
  const deviceColorMap = createMemo(() => {
    const map = new Map<bigint, number>();
    props.maps.forEach((m, idx) => {
      map.set(m.device.id, idx);
    });
    return map;
  });

  // Device color palette - generated using poline
  const devicePalette = createMemo(() => {
    const numDevices = props.maps.length;
    return generateDevicePalette(Math.max(numDevices, 2));
  });

  // Flatten all extents from all devices with pixel positions proportional to size
  const allExtents = createMemo(() => {
    // Calculate total size across all devices
    let totalSize = 0n;
    for (const map of props.maps) {
      totalSize += map.totalSize;
    }
    if (totalSize === 0n) return { extents: [] as ExtentCell[], totalPixels: 0, bytesPerPixel: 1 };

    const width = containerWidth();
    const rows = Math.ceil(width / 1.618 / ROW_HEIGHT); // Golden ratio aspect
    const totalPixels = width * rows;
    const bytesPerPixel = Number(totalSize) / totalPixels;

    const extents: ExtentCell[] = [];
    let globalByteOffset = 0;

    for (const map of props.maps) {
      for (const entry of map.entries) {
        const startByte = globalByteOffset + Number(entry.offset);
        const endByte = startByte + Number(entry.length);

        extents.push({
          device: map.device,
          offset: entry.offset,
          length: entry.length,
          allocated: entry.allocated,
          type: entry.type,
          chunkUsed: entry.chunkUsed,
          chunkLength: entry.chunkLength,
          pixelStart: Math.floor(startByte / bytesPerPixel),
          pixelEnd: Math.ceil(endByte / bytesPerPixel),
        });
      }
      globalByteOffset += Number(map.totalSize);
    }

    return { extents, totalPixels, bytesPerPixel };
  });

  // Handle selectByType - just selects the nth extent of the specified type (like a normal click)
  createEffect(() => {
    const sel = props.selectByType;
    if (!sel) return;

    const { extents } = allExtents();
    const matchingExtents = extents.filter(ext => getBlockType(ext.type, ext.allocated) === sel.type);

    if (matchingExtents.length === 0) return;

    // Wrap around if index exceeds count
    const actualIndex = sel.index % matchingExtents.length;
    const selectedExtent = matchingExtents[actualIndex];
    setLockedExtent(selectedExtent);
    setHoveredExtent(selectedExtent);
  });

  const drawGrid = () => {
    if (!canvasRef || !containerRef) return;
    const ctx = canvasRef.getContext('2d');
    if (!ctx) return;

    const width = containerRef.clientWidth;
    if (width !== containerWidth()) {
      setContainerWidth(width);
      return;
    }

    setIsDrawing(true);

    requestAnimationFrame(() => {
      const rows = Math.ceil(width / 1.618 / ROW_HEIGHT);
      const canvasWidth = width;
      const canvasHeight = rows * ROW_HEIGHT;

      canvasRef!.width = canvasWidth;
      canvasRef!.height = canvasHeight;
      setCanvasDimensions({ width: canvasWidth, height: canvasHeight });

      const { extents } = allExtents();
      const palette = devicePalette();
      const colorMap = deviceColorMap();

      // Use ImageData for fast pixel writing
      const imageData = ctx.createImageData(canvasWidth, canvasHeight);
      const data32 = new Uint32Array(imageData.data.buffer);

      // Fill with background (free space color - light gray)
      const bgColor = (255 << 24) | (240 << 16) | (240 << 8) | 240;
      data32.fill(bgColor);

      // Draw each extent proportionally
      for (const extent of extents) {
        let r: number, g: number, b: number;

        if (props.colorMode === "type") {
          // Type mode: color by data/metadata/system/free
          const typeColor = extent.allocated ? getBlockTypeColor(extent.type) : typeColors.free;
          [r, g, b] = typeColor.rgb;
        } else {
          // Utilization mode: color by device with intensity based on chunk usage
          const colorIdx = colorMap.get(extent.device.id) ?? 0;
          const [baseR, baseG, baseB] = palette[colorIdx % palette.length];

          let utilization = 0;
          if (extent.allocated && extent.chunkLength > 0n) {
            utilization = Number(extent.chunkUsed) / Number(extent.chunkLength);
          }

          if (!extent.allocated) {
            // Unallocated space - very light version of device color
            r = Math.floor(baseR + (250 - baseR) * 0.85);
            g = Math.floor(baseG + (250 - baseG) * 0.85);
            b = Math.floor(baseB + (250 - baseB) * 0.85);
          } else {
            // Allocated space - intensity based on utilization
            const lightR = Math.floor(baseR + (245 - baseR) * 0.7);
            const lightG = Math.floor(baseG + (245 - baseG) * 0.7);
            const lightB = Math.floor(baseB + (245 - baseB) * 0.7);
            r = Math.floor(lightR + (baseR - lightR) * utilization);
            g = Math.floor(lightG + (baseG - lightG) * utilization);
            b = Math.floor(lightB + (baseB - lightB) * utilization);
          }
        }

        const rgba = (255 << 24) | (b << 16) | (g << 8) | r;

        const startPixel = extent.pixelStart;
        const endPixel = extent.pixelEnd;

        // Fill pixels using serpentine pattern
        for (let pixel = startPixel; pixel < endPixel; pixel++) {
          const row = Math.floor(pixel / canvasWidth);
          const colInRow = pixel % canvasWidth;
          // Serpentine: reverse direction on odd rows
          const x = row % 2 === 0 ? colInRow : canvasWidth - 1 - colInRow;

          if (row >= rows) continue;

          // Fill ROW_HEIGHT pixels vertically
          for (let py = 0; py < ROW_HEIGHT; py++) {
            const idx = (row * ROW_HEIGHT + py) * canvasWidth + x;
            if (idx < data32.length) {
              data32[idx] = rgba;
            }
          }
        }
      }

      ctx.putImageData(imageData, 0, 0);

      // Draw device boundaries
      const { bytesPerPixel } = allExtents();
      let devicePixelStart = 0;
      // Use different boundary colors based on color mode
      ctx.strokeStyle = props.colorMode === "type" ? 'rgba(0, 0, 0, 0.4)' : 'rgba(255, 255, 255, 0.9)';
      ctx.lineWidth = 2;

      for (const map of props.maps) {
        const devicePixels = Math.ceil(Number(map.totalSize) / bytesPerPixel);
        const devicePixelEnd = devicePixelStart + devicePixels;

        // Draw boundary line at device end
        const endRow = Math.floor(devicePixelEnd / canvasWidth);
        const endCol = devicePixelEnd % canvasWidth;
        const endX = endRow % 2 === 0 ? endCol : canvasWidth - 1 - endCol;

        if (endRow < rows && endCol > 0) {
          ctx.beginPath();
          ctx.moveTo(endX, endRow * ROW_HEIGHT);
          ctx.lineTo(endX, (endRow + 1) * ROW_HEIGHT);
          ctx.stroke();
        }

        devicePixelStart = devicePixelEnd;
      }

      setIsDrawing(false);
    });
  };

  // Click outside canvas to deselect
  const handleDocumentClick = (e: MouseEvent) => {
    if (canvasRef && !canvasRef.contains(e.target as Node)) {
      setLockedExtent(null);
    }
  };

  onMount(() => {
    if (containerRef) {
      setContainerWidth(containerRef.clientWidth);
    }
    drawGrid();

    if (containerRef) {
      const resizeObserver = new ResizeObserver((entries) => {
        for (const entry of entries) {
          const newWidth = entry.contentRect.width;
          if (newWidth > 0 && newWidth !== containerWidth()) {
            setContainerWidth(newWidth);
          }
        }
      });
      resizeObserver.observe(containerRef);
    }

    document.addEventListener('click', handleDocumentClick);
    document.addEventListener('keydown', handleKeyDown);
  });

  onCleanup(() => {
    document.removeEventListener('click', handleDocumentClick);
    document.removeEventListener('keydown', handleKeyDown);
  });

  createEffect(() => {
    allExtents();
    containerWidth();
    drawGrid();
  });

  const findExtentAtPixel = (pixel: number): ExtentCell | null => {
    const { extents } = allExtents();
    // Binary search would be faster but linear is fine for now
    for (const ext of extents) {
      if (pixel >= ext.pixelStart && pixel < ext.pixelEnd) {
        return ext;
      }
    }
    return null;
  };

  // Keyboard navigation - left/right arrows to move between extents
  const handleKeyDown = (e: KeyboardEvent) => {
    const current = lockedExtent();
    if (!current) return;
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;

    e.preventDefault();
    const { extents } = allExtents();
    const currentIndex = extents.findIndex(ext =>
      ext.device.id === current.device.id &&
      ext.offset === current.offset
    );

    if (currentIndex === -1) return;

    let newIndex: number;
    if (e.key === 'ArrowLeft') {
      newIndex = currentIndex > 0 ? currentIndex - 1 : extents.length - 1;
    } else {
      newIndex = currentIndex < extents.length - 1 ? currentIndex + 1 : 0;
    }

    const newExtent = extents[newIndex];
    setLockedExtent(newExtent);
    setHoveredExtent(newExtent);
  };

  const handleMouseMove = (e: MouseEvent) => {
    if (lockedExtent()) return;
    if (!canvasRef) return;

    const rect = canvasRef.getBoundingClientRect();
    const x = Math.floor(e.clientX - rect.left);
    const row = Math.floor((e.clientY - rect.top) / ROW_HEIGHT);
    const width = containerWidth();

    // Convert to pixel index (accounting for serpentine)
    const colInRow = row % 2 === 0 ? x : width - 1 - x;
    const pixel = row * width + colInRow;

    const extent = findExtentAtPixel(pixel);
    setHoveredExtent(extent);
  };

  const handleMouseLeave = () => {
    if (lockedExtent()) return;
    setHoveredExtent(null);
  };

  const handleClick = (e: MouseEvent) => {
    if (!canvasRef) return;

    const rect = canvasRef.getBoundingClientRect();
    const x = Math.floor(e.clientX - rect.left);
    const row = Math.floor((e.clientY - rect.top) / ROW_HEIGHT);
    const width = containerWidth();

    const colInRow = row % 2 === 0 ? x : width - 1 - x;
    const pixel = row * width + colInRow;

    const extent = findExtentAtPixel(pixel);

    if (lockedExtent() === extent) {
      setLockedExtent(null);
    } else {
      setLockedExtent(extent);
      setHoveredExtent(extent);
    }
  };

  const displayExtent = () => lockedExtent() || hoveredExtent();

  const getTypeName = (type: bigint, allocated: boolean) => {
    if (!allocated) return "free";
    const t = Number(type);
    if (t & 1) return "data";
    if (t & 4) return "metadata";
    if (t & 2) return "system";
    return "unknown";
  };

  return (
    <div ref={containerRef} class="w-full">
      {/* Tooltip */}
      <div class="mb-1 text-xs text-text-secondary h-4 flex items-center">
        <Show when={displayExtent()}>
          {(ext) => {
            // Use accessors for reactivity - ext() is called each time to get latest value
            const used = () => ext().chunkUsed ?? 0n;
            const total = () => ext().chunkLength ?? 0n;
            const utilPct = () => {
              const e = ext();
              const t = e.chunkLength ?? 0n;
              if (e.allocated && t > 0n) {
                return ((Number(e.chunkUsed ?? 0n) / Number(t)) * 100).toFixed(1);
              }
              return null;
            };
            return (
              <>
                <Show when={lockedExtent()}>
                  <span class="text-text-muted mr-2">[locked]</span>
                </Show>
                <span class="font-medium">{ext().device.path || `Device ${ext().device.id}`}</span>
                <span class="mx-2">|</span>
                <span class="font-mono">
                  {formatBytes(ext().offset, props.showRawBytes)}
                </span>
                <span class="mx-2">|</span>
                <span>size: {formatBytes(ext().length, props.showRawBytes)}</span>
                <span class="mx-2">|</span>
                <Show when={props.colorMode === "type"} fallback={
                  <Show when={ext().allocated} fallback={<span>unallocated</span>}>
                    <span>chunk: {formatBytes(used(), props.showRawBytes)} / {formatBytes(total(), props.showRawBytes)}</span>
                    <Show when={utilPct()}>
                      {(pct) => <span class="ml-1">({pct()}% used)</span>}
                    </Show>
                  </Show>
                }>
                  <span>{getTypeName(ext().type, ext().allocated)}</span>
                </Show>
              </>
            );
          }}
        </Show>
      </div>

      <div class="relative">
        <Show when={isDrawing()}>
          <div class="absolute inset-0 flex items-center justify-center bg-bg-surface/80 z-10">
            <span class="text-xs text-text-muted">Drawing...</span>
          </div>
        </Show>
        <canvas
          ref={canvasRef}
          class="cursor-pointer"
          style={{ "image-rendering": "pixelated" }}
          onMouseMove={handleMouseMove}
          onMouseLeave={handleMouseLeave}
          onClick={handleClick}
        />
        {/* Highlight overlay for hovered/locked extent - uses SVG for smart outline */}
        <Show when={extentHighlightPath()}>
          {(path) => (
            <svg
              class="absolute top-0 left-0 pointer-events-none"
              width={canvasDimensions().width}
              height={canvasDimensions().height}
              style={{ overflow: "visible" }}
            >
              <path
                d={path()}
                fill="none"
                stroke={lockedExtent() ? "rgba(250, 204, 21, 0.9)" : props.colorMode === "type" ? "rgba(0, 0, 0, 0.7)" : "rgba(255, 255, 255, 0.9)"}
                stroke-width="2"
                filter={lockedExtent() ? "drop-shadow(0 0 4px rgba(250, 204, 21, 0.7))" : props.colorMode === "type" ? "drop-shadow(0 0 2px rgba(255, 255, 255, 0.8))" : "drop-shadow(0 0 3px rgba(0, 0, 0, 0.6))"}
              />
            </svg>
          )}
        </Show>
      </div>
    </div>
  );
}

// REMOVED: LayoutGridView - now using BaseGridView with colorMode="type"
// REMOVED: BlockMapView - deprecated

function getBlockTypeName(type: bigint): string {
  const t = Number(type);
  if (t & 1) return "data";
  if (t & 4) return "metadata";
  if (t & 2) return "system";
  return "unknown";
}

function getProfileInfo(profile: bigint): { name: string; efficiency: number } {
  const p = Number(profile);
  // Profile bit flags from btrfs
  if (p & (1 << 10)) return { name: "raid1c4", efficiency: 25 };
  if (p & (1 << 9)) return { name: "raid1c3", efficiency: 33 };
  if (p & (1 << 8)) return { name: "raid6", efficiency: 67 };
  if (p & (1 << 7)) return { name: "raid5", efficiency: 75 };
  if (p & (1 << 6)) return { name: "raid10", efficiency: 50 };
  if (p & (1 << 5)) return { name: "dup", efficiency: 50 };
  if (p & (1 << 4)) return { name: "raid1", efficiency: 50 };
  if (p & (1 << 3)) return { name: "raid0", efficiency: 100 };
  return { name: "single", efficiency: 100 };
}
