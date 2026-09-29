export interface AnchorRect {
  left: number;
  top: number;
  bottom: number;
  width: number;
}

export interface Viewport {
  width: number;
  height: number;
}

export interface AnchorPlacement {
  left: number;
  top: number;
  bottom: number;
  above: boolean;
  minWidth: number;
}

export function placeUnder(
  rect: AnchorRect,
  viewport: Viewport,
  panelHeight: number,
  options: { minWidth?: number; gap?: number; margin?: number } = {},
): AnchorPlacement {
  const gap = options.gap ?? 6;
  const margin = options.margin ?? 8;
  const minWidth = Math.min(
    Math.max(rect.width, options.minWidth ?? 160),
    viewport.width - margin * 2,
  );
  const roomBelow = viewport.height - rect.bottom - gap - margin;
  const roomAbove = rect.top - gap - margin;
  const above = roomBelow < panelHeight && roomAbove > roomBelow;
  const left = Math.max(
    margin,
    Math.min(rect.left, viewport.width - minWidth - margin),
  );
  return {
    left,
    top: rect.bottom + gap,
    bottom: viewport.height - rect.top + gap,
    above,
    minWidth,
  };
}
