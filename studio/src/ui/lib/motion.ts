export const DURATION = {
  quick: 100,
  base: 150,
  panel: 200,
  layout: 200,
} as const;

export type Easing = (t: number) => number;

export function cubicBezier(
  x1: number,
  y1: number,
  x2: number,
  y2: number,
): Easing {
  const cx = 3 * x1;
  const bx = 3 * (x2 - x1) - cx;
  const ax = 1 - cx - bx;
  const cy = 3 * y1;
  const by = 3 * (y2 - y1) - cy;
  const ay = 1 - cy - by;
  const sampleX = (t: number) => ((ax * t + bx) * t + cx) * t;
  const sampleY = (t: number) => ((ay * t + by) * t + cy) * t;
  const slopeX = (t: number) => (3 * ax * t + 2 * bx) * t + cx;

  function solve(x: number): number {
    let t = x;
    for (let i = 0; i < 8; i++) {
      const error = sampleX(t) - x;
      if (Math.abs(error) < 1e-6) return t;
      const slope = slopeX(t);
      if (Math.abs(slope) < 1e-6) break;
      t -= error / slope;
    }
    let low = 0;
    let high = 1;
    t = x;
    while (low < high) {
      const value = sampleX(t);
      if (Math.abs(value - x) < 1e-6) return t;
      if (x > value) low = t;
      else high = t;
      t = (high - low) / 2 + low;
      if (high - low < 1e-7) break;
    }
    return t;
  }

  return (x: number) => {
    if (x <= 0) return 0;
    if (x >= 1) return 1;
    return sampleY(solve(x));
  };
}

export const EASE_OUT: Easing = cubicBezier(0.23, 1, 0.32, 1);
export const EASE_IN_OUT: Easing = cubicBezier(0.77, 0, 0.175, 1);
export const EASE_DRAWER: Easing = cubicBezier(0.32, 0.72, 0, 1);
export const EASE_ENTER: Easing = EASE_OUT;
export const EASE_EXIT: Easing = EASE_OUT;

export function reducedMotion(): boolean {
  return (
    typeof window !== "undefined" &&
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches
  );
}

let quietUntil = 0;
let quietTimer: ReturnType<typeof setTimeout> | undefined;

export const QUIET_ENTRANCE_MS = 1000;

function now(): number {
  return typeof performance !== "undefined" ? performance.now() : 0;
}

function quietEntrances(): void {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  root.dataset.quiet = "";
  clearTimeout(quietTimer);
  quietTimer = setTimeout(() => {
    delete root.dataset.quiet;
  }, QUIET_ENTRANCE_MS);
}

export function quiet(windowMs = 250): void {
  quietUntil = now() + windowMs;
  quietEntrances();
}

export function isQuiet(): boolean {
  return now() < quietUntil;
}

function settle(duration: number): number {
  return isQuiet() ? 0 : duration;
}

interface TransitionConfig {
  duration: number;
  easing: Easing;
  css: (t: number, u: number) => string;
}

export function fade(
  _node: Element,
  options: { duration?: number; exit?: boolean; blur?: number } = {},
): TransitionConfig {
  const blur = reducedMotion() ? 0 : (options.blur ?? 0);
  return {
    duration: settle(options.duration ?? DURATION.base),
    easing: options.exit ? EASE_EXIT : EASE_ENTER,
    css: (t, u) =>
      blur > 0 ? `opacity: ${t}; filter: blur(${u * blur}px)` : `opacity: ${t}`,
  };
}

export function rise(
  _node: Element,
  options: {
    duration?: number;
    exit?: boolean;
    from?: "above" | "below" | "left";
    distance?: number;
  } = {},
): TransitionConfig {
  const still = reducedMotion();
  const distance = options.distance ?? 4;
  const axis = options.from === "left" ? "X" : "Y";
  const sign = options.from === "below" ? 1 : -1;
  return {
    duration: settle(
      options.duration ?? (options.exit ? DURATION.quick : DURATION.base),
    ),
    easing: options.exit ? EASE_EXIT : EASE_ENTER,
    css: (t, u) =>
      still
        ? `opacity: ${t}`
        : `opacity: ${t}; transform: translate${axis}(${sign * u * distance}px) scale(${0.97 + 0.03 * t})`,
  };
}

export function slide(
  _node: Element,
  options: { duration?: number; exit?: boolean; distance?: number } = {},
): TransitionConfig {
  const still = reducedMotion();
  const distance = options.distance ?? 12;
  return {
    duration: settle(
      options.duration ?? (options.exit ? DURATION.base : DURATION.panel),
    ),
    easing: options.exit ? EASE_EXIT : EASE_DRAWER,
    css: (t, u) =>
      still
        ? `opacity: ${t}`
        : `opacity: ${t}; transform: translateX(${u * distance}px)`,
  };
}

export function expand(
  node: Element,
  options: { duration?: number; exit?: boolean } = {},
): TransitionConfig {
  const still = reducedMotion();
  const height = (node as HTMLElement).offsetHeight;
  return {
    duration: settle(
      options.duration ?? (options.exit ? DURATION.quick : DURATION.layout),
    ),
    easing: options.exit ? EASE_EXIT : EASE_ENTER,
    css: (t, u) =>
      still
        ? `opacity: ${t}`
        : `overflow: hidden; height: ${t * height}px; opacity: ${Math.min(1, t * 1.4)}; transform: translateY(${-u * 4}px)`,
  };
}
