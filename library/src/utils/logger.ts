/**
 * Lightweight namespaced debug logger for MongoDBee internals.
 *
 * Activation:
 * - MONGODBEE_DEBUG: comma-separated namespaces, or "*" for all.
 *   Supports trailing wildcards (e.g. "queue,multi-*") and leading "-"
 *   to exclude (e.g. "*,-queue").
 *   Examples:
 *     MONGODBEE_DEBUG=*
 *     MONGODBEE_DEBUG=queue,multi-collection
 *     MONGODBEE_DEBUG=*,-queue
 *
 * - MONGODBEE_LOG_LEVEL: trace | debug | info | warn | error (default: info).
 *   When MONGODBEE_DEBUG is unset, only warn/error are emitted (regardless
 *   of level) so the library stays quiet by default.
 *
 * Output goes to stderr with a [+Nms] delta since the previous log on the
 * same namespace, which makes hangs easy to spot.
 */

import process from "node:process";

type Level = "trace" | "debug" | "info" | "warn" | "error";

const LEVEL_ORDER: Record<Level, number> = {
  trace: 10,
  debug: 20,
  info: 30,
  warn: 40,
  error: 50,
};

function readEnv(name: string): string | undefined {
  try {
    // `node:process` is the one env surface every target runtime implements —
    // Deno populates `process.env` too, so this needs no per-runtime branch.
    const value = process.env[name];
    if (typeof value === "string" && value !== "") return value;
  } catch {
    // Deno without --allow-env throws on access rather than returning
    // undefined; a logger must never be the reason a program dies.
  }
  return undefined;
}

const debugSpec = readEnv("MONGODBEE_DEBUG");
// If MONGODBEE_DEBUG is set, default level is "debug" (the whole point is to see
// debug logs); otherwise default to "info" so warn/error still surface.
const defaultLevel: Level = debugSpec ? "debug" : "info";
const levelSpec = (
  readEnv("MONGODBEE_LOG_LEVEL") || defaultLevel
).toLowerCase() as Level;
const minLevel = LEVEL_ORDER[levelSpec] ?? LEVEL_ORDER[defaultLevel];

const includePatterns: string[] = [];
const excludePatterns: string[] = [];
if (debugSpec) {
  for (const raw of debugSpec.split(",")) {
    const part = raw.trim();
    if (!part) continue;
    if (part.startsWith("-")) {
      excludePatterns.push(part.slice(1));
    } else {
      includePatterns.push(part);
    }
  }
}

function matchPattern(pattern: string, namespace: string): boolean {
  if (pattern === "*") return true;
  if (pattern.endsWith("*")) {
    return namespace.startsWith(pattern.slice(0, -1));
  }
  return pattern === namespace;
}

function namespaceEnabled(namespace: string): boolean {
  if (includePatterns.length === 0) return false;
  for (const p of excludePatterns) {
    if (matchPattern(p, namespace)) return false;
  }
  for (const p of includePatterns) {
    if (matchPattern(p, namespace)) return true;
  }
  return false;
}

const lastTimestamps = new Map<string, number>();

function formatDelta(namespace: string, now: number): string {
  const prev = lastTimestamps.get(namespace);
  lastTimestamps.set(namespace, now);
  if (prev === undefined) return "+0ms";
  const delta = now - prev;
  if (delta < 1000) return `+${delta}ms`;
  return `+${(delta / 1000).toFixed(2)}s`;
}

function formatArg(arg: unknown): string {
  if (typeof arg === "string") return arg;
  if (arg instanceof Error) return `${arg.name}: ${arg.message}`;
  try {
    return JSON.stringify(arg);
  } catch {
    return String(arg);
  }
}

function emit(level: Level, namespace: string, args: unknown[]): void {
  const now = Date.now();
  const delta = formatDelta(namespace, now);
  const message = args.map(formatArg).join(" ");
  const line = `[mongodbee:${namespace}] ${level.toUpperCase()} ${message} (${delta})`;
  console.log(line);
}

export interface Logger {
  trace(...args: unknown[]): void;
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
  error(...args: unknown[]): void;
  /** True if any of trace/debug/info would be emitted — guard expensive payloads. */
  enabled: boolean;
}

/**
 * Create a logger bound to a namespace.
 *
 * Warn and error always emit (gated only by MONGODBEE_LOG_LEVEL).
 * Trace/debug/info require both the namespace to be enabled via
 * MONGODBEE_DEBUG and the level to be >= MONGODBEE_LOG_LEVEL.
 */
export function createLogger(namespace: string): Logger {
  const nsEnabled = namespaceEnabled(namespace);

  const make = (level: Level) => {
    const levelNum = LEVEL_ORDER[level];
    // warn/error always allowed if level permits — debug flag not required
    const alwaysOn = level === "warn" || level === "error";
    return (...args: unknown[]) => {
      if (levelNum < minLevel) return;
      if (!alwaysOn && !nsEnabled) return;
      emit(level, namespace, args);
    };
  };

  return {
    trace: make("trace"),
    debug: make("debug"),
    info: make("info"),
    warn: make("warn"),
    error: make("error"),
    enabled: nsEnabled,
  };
}
