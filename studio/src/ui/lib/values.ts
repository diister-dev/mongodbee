import { decodeTime } from "@diister/mongodbee/ids";

export const ULID_LENGTH = 26;
export const ULID_TIME_LENGTH = 10;

const ULID_SHAPE = /^[0-9A-HJKMNP-TV-Z]{26}$/i;
const OBJECT_ID_SHAPE = /^[0-9a-f]{24}$/i;
const TYPED_ID_SHAPE = /^([A-Za-z][A-Za-z0-9_.-]*):([A-Za-z0-9]+)$/;
const EMAIL_SHAPE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const URL_SHAPE = /^https?:\/\/[^\s]+$/i;
const MIGRATION_ID_SHAPE = /^(\d{4}_\d{2}_\d{2})_(.+)$/;

export interface TypedId {
  prefix: string;
  id: string;
}

export interface Hue {
  name: string;
  dot: string;
  text: string;
  tint: string;
}

export const PALETTE: readonly Hue[] = [
  { name: "cyan", dot: "#2fb5e6", text: "#0d5f80", tint: "#e3f5fc" },
  { name: "magenta", dot: "#e24bd2", text: "#8e1f84", tint: "#fbe7f8" },
  { name: "lime", dot: "#7fd13b", text: "#3d6d14", tint: "#eff9e3" },
  { name: "honey", dot: "#f2b81f", text: "#7d5a00", tint: "#fff4d1" },
  { name: "flame", dot: "#ff5a1f", text: "#9c2e05", tint: "#ffeadf" },
  { name: "leaf", dot: "#1f8a4c", text: "#0f5530", tint: "#e3f2e8" },
  { name: "violet", dot: "#7b5cf0", text: "#44309a", tint: "#eeeafd" },
  { name: "slate", dot: "#6c7569", text: "#434a41", tint: "#eeefe9" },
];

export function isUlid(value: unknown): value is string {
  return typeof value === "string" && ULID_SHAPE.test(value);
}

export function ulidTime(value: string): number | null {
  if (!isUlid(value)) return null;
  try {
    return decodeTime(value);
  } catch {
    return null;
  }
}

export function splitUlid(value: string): [string, string] {
  return [value.slice(0, ULID_TIME_LENGTH), value.slice(ULID_TIME_LENGTH)];
}

export function isObjectIdHex(value: unknown): value is string {
  return typeof value === "string" && OBJECT_ID_SHAPE.test(value);
}

export function objectIdTime(hex: string): number | null {
  if (!isObjectIdHex(hex)) return null;
  return Number.parseInt(hex.slice(0, 8), 16) * 1000;
}

export function splitObjectId(hex: string): [string, string, string] {
  return [hex.slice(0, 8), hex.slice(8, 18), hex.slice(18)];
}

export function parseTypedId(value: unknown): TypedId | null {
  if (typeof value !== "string") return null;
  const match = TYPED_ID_SHAPE.exec(value);
  if (!match) return null;
  return { prefix: match[1], id: match[2] };
}

export function isEmail(value: unknown): value is string {
  return typeof value === "string" && EMAIL_SHAPE.test(value);
}

export function isUrl(value: unknown): value is string {
  return typeof value === "string" && URL_SHAPE.test(value);
}

export function splitMigrationId(value: string): {
  date?: string;
  name: string;
} {
  const match = MIGRATION_ID_SHAPE.exec(value);
  if (!match) return { name: value };
  return { date: match[1], name: match[2] };
}

export function hashString(value: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

export function hueFor(key: string): Hue {
  return PALETTE[hashString(key) % PALETTE.length];
}

export function hueForOption(
  value: string,
  options: readonly unknown[] | undefined,
  field = "",
): Hue {
  const index = options ? options.indexOf(value) : -1;
  if (index >= 0) return PALETTE[index % PALETTE.length];
  return hueFor(`${field}:${value}`);
}

export function middleTruncate(value: string, max: number): string {
  if (max < 3 || value.length <= max) return value;
  const keep = max - 1;
  const head = Math.ceil(keep / 2);
  const tail = Math.floor(keep / 2);
  return `${value.slice(0, head)}…${value.slice(value.length - tail)}`;
}

const UNITS: ReadonlyArray<[Intl.RelativeTimeFormatUnit, number]> = [
  ["year", 365 * 24 * 3600 * 1000],
  ["month", 30 * 24 * 3600 * 1000],
  ["week", 7 * 24 * 3600 * 1000],
  ["day", 24 * 3600 * 1000],
  ["hour", 3600 * 1000],
  ["minute", 60 * 1000],
  ["second", 1000],
];

const RELATIVE = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

export function relativeTime(time: number, now: number = Date.now()): string {
  const delta = time - now;
  const size = Math.abs(delta);
  for (const [unit, ms] of UNITS) {
    if (size >= ms || unit === "second") {
      return RELATIVE.format(Math.round(delta / ms), unit);
    }
  }
  return RELATIVE.format(0, "second");
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}

export function formatDay(time: number): string {
  const date = new Date(time);
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(
    date.getUTCDate(),
  )}`;
}

export function formatCompactDate(time: number): string {
  const date = new Date(time);
  const midnight =
    date.getUTCHours() === 0 &&
    date.getUTCMinutes() === 0 &&
    date.getUTCSeconds() === 0;
  if (midnight) return formatDay(time);
  return `${formatDay(time)} ${pad(date.getUTCHours())}:${pad(
    date.getUTCMinutes(),
  )}`;
}

export function formatFullDate(time: number): string {
  const date = new Date(time);
  return `${formatDay(time)} ${pad(date.getUTCHours())}:${pad(
    date.getUTCMinutes(),
  )}:${pad(date.getUTCSeconds())} UTC`;
}

export function formatNumberValue(value: number): string {
  if (!Number.isFinite(value)) return String(value);
  if (Number.isInteger(value)) return String(value);
  return String(Number.parseFloat(value.toFixed(6)));
}

const COUNT = new Intl.NumberFormat("en");

export function formatCount(value: number): string {
  return COUNT.format(value);
}

export type FieldFamily =
  | "identity"
  | "reference"
  | "text"
  | "number"
  | "date"
  | "boolean"
  | "choice"
  | "list"
  | "object"
  | "other";

export const FIELD_FAMILY_LABEL: Record<FieldFamily, string> = {
  identity: "Document",
  reference: "References",
  text: "Text",
  number: "Numbers",
  date: "Dates",
  boolean: "Booleans",
  choice: "Choices",
  list: "Lists",
  object: "Objects",
  other: "Other",
};

export function fieldFamily(
  name: string,
  node?: { kind: string; ref?: string } | null,
): FieldFamily {
  if (name === "_id" || name === "_type" || name === "_scope")
    return "identity";
  if (!node) return "other";
  if (node.ref) return "reference";
  switch (node.kind) {
    case "string":
      return "text";
    case "number":
    case "bigint":
      return "number";
    case "date":
      return "date";
    case "boolean":
      return "boolean";
    case "picklist":
    case "enum":
    case "literal":
      return "choice";
    case "array":
    case "tuple":
    case "set":
      return "list";
    case "object":
    case "loose_object":
    case "strict_object":
    case "object_with_rest":
    case "record":
    case "map":
      return "object";
    default:
      return "other";
  }
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return "";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value >= 100 ? Math.round(value) : value.toFixed(1).replace(/\.0$/, "")} ${units[unit]}`;
}

export type DurationPart = [value: string, unit: string];

export function durationParts(ms: number): DurationPart[] {
  if (!Number.isFinite(ms) || ms < 0) return [["0", "ms"]];
  if (ms < 1000) return [[String(Math.round(ms)), "ms"]];
  if (ms < 10_000) return [[(Math.floor(ms / 100) / 10).toFixed(1), "s"]];
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return [[String(seconds), "s"]];
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    const rest = seconds % 60;
    return rest === 0
      ? [[String(minutes), "min"]]
      : [
          [String(minutes), "min"],
          [String(rest), "s"],
        ];
  }
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest === 0
    ? [[String(hours), "h"]]
    : [
        [String(hours), "h"],
        [String(rest), "min"],
      ];
}

export function idTime(value: unknown): number | null {
  if (value && typeof value === "object" && "$oid" in value) {
    return objectIdTime(String((value as { $oid: unknown }).$oid));
  }
  if (typeof value !== "string") return null;
  const id = parseTypedId(value)?.id ?? value;
  return ulidTime(id) ?? objectIdTime(id);
}
