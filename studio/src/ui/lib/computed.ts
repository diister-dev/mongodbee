export const COMPUTED_ROOT = "_computed";
export const COMPUTED_REVISION = "_rev";
export const COMPUTED_PREFIX = `${COMPUTED_ROOT}.`;

export interface FieldNode {
  kind?: string;
  entries?: Record<string, FieldNode>;
  system?: string;
  computed?: string;
  [key: string]: unknown;
}

export type ComputedLiteral = string | number | boolean | null;

export interface ComputedSourceInfo {
  collection: string;
  type?: string;
  scoped: boolean;
}

export interface ComputedFieldInfo {
  subject: string;
  name: string;
  description: string;
  source: ComputedSourceInfo;
  by: string;
  where: Record<string, ComputedLiteral | ComputedLiteral[]>;
  sameScope: boolean;
  through?: {
    source: ComputedSourceInfo;
    via: string;
    where: Record<string, ComputedLiteral | ComputedLiteral[]>;
  };
  aggregate:
    | { kind: "collect"; path: string; distinct: boolean; maxEntries?: number }
    | { kind: "count" };
  filled?: number;
  sampled?: number;
  pending?: number;
}

export interface ComputedReport {
  subjects: string[];
  fields: ComputedFieldInfo[];
  topologyError?: string;
  stats?: {
    sampled: number;
    revisions: Array<{ revision: number | null; count: number }>;
  };
  pending?: { count: number; oldestAgeMs: number | null };
}

export interface ComputedDriftReport {
  subject: string;
  checked: number;
  complete: boolean;
  drifted: Record<string, number>;
  examples: Array<{
    id: string;
    open: string;
    field: string;
    stored: unknown;
    truth: unknown;
    missing: boolean;
  }>;
}

export interface SourceTarget {
  view: "collection";
  collection: string;
  tab: "data";
  type: string;
  scope: string;
  where: string[];
}

export function isComputedColumn(column: string): boolean {
  return column.startsWith(COMPUTED_PREFIX);
}

export function computedName(column: string): string {
  return column.slice(COMPUTED_PREFIX.length);
}

export function withComputedColumns(
  fields: Record<string, FieldNode>,
): Record<string, FieldNode> {
  const entries = fields[COMPUTED_ROOT]?.entries;
  if (!entries) return fields;
  const result: Record<string, FieldNode> = {};
  for (const [name, node] of Object.entries(fields)) {
    if (name !== COMPUTED_ROOT) result[name] = node;
  }
  for (const [name, node] of Object.entries(entries)) {
    if (name !== COMPUTED_REVISION) result[`${COMPUTED_PREFIX}${name}`] = node;
  }
  return result;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function valueAt(
  item: Record<string, unknown>,
  column: string,
): unknown {
  if (!isComputedColumn(column)) return item[column];
  const root = item[COMPUTED_ROOT];
  return isRecord(root) ? root[computedName(column)] : undefined;
}

export function computedOf(
  document: Record<string, unknown> | null | undefined,
): Record<string, unknown> | undefined {
  const root = document?.[COMPUTED_ROOT];
  return isRecord(root) ? root : undefined;
}

export function withoutComputed(
  document: Record<string, unknown>,
): Record<string, unknown> {
  if (!(COMPUTED_ROOT in document)) return document;
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(document)) {
    if (key !== COMPUTED_ROOT) result[key] = value;
  }
  return result;
}

export function plainId(id: unknown): string | undefined {
  if (typeof id === "string") return id;
  if (isRecord(id) && typeof id.$oid === "string") return id.$oid;
  return undefined;
}

function literalParam(value: ComputedLiteral): string {
  return value === null ? "null" : String(value);
}

export function whereParams(
  where: Record<string, ComputedLiteral | ComputedLiteral[]>,
): string[] {
  return Object.entries(where).map(([path, value]) =>
    Array.isArray(value)
      ? `${path}:in:${JSON.stringify(value)}`
      : `${path}:eq:${literalParam(value)}`,
  );
}

export function sourceTarget(
  info: ComputedFieldInfo,
  document: Record<string, unknown>,
): SourceTarget | undefined {
  const id = plainId(document._id);
  if (id === undefined) return undefined;
  const scope =
    info.source.scoped && typeof document._scope === "string"
      ? document._scope
      : "";
  return {
    view: "collection",
    collection: info.source.collection,
    tab: "data",
    type: info.source.type ?? "",
    scope,
    where: [`${info.by}:eq:${id}`, ...whereParams(info.where)],
  };
}

export function sourceLabel(source: ComputedSourceInfo): string {
  return source.type ?? source.collection;
}
