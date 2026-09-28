import type * as m from "mongodb";
import { fieldsOf } from "../../type-definition.ts";
import {
  type CatalogEntry,
  findCatalogEntry,
  isTyped,
  META_TYPES,
} from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import {
  clampInteger,
  encodeId,
  hasOperatorKeys,
  parseExtendedJson,
  StudioHttpError,
} from "../http.ts";
import { nodeAtPath } from "../field-paths.ts";
import { schemaToNode } from "../schema-tree.ts";
import {
  countScopes,
  QUERY_TIME_LIMIT_MS,
  type ScopeCount,
} from "./overview.ts";
import { boundedCount, type Count } from "./plan.ts";

export const DEFAULT_PAGE_SIZE = 50;
export const MAX_PAGE_SIZE = 200;
export const MAX_CONDITIONS = 20;
export const MAX_OFFSET = 100_000;
export const RESULT_COUNT_LIMIT = 100_000;

const FIELD_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
export const FIELD_PATH = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z0-9_]+)*$/;

export const CONDITION_OPERATORS = [
  "eq",
  "ne",
  "gt",
  "gte",
  "lt",
  "lte",
  "contains",
  "starts",
  "exists",
  "missing",
] as const;

export type ConditionOperator = (typeof CONDITION_OPERATORS)[number];

export interface Condition {
  field: string;
  op: ConditionOperator;
  value: string;
}

export type SortDirection = "asc" | "desc";

export interface DocumentsQuery {
  type?: string;
  scope?: string;
  after?: string;
  before?: string;
  limit?: number;
  filters?: Record<string, string>;
  conditions?: Condition[];
  sort?: string;
  direction?: SortDirection;
  offset?: number;
  count?: boolean;
}

export interface DocumentsPage {
  collection: string;
  items: Record<string, unknown>[];
  limit: number;
  hasNext: boolean;
  hasPrevious: boolean;
  firstId?: string;
  lastId?: string;
  sort: {
    field: string;
    direction: SortDirection;
    paging: "cursor" | "offset";
  };
  offset?: number;
  total?: Count;
}

export async function requireEntry(
  context: StudioContext,
  name: string,
): Promise<CatalogEntry> {
  const entry = await findCatalogEntry(context, name);
  if (!entry) {
    throw new StudioHttpError(404, `Unknown collection "${name}"`);
  }
  return entry;
}

function fieldKind(
  entry: CatalogEntry,
  type: string | undefined,
  field: string,
) {
  const sources =
    type && entry.types[type]
      ? [entry.types[type]]
      : Object.values(entry.types);
  const [head] = field.split(".");
  for (const source of sources) {
    const schema = (fieldsOf(source) as Record<string, unknown>)[head];
    if (!schema) continue;
    const node = nodeAtPath({ [head]: schemaToNode(schema) }, field);
    if (node) return node.kind;
  }
  return undefined;
}

export function coerceFilterValue(
  raw: string,
  kind: string | undefined,
): unknown {
  if (raw === "null") return null;
  if (kind === "number") {
    const value = Number(raw);
    if (Number.isNaN(value)) {
      throw new StudioHttpError(400, `"${raw}" is not a number`);
    }
    return value;
  }
  if (kind === "boolean") {
    if (raw === "true") return true;
    if (raw === "false") return false;
    throw new StudioHttpError(400, `"${raw}" is not a boolean`);
  }
  if (kind === "date") {
    const value = new Date(raw);
    if (Number.isNaN(value.getTime())) {
      throw new StudioHttpError(400, `"${raw}" is not a date`);
    }
    return value;
  }
  const trimmed = raw.trim();
  if (
    kind !== "string" &&
    (trimmed.startsWith("{") ||
      trimmed.startsWith("[") ||
      trimmed.startsWith('"'))
  ) {
    const parsed = parseExtendedJson(trimmed);
    if (hasOperatorKeys(parsed)) {
      throw new StudioHttpError(400, "Filters only support equality values");
    }
    return parsed;
  }
  return raw;
}

export function parseCondition(raw: string): Condition {
  const first = raw.indexOf(":");
  const second = first < 0 ? -1 : raw.indexOf(":", first + 1);
  if (first < 0 || second < 0) {
    throw new StudioHttpError(
      400,
      `Conditions are written field:operator:value, got "${raw}"`,
    );
  }
  const field = raw.slice(0, first);
  const op = raw.slice(first + 1, second) as ConditionOperator;
  if (!FIELD_PATH.test(field)) {
    throw new StudioHttpError(400, `Invalid field path "${field}"`);
  }
  if (!CONDITION_OPERATORS.includes(op)) {
    throw new StudioHttpError(400, `Unknown operator "${op}"`);
  }
  return { field, op, value: raw.slice(second + 1) };
}

export function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function comparable(raw: string, kind: string | undefined): unknown {
  if (kind === "number" || kind === "date") {
    return coerceFilterValue(raw, kind);
  }
  const trimmed = raw.trim();
  if (kind === undefined && trimmed !== "" && !Number.isNaN(Number(trimmed))) {
    return Number(trimmed);
  }
  return raw;
}

export function conditionClause(
  condition: Condition,
  kind: string | undefined,
): m.Filter<m.Document> {
  const { field, op, value } = condition;
  switch (op) {
    case "exists":
      return { [field]: { $exists: true, $ne: null } };
    case "missing":
      return { $or: [{ [field]: { $exists: false } }, { [field]: null }] };
    case "contains":
      return { [field]: { $regex: escapeRegex(value), $options: "i" } };
    case "starts":
      return { [field]: { $regex: `^${escapeRegex(value)}`, $options: "i" } };
    case "eq":
      return { [field]: coerceFilterValue(value, kind) };
    case "ne":
      return { [field]: { $ne: coerceFilterValue(value, kind) } };
    default:
      return { [field]: { [`$${op}`]: comparable(value, kind) } };
  }
}

function parseIdParam(raw: string): unknown {
  const parsed = parseExtendedJson(raw);
  if (hasOperatorKeys(parsed)) {
    throw new StudioHttpError(400, "Invalid document id");
  }
  return parsed;
}

function parseScope(raw: string): unknown {
  const trimmed = raw.trim();
  if (trimmed.startsWith("{") || trimmed.startsWith('"')) {
    return parseIdParam(trimmed);
  }
  return raw;
}

export function buildDocumentFilter(
  entry: CatalogEntry,
  query: DocumentsQuery,
): m.Filter<m.Document> {
  const clauses: m.Filter<m.Document>[] = [];
  if (isTyped(entry)) {
    if (query.type) {
      clauses.push({ _type: query.type });
    } else if (entry.kind !== "scopedMultiCollection") {
      clauses.push({ _type: { $nin: [...META_TYPES] } });
    }
  }
  if (query.scope !== undefined && query.scope !== "") {
    if (entry.kind !== "scopedMultiCollection") {
      throw new StudioHttpError(
        400,
        `"${entry.name}" is not a scoped multi-collection`,
      );
    }
    clauses.push({ _scope: parseScope(query.scope) });
  }
  for (const [field, raw] of Object.entries(query.filters ?? {})) {
    if (!FIELD_NAME.test(field)) {
      throw new StudioHttpError(
        400,
        `Filters apply to top-level fields only, got "${field}"`,
      );
    }
    clauses.push({
      [field]: coerceFilterValue(raw, fieldKind(entry, query.type, field)),
    });
  }
  const conditions = query.conditions ?? [];
  if (conditions.length > MAX_CONDITIONS) {
    throw new StudioHttpError(
      400,
      `At most ${MAX_CONDITIONS} conditions are allowed`,
    );
  }
  for (const condition of conditions) {
    const kind = fieldKind(entry, query.type, condition.field);
    clauses.push(conditionClause(condition, kind));
  }
  if (clauses.length === 0) return {};
  if (clauses.length === 1) return clauses[0];
  return { $and: clauses };
}

export async function listDocuments(
  context: StudioContext,
  collectionName: string,
  query: DocumentsQuery,
): Promise<DocumentsPage> {
  const entry = await requireEntry(context, collectionName);
  const limit = Math.min(
    MAX_PAGE_SIZE,
    Math.max(1, query.limit ?? DEFAULT_PAGE_SIZE),
  );
  if (query.after && query.before) {
    throw new StudioHttpError(400, "Use either after or before, not both");
  }
  const sortField = query.sort || "_id";
  if (!FIELD_PATH.test(sortField)) {
    throw new StudioHttpError(400, `Invalid sort field "${sortField}"`);
  }
  const direction: SortDirection = query.direction === "desc" ? "desc" : "asc";
  const base = buildDocumentFilter(entry, query);
  const collection = context.db.collection(entry.name);
  const total = query.count
    ? await boundedCount(context, entry.name, base, RESULT_COUNT_LIMIT)
    : undefined;

  if (sortField !== "_id") {
    const offset = Math.min(MAX_OFFSET, Math.max(0, query.offset ?? 0));
    const order = direction === "desc" ? -1 : 1;
    const rows = await collection
      .find(base, {
        sort: { [sortField]: order, _id: order },
        skip: offset,
        limit: limit + 1,
        maxTimeMS: QUERY_TIME_LIMIT_MS,
      })
      .toArray();
    const overflow = rows.length > limit;
    const page = overflow ? rows.slice(0, limit) : rows;
    const result: DocumentsPage = {
      collection: entry.name,
      items: page,
      limit,
      hasNext: overflow && offset + limit < MAX_OFFSET,
      hasPrevious: offset > 0,
      sort: { field: sortField, direction, paging: "offset" },
      offset,
    };
    if (total) result.total = total;
    return result;
  }

  const backwards = Boolean(query.before);
  const descending = direction === "desc";
  const anchor = query.after ?? query.before;
  const comparison = backwards !== descending ? "$lt" : "$gt";
  const filter: m.Filter<m.Document> = anchor
    ? { $and: [base, { _id: { [comparison]: parseIdParam(anchor) } }] }
    : base;

  const rows = await collection
    .find(filter, {
      sort: { _id: backwards !== descending ? -1 : 1 },
      limit: limit + 1,
      maxTimeMS: QUERY_TIME_LIMIT_MS,
    })
    .toArray();

  const overflow = rows.length > limit;
  const page = overflow ? rows.slice(0, limit) : rows;
  if (backwards) page.reverse();

  const result: DocumentsPage = {
    collection: entry.name,
    items: page,
    limit,
    hasNext: backwards ? true : overflow,
    hasPrevious: backwards ? overflow : Boolean(query.after),
    sort: { field: "_id", direction, paging: "cursor" },
  };
  if (page.length > 0) {
    result.firstId = encodeId(page[0]._id);
    result.lastId = encodeId(page[page.length - 1]._id);
  } else if (backwards) {
    result.hasNext = true;
  }
  if (total) result.total = total;
  return result;
}

export async function getDocument(
  context: StudioContext,
  collectionName: string,
  rawId: string,
): Promise<Record<string, unknown>> {
  const entry = await requireEntry(context, collectionName);
  const document = await context.db
    .collection(entry.name)
    .findOne(
      { _id: parseIdParam(rawId) as m.Document["_id"] },
      { maxTimeMS: QUERY_TIME_LIMIT_MS },
    );
  if (!document) {
    throw new StudioHttpError(404, `No document with _id ${rawId}`);
  }
  return document;
}

export async function listScopes(
  context: StudioContext,
  collectionName: string,
  limit: number,
): Promise<{ top: ScopeCount[]; distinct: number }> {
  const entry = await requireEntry(context, collectionName);
  if (entry.kind !== "scopedMultiCollection") {
    throw new StudioHttpError(
      400,
      `"${entry.name}" is not a scoped multi-collection`,
    );
  }
  if (!entry.exists) return { top: [], distinct: 0 };
  return await countScopes(context, entry.name, limit);
}

export function parseDocumentsQuery(params: URLSearchParams): DocumentsQuery {
  const filters: Record<string, string> = {};
  for (const [key, value] of params.entries()) {
    if (key.startsWith("f.")) filters[key.slice(2)] = value;
  }
  const direction = params.get("dir");
  return {
    type: params.get("type") || undefined,
    scope: params.get("scope") ?? undefined,
    after: params.get("after") || undefined,
    before: params.get("before") || undefined,
    limit: clampInteger(
      params.get("limit"),
      DEFAULT_PAGE_SIZE,
      1,
      MAX_PAGE_SIZE,
    ),
    filters,
    conditions: params.getAll("w").map(parseCondition),
    sort: params.get("sort") || undefined,
    direction: direction === "desc" ? "desc" : "asc",
    offset: clampInteger(params.get("offset"), 0, 0, MAX_OFFSET),
    count: params.get("count") === "1",
  };
}
