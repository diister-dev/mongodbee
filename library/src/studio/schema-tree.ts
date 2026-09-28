import { type IndexDatabase, indexMetadataOf } from "../indexes.ts";
import { isRecord } from "../utils/guards.ts";

export interface SchemaCheck {
  type: string;
  requirement?: unknown;
}

export interface SchemaNode {
  kind: string;
  optional?: true;
  nullable?: true;
  hasDefault?: true;
  ref?: string;
  values?: unknown[];
  literal?: unknown;
  checks?: SchemaCheck[];
  index?: IndexDatabase;
  description?: string;
  discriminator?: string;
  entries?: Record<string, SchemaNode>;
  item?: SchemaNode;
  items?: SchemaNode[];
  options?: SchemaNode[];
  key?: SchemaNode;
  value?: SchemaNode;
  rest?: SchemaNode;
  truncated?: true;
}

type Raw = Readonly<Record<PropertyKey, unknown>>;

function isRaw(value: unknown): value is Raw {
  return typeof value === "object" && value !== null;
}

function listOf(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function recordOf(value: unknown): Readonly<Record<string, unknown>> {
  return isRecord(value) ? value : {};
}

const MAX_DEPTH = 16;
const REF_PATTERN = /^\^([A-Za-z0-9_.-]+):\[a-zA-Z0-9\]\+$/;

function serializeValue(value: unknown): unknown {
  if (value === null) return null;
  if (value instanceof RegExp) return `/${value.source}/${value.flags}`;
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "function") return "[function]";
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) return value.map(serializeValue);
  if (typeof value === "object") {
    try {
      return JSON.parse(JSON.stringify(value));
    } catch {
      return String(value);
    }
  }
  return value;
}

function collectActions(schema: Raw): Raw[] {
  const pipe = schema.pipe;
  if (!Array.isArray(pipe) || pipe.length === 0) return [];
  const [root, ...actions] = pipe;
  const inner =
    isRaw(root) && root !== schema && Array.isArray(root.pipe)
      ? collectActions(root)
      : [];
  return [...inner, ...actions.filter(isRaw)];
}

function applyActions(node: SchemaNode, schema: Raw): void {
  for (const action of collectActions(schema)) {
    if (action.kind === "metadata") {
      const index = indexMetadataOf(recordOf(action.metadata));
      if (index) node.index = index;
      if (
        action.type === "description" &&
        typeof action.description === "string"
      ) {
        node.description = action.description;
      }
      continue;
    }
    if (action.kind === "transformation") {
      (node.checks ??= []).push({ type: String(action.type) });
      continue;
    }
    if (action.kind !== "validation") continue;
    const requirement = action.requirement;
    if (action.type === "regex" && requirement instanceof RegExp) {
      const match = REF_PATTERN.exec(requirement.source);
      if (match) {
        node.ref = match[1];
        continue;
      }
    }
    const check: SchemaCheck = { type: String(action.type) };
    if (requirement !== undefined && typeof requirement !== "function") {
      check.requirement = serializeValue(requirement);
    }
    (node.checks ??= []).push(check);
  }
  const ownIndex = indexMetadataOf(recordOf(schema.metadata));
  if (ownIndex) node.index = ownIndex;
}

export function schemaToNode(schema: unknown, depth = 0): SchemaNode {
  if (!isRaw(schema)) return { kind: "unknown" };
  const raw = schema;
  const type = String(raw.type ?? "unknown");

  if (
    type === "optional" ||
    type === "exact_optional" ||
    type === "undefinedable" ||
    type === "nullable" ||
    type === "nullish"
  ) {
    const inner = schemaToNode(raw.wrapped, depth);
    if (type !== "nullable") inner.optional = true;
    if (type === "nullable" || type === "nullish") inner.nullable = true;
    if (raw.default !== undefined) inner.hasDefault = true;
    applyActions(inner, raw);
    return inner;
  }

  const node: SchemaNode = { kind: type };
  applyActions(node, raw);

  if (depth >= MAX_DEPTH) {
    node.truncated = true;
    return node;
  }

  switch (type) {
    case "object":
    case "loose_object":
    case "strict_object":
    case "object_with_rest": {
      node.entries = entriesToNodes(recordOf(raw.entries), depth + 1);
      if (raw.rest) node.rest = schemaToNode(raw.rest, depth + 1);
      break;
    }
    case "array":
      node.item = schemaToNode(raw.item, depth + 1);
      break;
    case "tuple":
    case "loose_tuple":
    case "strict_tuple":
    case "tuple_with_rest":
      node.items = listOf(raw.items).map((item) =>
        schemaToNode(item, depth + 1),
      );
      if (raw.rest) node.rest = schemaToNode(raw.rest, depth + 1);
      break;
    case "union":
    case "intersect":
      node.options = listOf(raw.options).map((option) =>
        schemaToNode(option, depth + 1),
      );
      break;
    case "variant":
      node.discriminator = String(raw.key);
      node.options = listOf(raw.options).map((option) =>
        schemaToNode(option, depth + 1),
      );
      break;
    case "picklist":
      node.values = listOf(raw.options).map(serializeValue);
      break;
    case "enum":
      node.values = (
        Array.isArray(raw.options)
          ? raw.options
          : Object.values(recordOf(raw.enum))
      ).map(serializeValue);
      break;
    case "literal":
      node.literal = serializeValue(raw.literal);
      break;
    case "record":
    case "map":
      node.key = schemaToNode(raw.key, depth + 1);
      node.value = schemaToNode(raw.value, depth + 1);
      break;
    case "set":
      node.value = schemaToNode(raw.value, depth + 1);
      break;
    default:
      break;
  }

  return node;
}

export function entriesToNodes(
  entries: Readonly<Record<string, unknown>>,
  depth = 0,
): Record<string, SchemaNode> {
  const result: Record<string, SchemaNode> = {};
  for (const [name, schema] of Object.entries(entries)) {
    result[name] = schemaToNode(schema, depth);
  }
  return result;
}
