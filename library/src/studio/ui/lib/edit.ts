export const PROTECTED_KEYS = ["_id", "_type", "_scope"] as const;

type Json = Record<string, unknown>;

export interface DocumentChange {
  set: Json;
  unset: string[];
  expected: Json;
  added: string[];
  changed: string[];
  removed: string[];
}

export type ParseResult =
  | { ok: true; value: Json }
  | { ok: false; message: string; line?: number };

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical((value as Json)[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

export function editableOf(document: Json): Json {
  const result: Json = {};
  for (const [key, value] of Object.entries(document)) {
    if (!(PROTECTED_KEYS as readonly string[]).includes(key))
      result[key] = value;
  }
  return result;
}

export function parseEditable(text: string): ParseResult {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const position = /position (\d+)/.exec(message);
    const line = position
      ? text.slice(0, Number(position[1])).split("\n").length
      : undefined;
    return { ok: false, message, line };
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return { ok: false, message: "The document must be a JSON object" };
  }
  const reserved = Object.keys(value).filter((key) =>
    (PROTECTED_KEYS as readonly string[]).includes(key),
  );
  if (reserved.length > 0) {
    return {
      ok: false,
      message: `${reserved.join(", ")} cannot be edited here`,
    };
  }
  return { ok: true, value: value as Json };
}

export function diffDocument(original: Json, edited: Json): DocumentChange {
  const before = editableOf(original);
  const change: DocumentChange = {
    set: {},
    unset: [],
    expected: {},
    added: [],
    changed: [],
    removed: [],
  };
  for (const [key, value] of Object.entries(edited)) {
    if (!(key in before)) {
      change.set[key] = value;
      change.added.push(key);
    } else if (canonical(before[key]) !== canonical(value)) {
      change.set[key] = value;
      change.expected[key] = before[key];
      change.changed.push(key);
    }
  }
  for (const key of Object.keys(before)) {
    if (!(key in edited)) {
      change.unset.push(key);
      change.expected[key] = before[key];
      change.removed.push(key);
    }
  }
  return change;
}

export function hasChanges(change: DocumentChange): boolean {
  return (
    change.added.length + change.changed.length + change.removed.length > 0
  );
}

interface TemplateNode {
  kind: string;
  optional?: boolean;
  nullable?: boolean;
  hasDefault?: boolean;
  values?: unknown[];
  literal?: unknown;
  entries?: Record<string, TemplateNode>;
  options?: TemplateNode[];
  ref?: string;
}

function placeholder(node: TemplateNode): unknown {
  if (node.values && node.values.length > 0) return node.values[0];
  if (node.literal !== undefined) return node.literal;
  switch (node.kind) {
    case "string":
      return node.ref ? `${node.ref}:` : "";
    case "number":
    case "bigint":
      return 0;
    case "boolean":
      return false;
    case "date":
      return { $date: new Date(0).toISOString() };
    case "array":
    case "tuple":
      return [];
    case "object":
    case "strict_object":
    case "loose_object":
      return templateOf(node.entries ?? {});
    case "variant":
    case "union":
      return node.options?.[0] ? placeholder(node.options[0]) : null;
    case "record":
      return {};
    default:
      return null;
  }
}

export function templateOf(fields: Record<string, TemplateNode>): Json {
  const result: Json = {};
  for (const [key, node] of Object.entries(fields)) {
    if ((PROTECTED_KEYS as readonly string[]).includes(key)) continue;
    if (node.optional || node.hasDefault) continue;
    result[key] = node.nullable ? null : placeholder(node);
  }
  return result;
}
