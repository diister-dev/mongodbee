export interface JsonSchemaNode {
  bsonType?: string | string[];
  type?: string | string[];
  properties?: Record<string, JsonSchemaNode>;
  required?: string[];
  items?: JsonSchemaNode | JsonSchemaNode[];
  anyOf?: JsonSchemaNode[];
  oneOf?: JsonSchemaNode[];
  enum?: unknown[];
  pattern?: string;
  [key: string]: unknown;
}

const LIMIT_KEYS = [
  "minLength",
  "maxLength",
  "minimum",
  "maximum",
  "exclusiveMinimum",
  "exclusiveMaximum",
  "multipleOf",
  "minItems",
  "maxItems",
  "uniqueItems",
  "minProperties",
  "maxProperties",
  "additionalProperties",
] as const;

function asNode(value: unknown): JsonSchemaNode | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonSchemaNode)
    : undefined;
}

export function jsonTypeOf(node: JsonSchemaNode | undefined): string {
  if (!node) return "";
  const raw = node.bsonType ?? node.type;
  if (Array.isArray(raw)) return raw.join(" | ");
  if (typeof raw === "string") return raw;
  const branches = node.anyOf ?? node.oneOf;
  if (branches) {
    const types = [...new Set(branches.map(jsonTypeOf).filter(Boolean))];
    return types.join(" | ");
  }
  return "";
}

export function jsonSchemaFacts(node: JsonSchemaNode | undefined): string[] {
  if (!node) return [];
  const facts: string[] = [];
  for (const key of LIMIT_KEYS) {
    if (node[key] !== undefined) facts.push(factLabel(key, node[key]));
  }
  if (node.enum) facts.push(`enum of ${node.enum.length}`);
  if (node.pattern) facts.push("pattern");
  return facts;
}

export function jsonSchemaChild(
  parent: JsonSchemaNode | undefined,
  name: string,
): JsonSchemaNode | undefined {
  if (!parent) return undefined;
  const own = asNode(parent.properties?.[name]);
  if (own) return own;
  const items = Array.isArray(parent.items) ? undefined : asNode(parent.items);
  if (items) return jsonSchemaChild(items, name);
  for (const branch of parent.anyOf ?? parent.oneOf ?? []) {
    const found = jsonSchemaChild(branch, name);
    if (found) return found;
  }
  return undefined;
}

export function isRequired(
  parent: JsonSchemaNode | undefined,
  name: string,
): boolean | undefined {
  if (!parent) return undefined;
  if (parent.properties && name in parent.properties) {
    return (parent.required ?? []).includes(name);
  }
  const items = Array.isArray(parent.items) ? undefined : asNode(parent.items);
  if (items) return isRequired(items, name);
  for (const branch of parent.anyOf ?? parent.oneOf ?? []) {
    const found = isRequired(branch, name);
    if (found !== undefined) return found;
  }
  return undefined;
}

export interface JsonSchemaFact {
  label: string;
  detail?: string;
  inert?: string;
}

const NUMBER_TYPES = ["int", "long", "double", "decimal", "number"];

const KEY_TYPES: Record<string, readonly string[]> = {
  minLength: ["string"],
  maxLength: ["string"],
  pattern: ["string"],
  minimum: NUMBER_TYPES,
  maximum: NUMBER_TYPES,
  exclusiveMinimum: NUMBER_TYPES,
  exclusiveMaximum: NUMBER_TYPES,
  multipleOf: NUMBER_TYPES,
  minItems: ["array"],
  maxItems: ["array"],
  uniqueItems: ["array"],
  minProperties: ["object"],
  maxProperties: ["object"],
  additionalProperties: ["object"],
};

function declaredTypes(node: JsonSchemaNode): string[] | undefined {
  const raw = node.bsonType ?? node.type;
  if (Array.isArray(raw)) return raw.map(String);
  return typeof raw === "string" ? [raw] : undefined;
}

function inertReason(node: JsonSchemaNode, key: string): string | undefined {
  const types = declaredTypes(node);
  const applies = KEY_TYPES[key];
  if (!types || !applies) return undefined;
  if (types.some((type) => applies.includes(type))) return undefined;
  return `MongoDB ignores ${key} on a ${types.join(" or ")}: it only applies to ${applies.includes("int") ? "numbers" : `${applies[0]} values`}`;
}

export function jsonSchemaFactDetails(
  node: JsonSchemaNode | undefined,
): JsonSchemaFact[] {
  if (!node) return [];
  const facts: JsonSchemaFact[] = [];
  const add = (key: string, fact: JsonSchemaFact) => {
    const inert = inertReason(node, key);
    facts.push(inert ? { ...fact, inert } : fact);
  };
  for (const key of LIMIT_KEYS) {
    if (node[key] !== undefined)
      add(
        key,
        isContainerValue(node[key])
          ? { label: key, detail: JSON.stringify(node[key]) }
          : { label: factLabel(key, node[key]) },
      );
  }
  if (node.enum) {
    facts.push({
      label: `enum of ${node.enum.length}`,
      detail: node.enum.map((value) => JSON.stringify(value)).join(", "),
    });
  }
  if (node.pattern)
    add("pattern", { label: "pattern", detail: `/${node.pattern}/` });
  return facts;
}

function isContainerValue(value: unknown): boolean {
  return value !== null && typeof value === "object";
}

function factLabel(key: string, value: unknown): string {
  return isContainerValue(value) ? key : `${key} ${String(value)}`;
}
