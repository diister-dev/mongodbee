export type Path = readonly (string | number)[];

export interface FormNode {
  kind: string;
  optional?: boolean;
  nullable?: boolean;
  hasDefault?: boolean;
  values?: unknown[];
  literal?: unknown;
  entries?: Record<string, FormNode>;
  item?: FormNode;
  options?: FormNode[];
  discriminator?: string;
  ref?: string;
  checks?: { type: string; requirement?: unknown }[];
}

export interface Issue {
  path: string;
  message: string;
}

const OBJECT_KINDS = new Set([
  "object",
  "loose_object",
  "strict_object",
  "object_with_rest",
]);

export function isObjectKind(node: FormNode | undefined): boolean {
  return Boolean(node && OBJECT_KINDS.has(node.kind));
}

export function pathKey(path: Path): string {
  return path.map(String).join(".");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function getAt(root: unknown, path: Path): unknown {
  let current = root;
  for (const key of path) {
    if (Array.isArray(current) && typeof key === "number")
      current = current[key];
    else if (isRecord(current)) current = current[String(key)];
    else return undefined;
  }
  return current;
}

export function setAt(root: unknown, path: Path, value: unknown): unknown {
  if (path.length === 0) return value;
  const [head, ...rest] = path;
  if (typeof head === "number") {
    const list = Array.isArray(root) ? [...root] : [];
    list[head] = setAt(list[head], rest, value);
    return list;
  }
  const record = isRecord(root) ? { ...root } : {};
  record[head] = setAt(record[head], rest, value);
  return record;
}

export function removeAt(root: unknown, path: Path): unknown {
  if (path.length === 0) return undefined;
  const [head, ...rest] = path;
  if (rest.length > 0) {
    const child = getAt(root, [head]);
    return setAt(root, [head], removeAt(child, rest));
  }
  if (typeof head === "number" && Array.isArray(root)) {
    return root.filter((_, index) => index !== head);
  }
  if (isRecord(root)) {
    const { [String(head)]: _removed, ...kept } = root;
    return kept;
  }
  return root;
}

export function defaultFor(node: FormNode): unknown {
  if (node.nullable && !node.values && node.kind !== "boolean") return null;
  if (node.values && node.values.length > 0) return node.values[0];
  if (node.literal !== undefined) return node.literal;
  switch (node.kind) {
    case "string":
      return node.ref ? "" : "";
    case "number":
    case "bigint":
      return 0;
    case "boolean":
      return false;
    case "date":
      return { $date: new Date().toISOString() };
    case "array":
    case "tuple":
      return [];
    case "record":
      return {};
    case "variant":
    case "union": {
      const first = node.options?.[0];
      return first ? defaultFor(first) : null;
    }
    default:
      if (isObjectKind(node)) return requiredDefaults(node.entries ?? {});
      return null;
  }
}

export function requiredDefaults(
  entries: Record<string, FormNode>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(entries)) {
    if (child.optional || child.hasDefault) continue;
    result[key] = defaultFor(child);
  }
  return result;
}

export function variantOption(
  node: FormNode,
  value: unknown,
): FormNode | undefined {
  if (!node.options || !node.discriminator) return undefined;
  const tag = isRecord(value) ? value[node.discriminator] : undefined;
  return (
    node.options.find(
      (option) => option.entries?.[node.discriminator!]?.literal === tag,
    ) ?? node.options[0]
  );
}

export function variantTags(node: FormNode): unknown[] {
  if (!node.discriminator) return [];
  return (node.options ?? [])
    .map((option) => option.entries?.[node.discriminator!]?.literal)
    .filter((tag) => tag !== undefined);
}

export function switchVariant(
  node: FormNode,
  value: unknown,
  tag: unknown,
): Record<string, unknown> {
  const option = (node.options ?? []).find(
    (candidate) => candidate.entries?.[node.discriminator!]?.literal === tag,
  );
  const fresh = option ? requiredDefaults(option.entries ?? {}) : {};
  const previous = isRecord(value) ? value : {};
  for (const key of Object.keys(fresh)) {
    if (key in previous && key !== node.discriminator)
      fresh[key] = previous[key];
  }
  fresh[node.discriminator!] = tag;
  return fresh;
}

export function issuesAt(issues: readonly Issue[], path: Path): string[] {
  const key = pathKey(path);
  return issues
    .filter((issue) => issue.path === key)
    .map((issue) => issue.message);
}

export function issuesUnder(issues: readonly Issue[], path: Path): number {
  const key = pathKey(path);
  return issues.filter(
    (issue) => issue.path === key || issue.path.startsWith(`${key}.`),
  ).length;
}

export function isDateValue(value: unknown): value is { $date: string } {
  return (
    isRecord(value) &&
    typeof value.$date === "string" &&
    Object.keys(value).length === 1
  );
}

export function numberFromText(text: string): number | undefined {
  const trimmed = text.trim();
  if (trimmed === "") return undefined;
  const value = Number(trimmed.replace(",", "."));
  return Number.isFinite(value) ? value : undefined;
}

export function checkHint(node: FormNode): string | undefined {
  const parts: string[] = [];
  for (const check of node.checks ?? []) {
    const requirement = check.requirement;
    switch (check.type) {
      case "min_length":
        parts.push(
          `at least ${requirement} character${requirement === 1 ? "" : "s"}`,
        );
        break;
      case "max_length":
        parts.push(
          `at most ${requirement} character${requirement === 1 ? "" : "s"}`,
        );
        break;
      case "min_value":
        parts.push(`at least ${requirement}`);
        break;
      case "max_value":
        parts.push(`at most ${requirement}`);
        break;
      case "email":
        parts.push("an email address");
        break;
      case "url":
        parts.push("a URL");
        break;
      case "integer":
        parts.push("a whole number");
        break;
      case "non_empty":
        parts.push("not empty");
        break;
      default:
        break;
    }
  }
  return parts.length > 0 ? parts.join(", ") : undefined;
}
