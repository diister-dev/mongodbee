export const INLINE_WIDTH = 80;

export type JsonTokenKind =
  | "key"
  | "string"
  | "number"
  | "literal"
  | "punct"
  | "space";

export interface JsonToken {
  kind: JsonTokenKind;
  text: string;
}

function isContainer(value: unknown): value is object {
  return value !== null && typeof value === "object";
}

function entriesOf(value: object): (readonly [string | undefined, unknown])[] {
  return Array.isArray(value)
    ? value.map((item) => [undefined, item] as const)
    : Object.entries(value).map(([key, item]) => [key, item] as const);
}

function inline(value: unknown): string {
  if (!isContainer(value)) return JSON.stringify(value) ?? "null";
  const entries = entriesOf(value);
  if (Array.isArray(value)) {
    return `[${entries.map(([, item]) => inline(item)).join(", ")}]`;
  }
  if (entries.length === 0) return "{}";
  return `{ ${entries.map(([key, item]) => `${JSON.stringify(key)}: ${inline(item)}`).join(", ")} }`;
}

export function compactJson(
  value: unknown,
  width: number = INLINE_WIDTH,
  indent = "",
): string {
  if (!isContainer(value)) return JSON.stringify(value) ?? "null";
  const entries = entriesOf(value);
  if (entries.length === 0) return Array.isArray(value) ? "[]" : "{}";
  const spaced = inline(value);
  if (indent.length + spaced.length <= width) return spaced;
  const inner = `${indent}  `;
  const lines = entries.map(([key, item]) => {
    const rendered = compactJson(item, width, inner);
    return key === undefined
      ? `${inner}${rendered}`
      : `${inner}${JSON.stringify(key)}: ${rendered}`;
  });
  const [open, close] = Array.isArray(value) ? ["[", "]"] : ["{", "}"];
  return `${open}\n${lines.join(",\n")}\n${indent}${close}`;
}

const TOKEN =
  /("(?:[^"\\]|\\.)*")(\s*:)?|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|(true|false|null)|([{}[\],:])|(\s+)/g;

export function tokenizeJson(text: string): JsonToken[] {
  const tokens: JsonToken[] = [];
  let last = 0;
  for (const match of text.matchAll(TOKEN)) {
    const index = match.index ?? 0;
    if (index > last)
      tokens.push({ kind: "punct", text: text.slice(last, index) });
    const [whole, string, colon, number, literal, punct] = match;
    if (string !== undefined) {
      tokens.push({ kind: colon ? "key" : "string", text: string });
      if (colon) tokens.push({ kind: "punct", text: colon });
    } else if (number !== undefined) {
      tokens.push({ kind: "number", text: number });
    } else if (literal !== undefined) {
      tokens.push({ kind: "literal", text: literal });
    } else if (punct !== undefined) {
      tokens.push({ kind: "punct", text: punct });
    } else {
      tokens.push({ kind: "space", text: whole });
    }
    last = index + whole.length;
  }
  if (last < text.length)
    tokens.push({ kind: "punct", text: text.slice(last) });
  return tokens;
}
