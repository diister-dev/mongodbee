export function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function wrapperKey(value) {
  if (!isPlainObject(value)) return null;
  const keys = Object.keys(value);
  if (keys.length === 1 && keys[0].startsWith("$")) return keys[0];
  return null;
}

function pad(value) {
  return String(value).padStart(2, "0");
}

export function toDate(raw) {
  const value = typeof raw === "string" ? raw : (raw?.$numberLong ?? raw);
  const date = new Date(typeof value === "string" && /^-?\d+$/.test(value) ? Number(value) : value);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function formatDate(raw) {
  const date = toDate(raw);
  if (!date) return String(raw);
  const day = `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}`;
  const midnight = date.getUTCHours() === 0 && date.getUTCMinutes() === 0 && date.getUTCSeconds() === 0;
  return midnight ? day : `${day} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}`;
}

export function formatDateTime(raw) {
  const date = toDate(raw);
  if (!date) return "";
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(
    date.getUTCHours(),
  )}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())} UTC`;
}

const NUMBER = new Intl.NumberFormat("en");

export function formatNumber(value) {
  if (typeof value !== "number") return String(value ?? 0);
  return Number.isInteger(value) && Math.abs(value) < 1e15 ? NUMBER.format(value) : String(value);
}

export function plural(count, singular, pluralForm = `${singular}s`) {
  return `${formatNumber(count)} ${count === 1 ? singular : pluralForm}`;
}

export function describeValue(value) {
  if (value === undefined) return { kind: "missing", text: "" };
  if (value === null) return { kind: "null", text: "null" };
  if (typeof value === "string") return { kind: "string", text: value };
  if (typeof value === "number") return { kind: "number", text: String(value) };
  if (typeof value === "boolean") return { kind: "boolean", text: String(value) };
  if (Array.isArray(value)) {
    return {
      kind: "array",
      text: value.length === 0 ? "empty list" : plural(value.length, "item"),
      count: value.length,
    };
  }
  const wrapper = wrapperKey(value);
  if (wrapper === "$date") return { kind: "date", text: formatDate(value.$date), full: formatDateTime(value.$date) };
  if (wrapper === "$oid") return { kind: "oid", text: value.$oid };
  if (wrapper === "$numberDecimal") return { kind: "number", text: value.$numberDecimal };
  if (wrapper === "$numberLong") return { kind: "number", text: value.$numberLong };
  if (wrapper === "$binary") return { kind: "binary", text: "binary" };
  if (wrapper === "$regularExpression") {
    return { kind: "string", text: `/${value.$regularExpression.pattern}/${value.$regularExpression.options}` };
  }
  const count = Object.keys(value).length;
  return { kind: "object", text: count === 0 ? "empty object" : plural(count, "field"), count };
}

export function idParam(id) {
  return JSON.stringify(id);
}

export function formatScope(scope) {
  if (typeof scope === "string") return scope;
  return JSON.stringify(scope);
}

export function kindLabel(kind) {
  switch (kind) {
    case "collection":
      return "Collection";
    case "multiCollection":
      return "Multi-collection";
    case "multiModelInstance":
      return "Multi-model instance";
    case "scopedMultiCollection":
      return "Scoped collection";
    case "internal":
      return "Internal";
    default:
      return "Undeclared";
  }
}

export function isTypedKind(kind) {
  return kind === "multiCollection" || kind === "multiModelInstance" || kind === "scopedMultiCollection";
}

export const GROUPS = [
  { kind: "collection", label: "Collections" },
  { kind: "multiCollection", label: "Multi-collections" },
  { kind: "scopedMultiCollection", label: "Scoped collections" },
  { kind: "multiModelInstance", label: "Multi-model instances" },
  { kind: "undeclared", label: "Undeclared" },
  { kind: "internal", label: "Internal" },
];

export function defaultTab(kind) {
  return isTypedKind(kind) ? "summary" : "data";
}
