import { storedCollection } from "@diister/mongodbee/inspect";
import { buildCatalog, type CatalogEntry, isTyped } from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import { StudioHttpError } from "../http.ts";
import { idPrefixOf, QUERY_TIME_LIMIT_MS } from "./overview.ts";

export const MAX_LABEL_IDS = 100;
export const LABEL_MAX_LENGTH = 80;

export const LABEL_FIELDS = [
  "name",
  "displayName",
  "title",
  "label",
  "fullName",
  "email",
  "slug",
  "code",
] as const;

const NESTED_LABEL_PARENTS = ["identity", "profile", "information", "info"];

const TYPED_ID = /^([A-Za-z][A-Za-z0-9_.-]*):([A-Za-z0-9][A-Za-z0-9_-]*)$/;

const LABEL_PROJECTION: Record<string, 1> = Object.fromEntries(
  [
    "_type",
    ...LABEL_FIELDS,
    "firstname",
    "firstName",
    "lastname",
    "lastName",
    ...NESTED_LABEL_PARENTS.flatMap((parent) =>
      [...LABEL_FIELDS, "firstname", "firstName", "lastname", "lastName"].map(
        (field) => `${parent}.${field}`,
      ),
    ),
  ].map((field) => [field, 1]),
);

export interface DocumentLabel {
  id: string;
  label: string;
  field: string;
  collection: string;
  type?: string;
}

function trimmed(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const text = value.trim();
  if (text === "") return undefined;
  return text.length > LABEL_MAX_LENGTH
    ? `${text.slice(0, LABEL_MAX_LENGTH - 1)}…`
    : text;
}

function localized(value: unknown): string | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  for (const text of Object.values(value)) {
    const found = trimmed(text);
    if (found) return found;
  }
  return undefined;
}

export function labelOf(
  document: Record<string, unknown>,
): { label: string; field: string } | undefined {
  const pick = (fields: readonly string[]) => {
    for (const field of fields) {
      const found = trimmed(document[field]) ?? localized(document[field]);
      if (found) return { label: found, field };
    }
    return undefined;
  };
  const primary = pick(LABEL_FIELDS.slice(0, LABEL_FIELDS.indexOf("email")));
  if (primary) return primary;
  const first = trimmed(document.firstname ?? document.firstName);
  const last = trimmed(document.lastname ?? document.lastName);
  if (first || last) {
    return { label: [first, last].filter(Boolean).join(" "), field: "name" };
  }
  const secondary = pick(LABEL_FIELDS.slice(LABEL_FIELDS.indexOf("email")));
  if (secondary) return secondary;
  for (const parent of NESTED_LABEL_PARENTS) {
    const nested = document[parent];
    if (!nested || typeof nested !== "object" || Array.isArray(nested)) {
      continue;
    }
    const inner = labelOf(nested as Record<string, unknown>);
    if (inner) return { label: inner.label, field: `${parent}.${inner.field}` };
  }
  return undefined;
}

function answers(entry: CatalogEntry, prefix: string): string[] | undefined {
  if (isTyped(entry)) {
    const types = Object.entries(entry.types)
      .filter(
        ([name, source]) => name === prefix || idPrefixOf(source) === prefix,
      )
      .map(([name]) => name);
    return types.length > 0 ? types : undefined;
  }
  if (entry.kind !== "collection") return undefined;
  const source = entry.types[entry.name];
  return entry.name === prefix || (source && idPrefixOf(source) === prefix)
    ? []
    : undefined;
}

export async function resolveLabels(
  context: StudioContext,
  ids: string[],
): Promise<DocumentLabel[]> {
  const unique = [...new Set(ids)];
  if (unique.length > MAX_LABEL_IDS) {
    throw new StudioHttpError(
      400,
      `At most ${MAX_LABEL_IDS} ids can be labelled at once`,
    );
  }
  const byPrefix = new Map<string, string[]>();
  for (const id of unique) {
    const match = TYPED_ID.exec(id);
    if (!match) continue;
    const list = byPrefix.get(match[1]) ?? [];
    list.push(id);
    byPrefix.set(match[1], list);
  }
  if (byPrefix.size === 0) return [];
  const catalog = (await buildCatalog(context)).filter(
    (entry) => entry.exists && !entry.internal,
  );
  const labels: DocumentLabel[] = [];
  for (const [prefix, prefixed] of byPrefix) {
    let remaining = prefixed;
    for (const entry of catalog) {
      if (remaining.length === 0) break;
      if (answers(entry, prefix) === undefined) continue;
      const found = await storedCollection(context.db, entry.name)
        .find(
          { _id: { $in: remaining } },
          {
            maxTimeMS: QUERY_TIME_LIMIT_MS,
            limit: remaining.length,
            projection: LABEL_PROJECTION,
          },
        )
        .toArray();
      const seen = new Set<string>();
      for (const document of found) {
        const id = String(document._id);
        seen.add(id);
        const label = labelOf(document as Record<string, unknown>);
        if (!label) continue;
        const item: DocumentLabel = {
          id,
          label: label.label,
          field: label.field,
          collection: entry.name,
        };
        if (typeof document._type === "string") item.type = document._type;
        labels.push(item);
      }
      remaining = remaining.filter((id) => !seen.has(id));
    }
  }
  return labels;
}

export async function getLabels(
  context: StudioContext,
  params: URLSearchParams,
): Promise<{ labels: DocumentLabel[] }> {
  return { labels: await resolveLabels(context, params.getAll("id")) };
}
