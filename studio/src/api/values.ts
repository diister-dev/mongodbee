import type { StudioContext } from "../context.ts";
import { clampInteger, StudioHttpError } from "../http.ts";
import {
  buildDocumentFilter,
  escapeRegex,
  FIELD_PATH,
  parseDocumentsQuery,
  requireEntry,
} from "./documents.ts";
import { QUERY_TIME_LIMIT_MS } from "./overview.ts";

export const VALUES_SCAN_LIMIT = 20_000;
export const VALUES_DEFAULT_LIMIT = 20;
export const VALUES_MAX_LIMIT = 100;
export const VALUES_PREFIX_MAX = 200;

export interface FieldValue {
  value: unknown;
  count: number;
}

export interface FieldValues {
  field: string;
  values: FieldValue[];
  scanned: number;
  sampled: boolean;
}

interface FacetResult {
  values: { _id: unknown; count: number }[];
  scanned: { n: number }[];
}

export async function listFieldValues(
  context: StudioContext,
  collectionName: string,
  params: URLSearchParams,
): Promise<FieldValues> {
  const field = params.get("field") ?? "";
  if (!FIELD_PATH.test(field)) {
    throw new StudioHttpError(400, `Invalid field "${field}"`);
  }
  const prefix = params.get("q") ?? "";
  if (prefix.length > VALUES_PREFIX_MAX) {
    throw new StudioHttpError(400, "The search text is too long");
  }
  const limit = clampInteger(
    params.get("limit"),
    VALUES_DEFAULT_LIMIT,
    1,
    VALUES_MAX_LIMIT,
  );
  const entry = await requireEntry(context, collectionName);
  const base = buildDocumentFilter(entry, parseDocumentsQuery(params));
  const own = prefix
    ? { [field]: { $regex: `^${escapeRegex(prefix)}`, $options: "i" } }
    : { [field]: { $exists: true } };
  const match = Object.keys(base).length === 0 ? own : { $and: [base, own] };
  const [result] = await context.db
    .collection(entry.name)
    .aggregate<FacetResult>(
      [
        { $match: match },
        { $limit: VALUES_SCAN_LIMIT },
        {
          $facet: {
            values: [
              { $project: { _id: 0, value: `$${field}` } },
              { $unwind: "$value" },
              ...(prefix
                ? [
                    {
                      $match: {
                        value: {
                          $regex: `^${escapeRegex(prefix)}`,
                          $options: "i",
                        },
                      },
                    },
                  ]
                : []),
              { $group: { _id: "$value", count: { $sum: 1 } } },
              { $sort: { count: -1, _id: 1 } },
              { $limit: limit },
            ],
            scanned: [{ $count: "n" }],
          },
        },
      ],
      { maxTimeMS: QUERY_TIME_LIMIT_MS },
    )
    .toArray();
  const scanned = result?.scanned[0]?.n ?? 0;
  return {
    field,
    values: (result?.values ?? [])
      .filter((row) => row._id !== null && row._id !== undefined)
      .map((row) => ({ value: row._id, count: row.count })),
    scanned,
    sampled: scanned >= VALUES_SCAN_LIMIT,
  };
}
