import type { StudioContext } from "../context.ts";
import {
  buildDocumentFilter,
  parseDocumentsQuery,
  requireEntry,
} from "./documents.ts";
import { QUERY_TIME_LIMIT_MS } from "./overview.ts";

export const COVERAGE_SAMPLE = 5_000;

export interface FieldCoverage {
  sampled: number;
  fields: Record<string, number>;
}

interface CoverageRow {
  _id: string;
  count: number;
}

export async function getFieldCoverage(
  context: StudioContext,
  collectionName: string,
  params: URLSearchParams,
): Promise<FieldCoverage> {
  const entry = await requireEntry(context, collectionName);
  if (!entry.exists) return { sampled: 0, fields: {} };
  const base = buildDocumentFilter(entry, parseDocumentsQuery(params));
  const [result] = await context.db
    .collection(entry.name)
    .aggregate<{ sampled: { n: number }[]; fields: CoverageRow[] }>(
      [
        ...(Object.keys(base).length > 0 ? [{ $match: base }] : []),
        { $sample: { size: COVERAGE_SAMPLE } },
        {
          $facet: {
            sampled: [{ $count: "n" }],
            fields: [
              { $project: { pairs: { $objectToArray: "$$ROOT" } } },
              { $unwind: "$pairs" },
              { $match: { "pairs.v": { $ne: null } } },
              { $group: { _id: "$pairs.k", count: { $sum: 1 } } },
            ],
          },
        },
      ],
      { maxTimeMS: QUERY_TIME_LIMIT_MS, allowDiskUse: false },
    )
    .toArray();
  return {
    sampled: result?.sampled[0]?.n ?? 0,
    fields: Object.fromEntries(
      (result?.fields ?? []).map((row) => [row._id, row.count]),
    ),
  };
}
