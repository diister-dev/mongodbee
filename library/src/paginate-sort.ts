/**
 * @fileoverview Shared machinery for `paginate({ sortPipeline })` — sorting a
 * page on a field COMPUTED by aggregation stages (typically a `$lookup`ed
 * document's field), used by all three paginating surfaces (collection,
 * multiCollection, scopedMultiCollection views).
 *
 * The paginate assembly for this path is:
 *
 *   $match(base) → ...sortPipeline → $addFields(hidden keys)
 *     → $match(cursor via $expr) → $sort(hidden keys) → $unset(hidden keys)
 *     → ...pipeline
 *
 * WHY hidden normalized sort keys (verified against MongoDB 8.0):
 * - Query operators (`{f: {$gt: v}}`) NEVER match a missing field, so a
 *   cursor ladder built with them silently drops parents that have no
 *   joined document from page 2 onward. The cursor must use `$expr`.
 * - But `$expr` comparisons rank a missing field STRICTLY BELOW `null`,
 *   while `$sort` ranks missing EQUAL to `null` (ties broken by the next
 *   sort key). A raw `$expr` ladder therefore disagrees with the order
 *   `$sort` produced around every missing/null boundary.
 * Materializing each sort key as `{$ifNull: [key, null]}` into a hidden
 * field, then running BOTH the `$sort` AND the `$expr` cursor against those
 * hidden fields, makes the two agree by construction.
 */

import { getNestedValue } from "./dot-notation.ts";

/** Single MongoDB aggregation stage (already-built object form). */
type AggregationStage = Record<string, unknown>;

/**
 * Hidden per-sort-key alias prefix. Improbable in user schemas; the aliases
 * are `$unset` right after the `$sort` consumes them, before the user
 * `pipeline` runs.
 */
const HIDDEN_SORT_PREFIX = "__mongodbee_sort_";

/** Precomputed stages + specs for the sortPipeline paginate path. */
export type SortMachinery = {
  /** Sort fields in cursor-ladder order (original names). */
  fields: string[];
  /** Hidden alias per sort field (same order as `fields`). */
  aliases: string[];
  /** `$addFields` materializing each sort key as `{$ifNull: [key, null]}`. */
  normalizeStage: AggregationStage;
  /** `$sort` spec over the hidden aliases (original directions). */
  hiddenSort: Record<string, 1 | -1>;
  /** Same spec, directions reversed — for backward (`beforeId`) pages. */
  hiddenSortReversed: Record<string, 1 | -1>;
  /** `$unset` removing the hidden aliases once the sort has consumed them. */
  unsetStage: AggregationStage;
};

/** Build the hidden-sort-key machinery for a normalized sort spec. */
export function buildSortMachinery(
  sortObj: Record<string, 1 | -1>,
): SortMachinery {
  const fields = Object.keys(sortObj);
  const aliases = fields.map((_, i) => `${HIDDEN_SORT_PREFIX}${i}`);
  const addFields: Record<string, unknown> = {};
  const hiddenSort: Record<string, 1 | -1> = {};
  const hiddenSortReversed: Record<string, 1 | -1> = {};
  for (let i = 0; i < fields.length; i++) {
    addFields[aliases[i]] = { $ifNull: [`$${fields[i]}`, null] };
    hiddenSort[aliases[i]] = sortObj[fields[i]];
    hiddenSortReversed[aliases[i]] = sortObj[fields[i]] === 1 ? -1 : 1;
  }
  return {
    fields,
    aliases,
    normalizeStage: { $addFields: addFields },
    hiddenSort,
    hiddenSortReversed,
    unsetStage: { $unset: aliases },
  };
}

/**
 * Build the cursor filter for the sortPipeline path: an `$expr` ladder over
 * the hidden sort keys, anchored on a document that was resolved THROUGH the
 * sort pipeline. Anchor values are normalized `?? null` to mirror
 * `normalizeStage`, and `$literal`-wrapped so a string value can never be
 * misread as a field path.
 *
 * Returns a filter usable as its own `{ $match: ... }` stage — it MUST run
 * after `normalizeStage` (it references the hidden aliases).
 */
export function buildExprCursorFilter(
  machinery: SortMachinery,
  anchorDoc: Record<string, unknown>,
  direction: "after" | "before",
): AggregationStage {
  const { fields, aliases, hiddenSort } = machinery;
  const isForward = direction === "after";
  const anchorValue = (i: number): unknown =>
    getNestedValue(anchorDoc, fields[i]) ?? null;

  const conditions: unknown[] = [];
  for (let i = 0; i < fields.length; i++) {
    const clauses: unknown[] = [];
    // All previous sort keys equal the anchor's…
    for (let j = 0; j < i; j++) {
      clauses.push({ $eq: [`$${aliases[j]}`, { $literal: anchorValue(j) }] });
    }
    // …and the current key is strictly past it.
    const op = (hiddenSort[aliases[i]] === 1) === isForward ? "$gt" : "$lt";
    clauses.push({ [op]: [`$${aliases[i]}`, { $literal: anchorValue(i) }] });
    conditions.push(clauses.length === 1 ? clauses[0] : { $and: clauses });
  }
  return {
    $expr: conditions.length === 1 ? conditions[0] : { $or: conditions },
  };
}

/**
 * Assemble a paginate pipeline for the sortPipeline path. ONE builder emits
 * both the data shape and the `$count` shape so the two can never drift —
 * counting a cursor against a pipeline that never computed the sort keys is
 * exactly the bug family this feature must not reintroduce.
 *
 * Data shape:  `$match(base) → ...sortStages → normalize → $match(cursor)?
 *               → $sort(hidden) → $unset(hidden) → ...pipeline`
 * Count shape: same head, no `$sort` (counts are order-independent), plus a
 *              trailing `$count` — the `$unset` stays so `pipeline` sees the
 *              same document shape in both.
 */
export function buildSortPaginateStages(opts: {
  baseMatch: Record<string, unknown>;
  sortStages: AggregationStage[];
  machinery: SortMachinery;
  /** `$expr` filter from {@link buildExprCursorFilter}; null on page 1. */
  cursorFilter: AggregationStage | null;
  /** After-sort user pipeline (display joins, …). */
  pipeline: AggregationStage[];
  /** Walk the reversed sort — backward (`beforeId`) paging. */
  reverse?: boolean;
  /** Emit the `$count` shape instead of the sorted data shape. */
  count?: boolean;
}): AggregationStage[] {
  const { machinery } = opts;
  const head: AggregationStage[] = [
    { $match: opts.baseMatch },
    ...opts.sortStages,
    machinery.normalizeStage,
    ...(opts.cursorFilter ? [{ $match: opts.cursorFilter }] : []),
  ];
  if (opts.count) {
    return [
      ...head,
      machinery.unsetStage,
      ...opts.pipeline,
      { $count: "total" },
    ];
  }
  return [
    ...head,
    {
      $sort: opts.reverse ? machinery.hiddenSortReversed : machinery.hiddenSort,
    },
    machinery.unsetStage,
    ...opts.pipeline,
  ];
}

/**
 * Fields provably INTRODUCED by a stage array: a `$lookup`'s `as` and the
 * top-level keys of `$addFields`/`$set`. Reshaping stages (`$group`,
 * `$project`, `$replaceRoot`, `$facet`, …) are untraceable — we deliberately
 * do NOT guess through them.
 */
function collectIntroducedFields(stages: AggregationStage[]): Set<string> {
  const introduced = new Set<string>();
  for (const stage of stages) {
    const lookup = stage.$lookup as { as?: unknown } | undefined;
    if (lookup && typeof lookup.as === "string") introduced.add(lookup.as);
    for (const stageName of ["$addFields", "$set"]) {
      const spec = stage[stageName];
      if (spec && typeof spec === "object") {
        for (const key of Object.keys(spec)) introduced.add(key);
      }
    }
  }
  return introduced;
}

/** `field` is `key` itself or a dot-path under it. */
function coveredBy(field: string, keys: Set<string>): string | undefined {
  for (const key of keys) {
    if (field === key || field.startsWith(`${key}.`)) return key;
  }
  return undefined;
}

/**
 * Guard: a sort key that is provably produced by the after-sort `pipeline`
 * cannot be sorted on — the `$sort` (and the cursor match) run before that
 * pipeline. Fail loud with the fix (`sortPipeline`) instead of silently
 * producing wrong pages. Fields the sort pipeline itself introduces are
 * exempt: the sort resolves them before `pipeline` re-touches them.
 */
export function assertSortResolvableBeforePipeline(
  sortFields: string[],
  sortPipelineStages: AggregationStage[],
  pipelineStages: AggregationStage[],
): void {
  if (pipelineStages.length === 0) return;
  const after = collectIntroducedFields(pipelineStages);
  if (after.size === 0) return;
  const before = collectIntroducedFields(sortPipelineStages);
  for (const field of sortFields) {
    if (coveredBy(field, before)) continue;
    const key = coveredBy(field, after);
    if (key) {
      throw new Error(
        `paginate: sort field "${field}" is introduced by the \`pipeline\` ` +
          `option (key "${key}"), but \`pipeline\` runs AFTER the $sort and ` +
          `the cursor match — the sort cannot see it. Move the stage(s) ` +
          `that produce "${key}" into \`sortPipeline\`, which runs before ` +
          `the sort.`,
      );
    }
  }
}

/**
 * One rung of the index-strategy cursor ladder: the condition carried by the
 * sort field itself, given the anchor's value and the direction the walk moves
 * in (`$gt` = toward higher ranks, `$lt` = toward lower ones).
 *
 * `$sort` ranks a MISSING field equal to null, and both below every real
 * value. Query operators disagree: `{f: {$gt: null}}` matches nothing, and
 * `{f: {$lt: v}}` skips null and missing. A ladder built from raw `$gt`/`$lt`
 * therefore dead-ends at the null boundary — the walk stops the first time its
 * anchor sits in the null block, while every document that HAS a value is
 * still unvisited, and `position` makes that look like the end of the list.
 *
 * These shapes agree with `$sort` across the boundary while staying query
 * operators, so the sort index remains usable (unlike the `$expr` ladder the
 * sortPipeline path needs).
 *
 * Returns `null` when nothing can rank beyond the anchor on this field and the
 * rung must be dropped — going lower than the null block.
 *
 * Cross-TYPE boundaries (a field holding both numbers and strings) are NOT
 * covered: BSON orders types, `$gt` does not compare across them. Sort on a
 * field of one type plus null/missing, which is what an optional field is.
 */
export function cursorRungCondition(
  field: string,
  anchorValue: unknown,
  op: "$gt" | "$lt",
): Record<string, unknown> | null {
  const anchorIsNull = anchorValue === undefined || anchorValue === null;
  if (op === "$gt") {
    // Above null ranks everything that HAS a value (`$ne: null` excludes
    // missing too, which is exactly the null block).
    return anchorIsNull
      ? { [field]: { $ne: null } }
      : { [field]: { $gt: anchorValue } };
  }
  if (anchorIsNull) return null;
  // Below a real value: smaller values, then the null block. `{f: null}`
  // matches missing as well.
  return { $or: [{ [field]: { $lt: anchorValue } }, { [field]: null }] };
}

/**
 * Value to pin a PREVIOUS sort field to in a ladder rung. Missing normalizes
 * to `null` so the equality matches both the null and the missing documents,
 * mirroring how `$sort` ranks them together.
 */
export function cursorRungEquality(anchorValue: unknown): unknown {
  return anchorValue === undefined ? null : anchorValue;
}
