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
 * Normalize a paginate `sort` option into the effective sort spec, appending
 * the `_id` tie-break when absent so duplicate sort values keep a stable
 * (cursor-safe) order.
 *
 * The tie-break follows the direction of the LAST explicit sort field —
 * NOT a fixed `1`. An index `{<field>: 1, _id: 1}` serves a sort only in its
 * own order or its exact reverse; `{field: -1, _id: 1}` matches neither, so a
 * fixed ascending tie-break silently turned EVERY descending page (cursor or
 * not) into a full blocking sort of the filtered set (measured: 2000 keys
 * examined for a page of 25 on a 2k scope, from page 1).
 */
export function normalizePaginateSort(sort: unknown): Record<string, 1 | -1> {
  const input = sort || { _id: 1 };
  const sortObj: Record<string, 1 | -1> =
    typeof input === "object" && !Array.isArray(input)
      ? { ...(input as Record<string, 1 | -1>) }
      : {
        _id: input === 1 || input === "asc" || input === "ascending" ? 1 : -1,
      };
  if (!("_id" in sortObj)) {
    const fields = Object.keys(sortObj);
    sortObj._id = fields.length > 0 ? sortObj[fields[fields.length - 1]] : 1;
  }
  return sortObj;
}

/**
 * Value to pin a PREVIOUS sort field to in a ladder rung. Missing normalizes
 * to `null` so the equality matches both the null and the missing documents,
 * mirroring how `$sort` ranks them together.
 */
export function cursorRungEquality(anchorValue: unknown): unknown {
  return anchorValue === undefined ? null : anchorValue;
}

/**
 * FLAT branch conditions for one rung of the index-strategy cursor ladder:
 * the predicates carried by the sort field itself, given the anchor's value
 * and the direction the walk moves in (`$gt` = toward higher ranks, `$lt` =
 * toward lower ones). One rung may contribute several branches; they are
 * mutually exclusive by construction.
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
 * sortPipeline path needs):
 *
 *   above null      → {f: {$ne: null}}     (everything that HAS a value)
 *   above a value   → {f: {$gt: v}}
 *   below a value   → {f: {$lt: v}} ∪ {f: null}   (…then the null block)
 *   below null      → nothing — no branch, nothing ranks lower
 *
 * `nonNullable` marks fields that structurally always exist (`_id`, `_ulid`,
 * `_scope`, `_type`): their rungs stay raw comparisons, sparing the planner
 * a dead `{f: null}` branch.
 *
 * Returns an EMPTY array when nothing can rank beyond the anchor on this
 * field — going lower than the null block.
 */
function cursorRungBranches(
  field: string,
  anchorValue: unknown,
  op: "$gt" | "$lt",
  nonNullable: boolean,
): Record<string, unknown>[] {
  const anchorIsNull = anchorValue === undefined || anchorValue === null;
  if (nonNullable) {
    return [{ [field]: { [op]: anchorValue } }];
  }
  if (op === "$gt") {
    return anchorIsNull
      ? [{ [field]: { $ne: null } }]
      : [{ [field]: { $gt: anchorValue } }];
  }
  if (anchorIsNull) return [];
  return [{ [field]: { $lt: anchorValue } }, { [field]: null }];
}

/**
 * Build the index-strategy cursor as FLAT disjunction branches (a DNF): each
 * branch pins every previous sort field to the anchor's value and carries one
 * rung predicate from {@link cursorRungBranches}. The result must be composed
 * with {@link composeCursorQuery} — never `$and`ed as one `{$or: ...}` next
 * to the base match (see there for why).
 */
export function buildCursorLadderBranches(opts: {
  /** Effective sort spec, tie-break included, in ladder order. */
  sortObj: Record<string, 1 | -1>;
  /** Anchor document (already enriched with any computed sort fields). */
  anchorDoc: Record<string, unknown>;
  direction: "after" | "before";
  /** Sort fields that structurally can never be null/missing. */
  nonNullable?: ReadonlySet<string>;
}): Record<string, unknown>[] {
  const fields = Object.keys(opts.sortObj);
  const isForward = opts.direction === "after";
  const branches: Record<string, unknown>[] = [];
  // Pins accumulate: branch i requires fields 0..i-1 equal to the anchor's.
  const pins: Record<string, unknown> = {};
  for (const field of fields) {
    const op = (opts.sortObj[field] === 1) === isForward ? "$gt" : "$lt";
    const anchorValue = getNestedValue(opts.anchorDoc, field);
    const rungs = cursorRungBranches(
      field,
      anchorValue,
      op,
      opts.nonNullable?.has(field) ?? false,
    );
    for (const rung of rungs) {
      branches.push({ ...pins, ...rung });
    }
    pins[field] = cursorRungEquality(anchorValue);
  }
  return branches;
}

/**
 * Compose the cursor DNF with the base filter parts into ONE rooted query:
 *
 *   { $or: [ { $and: [...baseParts, branch] }, ... ] }
 *
 * WHY rooted, with the base folded into every branch (measured, MongoDB
 * 8.0, 10k-doc scope, page of 25): only a TOP-LEVEL `$or` goes through the
 * subplanner, which plans each branch with tight index bounds and merge-sorts
 * them (totalKeysExamined ≈ 26). The same branches `$and`ed next to the base
 * (`{$and: [base, {$or: branches}]}`) lost the union bounds on descending
 * walks — the planner fell back to a full-range scan `[MaxKey, MinKey]` with
 * the whole `$or` as a residual FETCH filter, examining half the scope on
 * every page (measured totalKeysExamined 4951). The fold also keeps partial
 * `partialFilterExpression` indexes eligible per branch, since each branch
 * carries the `_type`/`_scope` constants as query operators.
 *
 * The empty-branch guard can only trigger if every rung was dropped — the
 * `_id` tie-break always survives, so it is defensive only.
 */
export function composeCursorQuery(
  baseParts: Record<string, unknown>[],
  branches: Record<string, unknown>[],
): Record<string, unknown> {
  if (branches.length === 0) return { _id: { $in: [] } };
  const parts = baseParts.filter((p) => Object.keys(p).length > 0);
  const folded = parts.length > 0
    ? branches.map((b) => ({ $and: [...parts, b] }))
    : branches;
  return folded.length === 1 ? folded[0] : { $or: folded };
}

/**
 * The cursor DNF as a standalone `$match` filter — for pipelines that must
 * apply it AFTER a computed stage (naturalIdSort's `_ulid`), where the base
 * match already ran. Index bounds are moot after an `$addFields`, so no base
 * folding here.
 */
export function composeCursorStageMatch(
  branches: Record<string, unknown>[],
): Record<string, unknown> {
  if (branches.length === 0) return { _id: { $in: [] } };
  return branches.length === 1 ? branches[0] : { $or: branches };
}
