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

const ASCENDING_DIRECTIONS: readonly unknown[] = [1, "asc", "ascending"];
const DESCENDING_DIRECTIONS: readonly unknown[] = [-1, "desc", "descending"];

/** A scalar the driver accepts as a sort direction. */
function isSortDirection(value: unknown): boolean {
  return ASCENDING_DIRECTIONS.includes(value) ||
    DESCENDING_DIRECTIONS.includes(value);
}

/**
 * A direction value to `1 | -1`, or a LOUD error. Anything else used to fall
 * through an `input === 1 ? 1 : -1` ternary: `{name: "asc"}` became a
 * DESCENDING ladder while the find path sorted ascending — from page 2 the
 * walk ran backward and dead-ended (measured: a 5-doc walk returned
 * `alpha, bravo, alpha`). `$meta` sorts are refused explicitly: the cursor
 * ladder needs comparable stored values, which a computed relevance score is
 * not.
 */
function normalizeSortDirection(value: unknown, field: string): 1 | -1 {
  if (ASCENDING_DIRECTIONS.includes(value)) return 1;
  if (DESCENDING_DIRECTIONS.includes(value)) return -1;
  if (value !== null && typeof value === "object" && "$meta" in value) {
    throw new Error(
      `paginate: cannot sort on a $meta expression (field "${field}") — ` +
        `the cursor ladder needs comparable stored values`,
    );
  }
  throw new Error(
    `paginate: invalid sort direction ${JSON.stringify(value)} for field ` +
      `"${field}" — expected 1, -1, "asc", "ascending", "desc" or "descending"`,
  );
}

/**
 * Normalize a paginate `sort` option into the effective sort spec, appending
 * the `_id` tie-break when absent so duplicate sort values keep a stable
 * (cursor-safe) order.
 *
 * Accepts every `m.Sort` form the option's type admits, mirroring the
 * driver's `formatSort`. This used to accept only plain `{field: 1|-1}`
 * objects and misread everything else as an `_id` direction: `sort: "name"`
 * (a legal m.Sort meaning `{name: 1}`) silently paginated by `{_id: -1}` —
 * the requested order ignored, no error anywhere. Unrecognized forms now
 * fail loud instead of walking a wrong-but-believable order.
 *
 * The tie-break follows the direction of the LAST explicit sort field —
 * NOT a fixed `1`. An index `{<field>: 1, _id: 1}` serves a sort only in its
 * own order or its exact reverse; `{field: -1, _id: 1}` matches neither, so a
 * fixed ascending tie-break silently turned EVERY descending page (cursor or
 * not) into a full blocking sort of the filtered set (measured: 2000 keys
 * examined for a page of 25 on a 2k scope, from page 1).
 */
export function normalizePaginateSort(sort: unknown): Record<string, 1 | -1> {
  const sortObj: Record<string, 1 | -1> = {};
  // Falsy (`undefined`, `0`, `""`) keeps its historical "unspecified" meaning.
  const input = sort || { _id: 1 };
  if (typeof input === "string" && !isSortDirection(input)) {
    // Driver semantics: a bare field name sorts ascending.
    sortObj[input] = 1;
  } else if (isSortDirection(input)) {
    sortObj._id = normalizeSortDirection(input, "_id");
  } else if (input instanceof Map) {
    for (const [field, dir] of input.entries()) {
      sortObj[String(field)] = normalizeSortDirection(dir, String(field));
    }
  } else if (Array.isArray(input)) {
    if (
      input.length === 2 && typeof input[0] === "string" &&
      isSortDirection(input[1])
    ) {
      // Single `[field, direction]` pair (driver disambiguates exactly so).
      sortObj[input[0]] = normalizeSortDirection(input[1], input[0]);
    } else {
      for (const entry of input) {
        if (typeof entry === "string") {
          sortObj[entry] = 1;
        } else if (Array.isArray(entry) && typeof entry[0] === "string") {
          sortObj[entry[0]] = normalizeSortDirection(entry[1], entry[0]);
        } else {
          throw new Error(
            `paginate: invalid sort entry ${JSON.stringify(entry)} — ` +
              `expected a field name or a [field, direction] pair`,
          );
        }
      }
    }
  } else if (typeof input === "object") {
    for (const [field, dir] of Object.entries(input)) {
      sortObj[field] = normalizeSortDirection(dir, field);
    }
  } else {
    throw new Error(
      `paginate: invalid sort ${JSON.stringify(input)}`,
    );
  }
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
 * BSON comparison brackets in `$sort` order, as query-side `$type` aliases.
 * Null/missing rank between MinKey and the first bracket and are handled by
 * dedicated branches; MinKey/MaxKey stored as VALUES are out of scope (they
 * are query sentinels — a field holding one cannot anchor a page, and is not
 * defended against elsewhere in the ladder).
 *
 * The array bracket (between object and binData) is deliberately ABSENT:
 * array-valued sort fields are outside the ladder's contract (an array
 * anchor throws — see bsonBracketIndex), and `{$type: "array"}` cannot take
 * tight index bounds — its presence forced a `[MinKey, MaxKey]` scan of the
 * whole set on every page (measured: 10 026 keys vs 27 for a page of 25).
 */
const BSON_TYPE_BRACKETS: readonly (readonly string[])[] = [
  ["number"], // int, long, double, decimal — cross-compare numerically
  ["string", "symbol"],
  ["object"],
  ["binData"],
  ["objectId"],
  ["bool"],
  ["date"],
  ["timestamp"],
  ["regex"],
];

/**
 * Bracket index of an anchor value, or `null` for the null/missing block.
 * Throws for values that CANNOT participate in a query-operator ladder:
 *
 * - arrays: a query predicate on an array field matches per ELEMENT while
 *   `$sort` ranks the array by its min (asc) / max (desc) element — no
 *   query-operator ladder can agree with the sort, so an array anchor fails
 *   loud instead of silently corrupting every following page;
 * - regexes: `{f: <regex>}` — the shape a ladder equality pin would take —
 *   is a pattern MATCH against strings, not an equality;
 * - MinKey/MaxKey/Code/anything unrecognized: exotic sentinels, refused
 *   rather than guessed.
 */
function bsonBracketIndex(value: unknown): number | null {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) {
    throw new Error(
      "paginate: cannot anchor a cursor on an ARRAY sort value — query " +
        "operators match array fields per element while $sort ranks the " +
        "array by its min/max element, so no cursor can agree with the sort",
    );
  }
  switch (typeof value) {
    case "number":
    case "bigint":
      return 0;
    case "string":
      return 1;
    case "boolean":
      return 5;
    case "object":
      break;
    default:
      throw new Error(
        `paginate: cannot anchor a cursor on a ${typeof value} sort value`,
      );
  }
  if (value instanceof Date) return 6;
  if (value instanceof RegExp) {
    throw new Error(
      "paginate: cannot anchor a cursor on a REGEX sort value — a query " +
        "equality on a regex is a pattern match, not a comparison",
    );
  }
  const bsonType = (value as { _bsontype?: string })._bsontype;
  if (bsonType === undefined) return 2; // plain embedded document
  switch (bsonType) {
    case "Int32":
    case "Long":
    case "Double":
    case "Decimal128":
      return 0;
    case "BSONSymbol":
      return 1;
    case "DBRef":
      return 2; // stored as a {$ref, $id} subdocument
    case "Binary":
      return 3;
    case "ObjectId":
    case "ObjectID":
      return 4;
    case "Timestamp":
      return 7;
    case "BSONRegExp":
      throw new Error(
        "paginate: cannot anchor a cursor on a REGEX sort value — a query " +
          "equality on a regex is a pattern match, not a comparison",
      );
    default:
      throw new Error(
        `paginate: cannot anchor a cursor on a BSON ${bsonType} sort value`,
      );
  }
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
 * `$gt`/`$lt` are also TYPE-BRACKETED: they never compare across BSON type
 * brackets, while `$sort` ranks the brackets (numbers < strings < objects <
 * …). A field that ever held two types — a schema migration away on any
 * real dataset — silently lost every document of the other brackets from the
 * walk. Each value rung therefore carries a `$type` branch for the brackets
 * ranked past the anchor's.
 *
 * The shapes agree with `$sort` across every boundary while staying query
 * operators, so the sort index remains usable (unlike the `$expr` ladder the
 * sortPipeline path needs — `$expr` comparisons are cross-type by nature):
 *
 *   above null      → {f: {$ne: null}}          (every value of every type)
 *   above a value   → {f: {$gt: v}} ∪ {f: {$type: brackets above}}
 *   below a value   → {f: {$lt: v}} ∪ {f: {$type: brackets below}} ∪ {f: null}
 *   below null      → nothing — no branch, nothing ranks lower
 *
 * `nonNullable` marks fields that structurally always exist AND hold one
 * type (`_id`, `_ulid`, `_scope`, `_type`): their rungs stay raw
 * comparisons, sparing the planner dead branches.
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
  if (nonNullable) {
    return [{ [field]: { [op]: anchorValue } }];
  }
  const bracket = bsonBracketIndex(anchorValue);
  // NaN sits at the BOTTOM of the number bracket for `$sort`, but every
  // range comparison against NaN matches nothing (measured: `{$gt: NaN}` is
  // empty, `{$lt: v}` skips NaN). Equality on NaN works, so pins are fine.
  const anchorIsNaN = typeof anchorValue === "number" &&
    Number.isNaN(anchorValue);
  if (op === "$gt") {
    if (bracket === null) return [{ [field]: { $ne: null } }];
    const above = BSON_TYPE_BRACKETS.slice(bracket + 1).flat();
    return [
      // Above NaN = every non-NaN number ({$gte: -Infinity} excludes NaN,
      // includes -Infinity — which ranks above NaN).
      anchorIsNaN
        ? { [field]: { $gte: -Infinity } }
        : { [field]: { $gt: anchorValue } },
      ...(above.length > 0 ? [{ [field]: { $type: above } }] : []),
    ];
  }
  if (bracket === null) return [];
  const below = BSON_TYPE_BRACKETS.slice(0, bracket).flat();
  return [
    { [field]: { $lt: anchorValue } },
    // Below a non-NaN number still contains NaN, which `$lt` never matches.
    ...(bracket === 0 && !anchorIsNaN ? [{ [field]: NaN }] : []),
    ...(below.length > 0 ? [{ [field]: { $type: below } }] : []),
    { [field]: null },
  ];
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
