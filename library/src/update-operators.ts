/**
 * Update operator sentinels: `increment`, `push`, `addToSet`, `pull`, `min`
 * and `max` are written as field values in a typed update, the way
 * `removeField()` is, and become `$inc`, `$push`, `$addToSet`, `$pull`,
 * `$min` and `$max`, each checked against the field's schema first.
 *
 * @example
 * ```typescript
 * await catalog.updateOne("product", id, {
 *   "usage.count": increment(1),
 *   tags: addToSet("sale"),
 *   lowestPrice: min(9.9),
 * });
 * ```
 *
 * @module
 */

import * as v from "./schema.ts";
import { isPartialUpdate, PARTIAL_UPDATE, REMOVE_FIELD } from "./sanitizer.ts";

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

/** The MongoDB operators a sentinel stands for. */
export type UpdateOperatorName =
  | "$inc"
  | "$push"
  | "$addToSet"
  | "$pull"
  | "$min"
  | "$max";

// A registry symbol so a sentinel made by another copy of mongodbee is still recognised.
const OPERATOR_BRAND = Symbol.for("mongodbee.updateOperator");

/**
 * A field value standing for an update operator. `V` is what the field must
 * accept: the delta of `increment`, an item of `push` / `addToSet` / `pull`,
 * the bound of `min` / `max`.
 */
export class UpdateOperator<
  Op extends UpdateOperatorName = UpdateOperatorName,
  V = unknown,
> {
  readonly [OPERATOR_BRAND] = true;
  readonly operator: Op;
  readonly operand: unknown;
  declare readonly accepts?: V;

  constructor(operator: Op, operand: unknown) {
    this.operator = operator;
    this.operand = operand;
    Object.freeze(this);
  }
}

/** Whether `value` is an update operator sentinel. */
export function isUpdateOperator(value: unknown): value is UpdateOperator {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<PropertyKey, unknown>)[OPERATOR_BRAND] === true
  );
}

/**
 * Adds `by` to a numeric field (`$inc`); a missing field starts from 0.
 * @example
 * await catalog.updateOne("product", id, { stock: increment(-1) });
 */
export function increment(by = 1): UpdateOperator<"$inc", number> {
  if (typeof by !== "number" || !Number.isFinite(by)) {
    throw new TypeError("increment() takes a finite number");
  }
  return new UpdateOperator("$inc", by);
}

/**
 * Appends items to an array field (`$push` with `$each`).
 * @example
 * await catalog.updateOne("order", id, { lines: push(line1, line2) });
 */
export function push<I>(...items: I[]): UpdateOperator<"$push", I> {
  return new UpdateOperator("$push", items);
}

/**
 * Appends the items an array field does not hold yet (`$addToSet` with `$each`).
 * @example
 * await catalog.updateOne("product", id, { tags: addToSet("sale") });
 */
export function addToSet<I>(...items: I[]): UpdateOperator<"$addToSet", I> {
  return new UpdateOperator("$addToSet", items);
}

/**
 * Removes from an array field every item equal to a value, or matching a
 * condition object (`$pull`).
 * @example
 * await catalog.updateOne("product", id, { tags: pull("sale") });
 * await catalog.updateOne("order", id, { lines: pull({ sku: "A-1" }) });
 * await catalog.updateOne("sensor", id, { readings: pull({ $lt: 0 }) });
 */
export function pull<I>(valueOrCondition: I): UpdateOperator<"$pull", I> {
  return new UpdateOperator("$pull", valueOrCondition);
}

/**
 * Lowers a field to `value` when `value` is smaller (`$min`).
 * @example
 * await catalog.updateOne("product", id, { lowestPrice: min(9.9) });
 */
export function min<V>(value: V): UpdateOperator<"$min", V> {
  return new UpdateOperator("$min", value);
}

/**
 * Raises a field to `value` when `value` is greater (`$max`).
 * @example
 * await catalog.updateOne("cursor", id, { seenUpTo: max(lastId) });
 */
export function max<V>(value: V): UpdateOperator<"$max", V> {
  return new UpdateOperator("$max", value);
}

type PullOperand<I> =
  | I
  | (I extends Record<string, unknown>
      ? Record<string, unknown>
      : { [operator: `$${string}`]: unknown });

/**
 * The sentinels a field of type `V` accepts: `increment` on numbers,
 * `push` / `addToSet` / `pull` on arrays, `min` / `max` on comparable values.
 */
export type OperatorFor<V> = V extends unknown
  ?
      | (V extends number ? UpdateOperator<"$inc", number> : never)
      | (V extends readonly (infer I)[]
          ?
              | UpdateOperator<"$push" | "$addToSet", I>
              | UpdateOperator<"$pull", PullOperand<I>>
          : never)
      | (V extends number | bigint | string | Date
          ? UpdateOperator<"$min" | "$max", V>
          : never)
  : never;

/** What an update document may hold at a field of type `V`. */
export type UpdateFieldValue<V> = V | symbol | OperatorFor<NonNullable<V>>;

/** An update document split by operator, before validation. */
export type SplitUpdate = {
  set: Record<string, unknown>;
  unset: Record<string, 1>;
  operators: Partial<Record<UpdateOperatorName, Record<string, unknown>>>;
};

/**
 * Splits a typed update document into `$set` values, `$unset` paths and the
 * sentinels' operands, by path. `partial()` objects are flattened to dot
 * paths; any other object is a whole value, so a sentinel inside one is
 * refused rather than stored.
 */
export function splitUpdate(
  doc: Record<string, unknown>,
  prefix = "",
  into: SplitUpdate = { set: {}, unset: {}, operators: {} },
): SplitUpdate {
  for (const [key, value] of Object.entries(doc)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (value === REMOVE_FIELD) {
      into.unset[path] = 1;
    } else if (isUpdateOperator(value)) {
      const fields = (into.operators[value.operator] ??= {});
      fields[path] = value.operand;
    } else if (isPartialUpdate(value)) {
      const clean = { ...(value as Record<string, unknown>) };
      delete clean[PARTIAL_UPDATE as unknown as string];
      splitUpdate(clean, path, into);
    } else {
      refuseNestedOperator(value, path);
      into.set[path] = value;
    }
  }
  return into;
}

/** Refuses a sentinel anywhere in `fields`, which only take plain values. */
export function refuseOperators(
  operation: string,
  fields: Record<string, unknown> | undefined,
): void {
  if (!fields) return;
  for (const [path, value] of Object.entries(fields)) {
    if (isUpdateOperator(value)) {
      throw new Error(
        `${operation}: "${path}" takes a plain value, not ${LABEL[value.operator]}`,
      );
    }
    refuseNestedOperator(value, path);
  }
}

function refuseNestedOperator(value: unknown, path: string): void {
  if (isUpdateOperator(value)) {
    throw new Error(
      `"${path}": an update operator must be the value of a field path; write "${path}" as a dot path or wrap its parent in partial()`,
    );
  }
  if (Array.isArray(value)) {
    for (const [index, item] of value.entries()) {
      refuseNestedOperator(item, `${path}.${index}`);
    }
  } else if (
    typeof value === "object" &&
    value !== null &&
    value.constructor === Object
  ) {
    for (const [key, item] of Object.entries(value)) {
      refuseNestedOperator(item, `${path}.${key}`);
    }
  }
}

type SchemaNode = AnySchema & {
  readonly type: string;
  readonly wrapped?: AnySchema;
  readonly options?: readonly unknown[];
  readonly entries?: Record<string, AnySchema>;
  readonly rest?: AnySchema;
  readonly value?: AnySchema;
  readonly item?: AnySchema;
};

const WRAPPERS = new Set([
  "optional",
  "exact_optional",
  "nullable",
  "nullish",
  "undefinedable",
  "non_optional",
  "non_nullable",
  "non_nullish",
]);

const OPAQUE = new Set(["unknown", "any", "custom", "lazy"]);

const ARRAY_SEGMENT = /^(\d+|\$|\$\[[^\]]*\])$/;

function alternatives(schema: AnySchema): SchemaNode[] {
  const node = schema as SchemaNode;
  if (WRAPPERS.has(node.type) && node.wrapped) {
    return alternatives(node.wrapped);
  }
  if (
    (node.type === "union" ||
      node.type === "variant" ||
      node.type === "intersect") &&
    node.options
  ) {
    return (node.options as AnySchema[]).flatMap(alternatives);
  }
  return [node];
}

function childrenOf(node: SchemaNode, segment: string): AnySchema[] {
  if (OPAQUE.has(node.type)) return [node];
  if (node.entries) {
    if (Object.hasOwn(node.entries, segment)) return [node.entries[segment]];
    return node.rest ? [node.rest] : [];
  }
  if (node.type === "record" && node.value) return [node.value];
  if (node.type === "array" && node.item) {
    return ARRAY_SEGMENT.test(segment) ? [node.item] : [];
  }
  return [];
}

/**
 * The schemas a dot path can land on: one per union branch it resolves in.
 * Empty when the path is unknown to the schema.
 */
export function schemasAtPath(root: AnySchema, path: string): SchemaNode[] {
  let current: AnySchema[] = [root];
  for (const segment of path.split(".")) {
    current = current
      .flatMap(alternatives)
      .flatMap((node) => childrenOf(node, segment));
    if (current.length === 0) return [];
  }
  return current.flatMap(alternatives);
}

/** Throws unless one of `schemas` accepts `value`, with the first one's issues. */
export function parseAgainst(
  schemas: readonly AnySchema[],
  value: unknown,
): void {
  if (schemas.some((schema) => v.safeParse(schema, value).success)) return;
  v.parse(schemas[0], value);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

const LABEL: Record<UpdateOperatorName, string> = {
  $inc: "increment()",
  $push: "push()",
  $addToSet: "addToSet()",
  $pull: "pull()",
  $min: "min()",
  $max: "max()",
};

function itemSchemasAt(
  operation: string,
  operator: UpdateOperatorName,
  root: AnySchema,
  path: string,
): AnySchema[] | undefined {
  const nodes = schemasAtPath(root, path);
  if (nodes.length === 0 || nodes.some((node) => OPAQUE.has(node.type))) {
    return undefined;
  }
  const items = nodes
    .filter((node) => node.type === "array" && node.item)
    .map((node) => node.item as AnySchema);
  if (items.length === 0) {
    throw new Error(
      `${operation}: ${LABEL[operator]} needs an array field, "${path}" is not one`,
    );
  }
  return items;
}

/**
 * Checks each sentinel operand against the schema at its path: `increment`
 * needs a field that accepts numbers, `push` / `addToSet` items and a plain
 * `pull` value must be valid items of the array, `min` / `max` bounds valid
 * values of the field. A path the schema does not describe is left to the
 * collection validator, as a `$set` on it is.
 */
export function checkOperators(
  operation: string,
  root: AnySchema,
  operators: SplitUpdate["operators"],
): void {
  for (const [operator, fields] of Object.entries(operators) as [
    UpdateOperatorName,
    Record<string, unknown>,
  ][]) {
    for (const [path, operand] of Object.entries(fields)) {
      switch (operator) {
        case "$inc": {
          if (typeof operand !== "number" || !Number.isFinite(operand)) {
            throw new TypeError(
              `${operation}: increment() on "${path}" takes a finite number`,
            );
          }
          const nodes = schemasAtPath(root, path);
          if (
            nodes.length > 0 &&
            !nodes.some(
              (node) =>
                node.type === "number" ||
                OPAQUE.has(node.type) ||
                (node.type === "picklist" &&
                  node.options?.some((option) => typeof option === "number")),
            )
          ) {
            throw new Error(
              `${operation}: increment() needs a numeric field, "${path}" is not one`,
            );
          }
          break;
        }
        case "$push":
        case "$addToSet": {
          const items = itemSchemasAt(operation, operator, root, path);
          if (items) {
            for (const item of operand as unknown[]) parseAgainst(items, item);
          }
          break;
        }
        case "$pull": {
          const items = itemSchemasAt(operation, operator, root, path);
          if (items && !isPlainObject(operand)) parseAgainst(items, operand);
          break;
        }
        case "$min":
        case "$max": {
          const nodes = schemasAtPath(root, path);
          if (nodes.length > 0) parseAgainst(nodes, operand);
          break;
        }
      }
    }
  }
}

/**
 * Moves the sentinels of a `$set` into their own operators, next to the
 * ones the update already names; `removeField()` values join `$unset`.
 * `operators` holds the sentinels' operands, for {@link checkOperators}.
 */
export function liftSetOperators(
  operation: string,
  update: Record<string, unknown>,
): { update: Record<string, unknown>; operators: SplitUpdate["operators"] } {
  for (const [key, fields] of Object.entries(update)) {
    if (key !== "$set" && isPlainObject(fields)) {
      refuseOperators(operation, fields);
    }
  }
  if (!isPlainObject(update.$set)) {
    return { update: { ...update }, operators: {} };
  }
  const { set, unset, operators } = splitUpdate(update.$set);
  const result: Record<string, unknown> = { ...update };
  if (Object.keys(set).length > 0) result.$set = set;
  else delete result.$set;
  if (Object.keys(unset).length > 0) {
    result.$unset = {
      ...(isPlainObject(update.$unset) ? update.$unset : {}),
      ...unset,
    };
  }
  for (const [operator, fields] of Object.entries(
    operatorDocuments(operators, (value) => value),
  )) {
    const existing = isPlainObject(result[operator])
      ? (result[operator] as Record<string, unknown>)
      : {};
    for (const path of Object.keys(fields)) {
      if (path in existing) {
        throw new Error(
          `${operation}: "${path}" is written twice in one update`,
        );
      }
    }
    result[operator] = { ...existing, ...fields };
  }
  return { update: result, operators };
}

/** The MongoDB value of each sentinel operand. */
export function operatorDocuments(
  operators: SplitUpdate["operators"],
  sanitize: (fields: Record<string, unknown>) => Record<string, unknown>,
): Record<string, Record<string, unknown>> {
  const ops: Record<string, Record<string, unknown>> = {};
  for (const [operator, fields] of Object.entries(operators) as [
    UpdateOperatorName,
    Record<string, unknown>,
  ][]) {
    if (Object.keys(fields).length === 0) continue;
    const clean = sanitize(fields);
    if (operator === "$push" || operator === "$addToSet") {
      ops[operator] = Object.fromEntries(
        Object.entries(clean).map(([path, items]) => [path, { $each: items }]),
      );
    } else {
      ops[operator] = clean;
    }
  }
  return ops;
}

function overlapping(a: string, b: string): boolean {
  return a === b || a.startsWith(`${b}.`) || b.startsWith(`${a}.`);
}

/**
 * Refuses an update that writes one path, or a path and one of its parents,
 * twice: MongoDB rejects it with a conflict error only once it reaches the
 * server.
 */
export function assertDisjointPaths(
  operation: string,
  paths: readonly string[],
): void {
  for (let i = 0; i < paths.length; i++) {
    for (let j = i + 1; j < paths.length; j++) {
      if (overlapping(paths[i], paths[j])) {
        throw new Error(
          paths[i] === paths[j]
            ? `${operation}: "${paths[i]}" is written twice in one update`
            : `${operation}: "${paths[i]}" and "${paths[j]}" overlap; one update cannot write a field and its parent`,
        );
      }
    }
  }
}
