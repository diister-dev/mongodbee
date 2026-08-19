import type {
  DatabaseState,
  MigrationDefinition,
  MigrationRule,
} from "../types.ts";
import {
  extractIdPrefix,
  flowDocumentPrefix,
  flowScopeTargetId,
  flowTargetId,
  resolveSeedId,
} from "../utils/seed-id.ts";
import { getIrreversibleOperations } from "../builder.ts";

/** Field-level `where` operators the simulation understands. */
const SUPPORTED_OPERATORS =
  "$eq, $ne, $in, $nin, $gt, $gte, $lt, $lte, $exists";

/**
 * Read a possibly dotted path (`"a.b.c"`) from a document, so a `where` using
 * nested field access matches the same documents the MongoDB applier would.
 * Returns `undefined` for any missing segment.
 */
/**
 * The simulation bucket holding `name`, whichever kind of collection it is.
 *
 * A migration addresses a collection by its physical name, and MongoDB stores
 * every kind in one namespace; the simulation splits them into three buckets.
 * Resolving that in one place is what stops an operation from silently
 * supporting only the first kind: `flow` looked its endpoints up in
 * `collections` alone, so a target the builder had just accepted as "a plain
 * collection, a multi-collection, or a scoped multi-collection" was refused by
 * the validator while the mongodb applier wrote it without trouble.
 *
 * Returns `undefined` when the name is unknown, so each caller keeps deciding
 * whether that is an error or a collection to create.
 */
function resolveStateCollection(
  state: DatabaseState,
  name: string,
): { content: Record<string, unknown>[] } | undefined {
  return state.collections[name] ??
    state.multiCollections[name] ??
    state.scopedMultiCollections[name];
}

function getFieldByPath(doc: Record<string, unknown>, path: string): unknown {
  if (!path.includes(".")) return doc[path];
  let current: unknown = doc;
  for (const part of path.split(".")) {
    if (current === null || current === undefined) return undefined;
    if (typeof current !== "object") return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

/** Structural equality for the JSON-shaped data these migrations deal in. */
function valuesEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null || a === undefined || b === undefined) {
    return a === b;
  }
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }
  if (typeof a !== "object" || typeof b !== "object") return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) {
      return false;
    }
    return a.every((v, i) => valuesEqual(v, b[i]));
  }
  const ao = a as Record<string, unknown>;
  const bo = b as Record<string, unknown>;
  const ak = Object.keys(ao);
  const bk = Object.keys(bo);
  if (ak.length !== bk.length) return false;
  return ak.every((k) => valuesEqual(ao[k], bo[k]));
}

/**
 * True when `cond` is a Mongo operator expression — a plain object whose keys
 * ALL begin with `$` (e.g. `{ $in: [...] }`). A plain object with non-`$` keys
 * is a nested-equality match, not an operator expression.
 */
function isOperatorExpression(cond: unknown): cond is Record<string, unknown> {
  if (cond === null || typeof cond !== "object" || Array.isArray(cond)) {
    return false;
  }
  const keys = Object.keys(cond);
  return keys.length > 0 && keys.every((k) => k.startsWith("$"));
}

/** Ordering comparison for `$gt`/`$gte`/`$lt`/`$lte`; `undefined` if incomparable. */
function compareValues(a: unknown, b: unknown): number | undefined {
  if (typeof a === "number" && typeof b === "number") return a - b;
  if (typeof a === "string" && typeof b === "string") {
    return a < b ? -1 : a > b ? 1 : 0;
  }
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }
  return undefined;
}

/**
 * Apply a single field-level operator. THROWS on any `$`-operator the
 * simulation does not implement — failing loud rather than silently matching
 * nothing (which would let a dry-run disagree with production).
 */
function applyOperator(
  fieldValue: unknown,
  op: string,
  operand: unknown,
): boolean {
  switch (op) {
    case "$eq":
      return valuesEqual(fieldValue, operand);
    case "$ne":
      return !valuesEqual(fieldValue, operand);
    case "$in":
      return Array.isArray(operand) &&
        operand.some((o) => valuesEqual(fieldValue, o));
    case "$nin":
      return Array.isArray(operand) &&
        !operand.some((o) => valuesEqual(fieldValue, o));
    case "$gt": {
      const c = compareValues(fieldValue, operand);
      return c !== undefined && c > 0;
    }
    case "$gte": {
      const c = compareValues(fieldValue, operand);
      return c !== undefined && c >= 0;
    }
    case "$lt": {
      const c = compareValues(fieldValue, operand);
      return c !== undefined && c < 0;
    }
    case "$lte": {
      const c = compareValues(fieldValue, operand);
      return c !== undefined && c <= 0;
    }
    case "$exists":
      return (fieldValue !== undefined) === Boolean(operand);
    default:
      throw new Error(
        `Operator "${op}" is not supported in simulation (memory applier). ` +
          `Supported operators: ${SUPPORTED_OPERATORS}.`,
      );
  }
}

/**
 * Match a document against an in-memory `where` filter with Mongo-like
 * semantics: nested dot-paths, and the common field operators ($eq, $ne, $in,
 * $nin, $gt, $gte, $lt, $lte, $exists). Any other `$`-operator — including a
 * top-level logical operator such as `$and`/`$or` — throws, so the simulation
 * never silently disagrees with what the real database would move or delete.
 */
function matchesWhere(
  doc: Record<string, unknown>,
  where?: Record<string, unknown>,
): boolean {
  if (!where) return true;
  return Object.entries(where).every(([key, condition]) => {
    if (key.startsWith("$")) {
      throw new Error(
        `Operator "${key}" is not supported in simulation (memory applier). ` +
          `Supported field operators: ${SUPPORTED_OPERATORS}.`,
      );
    }
    const fieldValue = getFieldByPath(doc, key);
    if (isOperatorExpression(condition)) {
      return Object.entries(condition).every(([op, operand]) =>
        applyOperator(fieldValue, op, operand)
      );
    }
    return valuesEqual(fieldValue, condition);
  });
}

export function createMemoryApplier(migration: MigrationDefinition) {
  const migrationId = migration?.id ?? "unknown";

  /**
   * Resolve the `_id` for a seed document: honour an explicit `_id`,
   * otherwise derive a deterministic one so apply/reverse agree.
   */
  function seedId(
    doc: Record<string, unknown>,
    schemaIdField: unknown,
    fallbackPrefix: string,
    opSignature: string,
    docIndex: number,
  ): string {
    return resolveSeedId(
      doc,
      schemaIdField,
      fallbackPrefix,
      migrationId,
      opSignature,
      docIndex,
    );
  }

  /** Move a physical collection across whichever bucket currently holds it. */
  function renamePhysical(
    state: DatabaseState,
    from: string,
    to: string,
    dropTarget: boolean,
  ): DatabaseState {
    if (dropTarget) {
      delete state.collections[to];
      delete state.multiCollections[to];
      delete state.multiModels[to];
      delete state.scopedMultiCollections[to];
    }
    if (state.collections[from]) {
      state.collections[to] = state.collections[from];
      delete state.collections[from];
    } else if (state.multiCollections[from]) {
      state.multiCollections[to] = state.multiCollections[from];
      delete state.multiCollections[from];
    } else if (state.multiModels[from]) {
      state.multiModels[to] = state.multiModels[from];
      delete state.multiModels[from];
    } else if (state.scopedMultiCollections[from]) {
      state.scopedMultiCollections[to] = state.scopedMultiCollections[from];
      delete state.scopedMultiCollections[from];
    }
    return state;
  }

  const migrations: {
    [K in MigrationRule["type"]]: {
      apply: (
        state: DatabaseState,
        operation: Extract<MigrationRule, { type: K }>,
      ) => DatabaseState | Promise<DatabaseState>;
      reverse: (
        state: DatabaseState,
        operation: Extract<MigrationRule, { type: K }>,
      ) => DatabaseState | Promise<DatabaseState>;
    };
  } = {
    create_collection: {
      apply: (state, operation) => {
        state.collections[operation.collectionName] = { content: [] };
        return state;
      },
      reverse: (state, operation) => {
        delete state.collections[operation.collectionName];
        return state;
      },
    },
    rename_collection: {
      apply: (state, operation) =>
        renamePhysical(
          state,
          operation.from,
          operation.to,
          operation.dropTarget ?? false,
        ),
      reverse: (state, operation) =>
        renamePhysical(state, operation.to, operation.from, false),
    },
    create_multicollection: {
      apply: (state, operation) => {
        state.multiCollections[operation.collectionName] = { content: [] };
        return state;
      },
      reverse: (state, operation) => {
        delete state.multiCollections[operation.collectionName];
        return state;
      },
    },
    create_multimodel_instance: {
      apply: (state, operation) => {
        state.multiModels[operation.collectionName] ??= {
          modelType: operation.modelType,
          content: [],
        };
        return state;
      },
      reverse: (state, operation) => {
        delete state.multiModels[operation.collectionName];
        return state;
      },
    },
    mark_as_multimodel: {
      apply: (state, operation) => {
        const original = state.collections[operation.collectionName];
        if (original) {
          state.multiModels[operation.collectionName] = {
            modelType: operation.modelType,
            content: original.content,
          };
          delete state.collections[operation.collectionName];
        }
        return state;
      },
      reverse: (state, operation) => {
        const multiModel = state.multiModels[operation.collectionName];
        if (multiModel) {
          state.collections[operation.collectionName] = {
            content: multiModel.content,
          };
          delete state.multiModels[operation.collectionName];
        }
        return state;
      },
    },
    seed_collection: {
      apply: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if (!collection) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }
        const sig = operation.collectionName;
        collection.content.push(
          ...operation.documents.map((doc: unknown, i) => {
            const typedDoc = doc as Record<string, unknown>;
            return {
              ...typedDoc,
              _id: seedId(typedDoc, operation.schema._id, "", sig, i),
            };
          }),
        );
        return state;
      },
      reverse: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if (!collection) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }
        const sig = operation.collectionName;
        // Recompute the exact ids that were inserted, then filter them out.
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(
              doc as Record<string, unknown>,
              operation.schema._id,
              "",
              sig,
              i,
            )
          ),
        );
        collection.content = collection.content.filter(
          (doc) => !seededIds.has(String(doc._id)),
        );
        return state;
      },
    },
    seed_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection =
          state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const sig = `${operation.collectionName}:${operation.documentType}`;
        multiCollection.content.push(
          ...operation.documents.map((doc: unknown, i) => {
            const typedDoc = doc as Record<string, unknown>;
            return {
              ...typedDoc,
              _id: seedId(
                typedDoc,
                operation.schema._id,
                operation.documentType,
                sig,
                i,
              ),
              _type: operation.documentType,
            };
          }),
        );
        return state;
      },
      reverse: (state, operation) => {
        const multiCollection =
          state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const sig = `${operation.collectionName}:${operation.documentType}`;
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(
              doc as Record<string, unknown>,
              operation.schema._id,
              operation.documentType,
              sig,
              i,
            )
          ),
        );
        multiCollection.content = multiCollection.content.filter(
          (doc) => !seededIds.has(String(doc._id)),
        );
        return state;
      },
    },
    seed_multimodel_instance_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }
        const sig =
          `${operation.collectionName}:${operation.modelType}:${operation.documentType}`;
        multiCollection.content.push(
          ...operation.documents.map((doc: unknown, i) => {
            const typedDoc = doc as Record<string, unknown>;
            return {
              ...typedDoc,
              _id: seedId(
                typedDoc,
                operation.schema._id,
                operation.documentType,
                sig,
                i,
              ),
              _type: operation.documentType,
            };
          }),
        );
        return state;
      },
      reverse: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }
        const sig =
          `${operation.collectionName}:${operation.modelType}:${operation.documentType}`;
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(
              doc as Record<string, unknown>,
              operation.schema._id,
              operation.documentType,
              sig,
              i,
            )
          ),
        );
        multiCollection.content = multiCollection.content.filter(
          (doc) => !seededIds.has(String(doc._id)),
        );
        return state;
      },
    },
    seed_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        const sig = `${operation.modelType}:${operation.documentType}`;
        for (
          const [_instanceName, instance] of Object.entries(state.multiModels)
        ) {
          if (instance.modelType === modelType) {
            instance.content.push(
              ...operation.documents.map((doc: unknown, i) => {
                const typedDoc = doc as Record<string, unknown>;
                return {
                  ...typedDoc,
                  _id: seedId(
                    typedDoc,
                    operation.schema._id,
                    operation.documentType,
                    sig,
                    i,
                  ),
                  _type: operation.documentType,
                };
              }),
            );
          }
        }
        return state;
      },
      reverse: (state, operation) => {
        const modelType = operation.modelType;
        const sig = `${operation.modelType}:${operation.documentType}`;
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(
              doc as Record<string, unknown>,
              operation.schema._id,
              operation.documentType,
              sig,
              i,
            )
          ),
        );
        for (
          const [_instanceName, instance] of Object.entries(state.multiModels)
        ) {
          if (instance.modelType === modelType) {
            instance.content = instance.content.filter(
              (doc) => !seededIds.has(String(doc._id)),
            );
          }
        }
        return state;
      },
    },
    transform_collection: {
      apply: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if (!collection) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }
        collection.content = collection.content.map(
          operation.up as (
            doc: Record<string, unknown>,
          ) => Record<string, unknown>,
        );
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const collection = state.collections[operation.collectionName];
        if (!collection) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }
        collection.content = collection.content.map(
          operation.down as (
            doc: Record<string, unknown>,
          ) => Record<string, unknown>,
        );
        return state;
      },
    },
    transform_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection =
          state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        multiCollection.content = multiCollection.content.map((doc) => {
          if (doc._type === operation.documentType) {
            return operation.up(doc as Record<string, unknown>);
          }
          return doc;
        });
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const multiCollection =
          state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        multiCollection.content = multiCollection.content.map((doc) => {
          if (doc._type === operation.documentType) {
            return operation.down(doc as Record<string, unknown>);
          }
          return doc;
        });
        return state;
      },
    },
    transform_multimodel_instance_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }
        multiCollection.content = multiCollection.content.map((doc) => {
          if (doc._type === operation.documentType) {
            return operation.up(doc as Record<string, unknown>);
          }
          return doc;
        });
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const multiCollection = state.multiModels[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }
        multiCollection.content = multiCollection.content.map((doc) => {
          if (doc._type === operation.documentType) {
            return operation.down(doc as Record<string, unknown>);
          }
          return doc;
        });
        return state;
      },
    },
    transform_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        for (
          const [_instanceName, instance] of Object.entries(state.multiModels)
        ) {
          if (instance.modelType === modelType) {
            instance.content = instance.content.map((doc) => {
              if (doc._type === operation.documentType) {
                return operation.up(doc as Record<string, unknown>);
              }
              return doc;
            });
          }
        }
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const modelType = operation.modelType;
        for (
          const [_instanceName, instance] of Object.entries(state.multiModels)
        ) {
          if (instance.modelType === modelType) {
            instance.content = instance.content.map((doc) => {
              if (doc._type === operation.documentType) {
                return operation.down(doc as Record<string, unknown>);
              }
              return doc;
            });
          }
        }
        return state;
      },
    },
    flow: {
      apply: (state, operation) => {
        const src = resolveStateCollection(state, operation.from.collection);
        if (!src) {
          throw new Error(
            `Flow source collection ${operation.from.collection} does not exist`,
          );
        }
        const tgt = resolveStateCollection(state, operation.into.collection);
        if (!tgt) {
          throw new Error(
            `Flow target collection ${operation.into.collection} does not exist`,
          );
        }

        const prefix = extractIdPrefix(operation.targetIdSchema, "");
        const matched = src.content.filter((doc) =>
          matchesWhere(doc, operation.from.where)
        );

        for (const doc of matched) {
          const mapped = operation.map({ ...doc }) as Record<string, unknown>;
          mapped._id = flowTargetId(
            flowDocumentPrefix(operation.targetIsTyped, prefix, mapped),
            migrationId,
            operation.from.collection,
            String(doc._id),
          );
          tgt.content.push(mapped);
        }

        if (operation.sourceDisposition === "consume") {
          src.content = src.content.filter(
            (doc) => !matchesWhere(doc, operation.from.where),
          );
        }
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(
            "Flow with source: 'consume' (move) is irreversible — cannot roll back",
          );
        }
        const src = resolveStateCollection(state, operation.from.collection);
        const tgt = resolveStateCollection(state, operation.into.collection);
        if (!src || !tgt) {
          throw new Error(`Flow collections missing for reverse`);
        }
        // Copy reverse: recompute target ids from the still-present source.
        const prefix = extractIdPrefix(operation.targetIdSchema, "");
        const targetIds = new Set(
          src.content
            .filter((doc) => matchesWhere(doc, operation.from.where))
            .map((doc) =>
              flowTargetId(
                flowDocumentPrefix(
                  operation.targetIsTyped,
                  prefix,
                  operation.map({ ...doc }) as Record<string, unknown>,
                ),
                migrationId,
                operation.from.collection,
                String(doc._id),
              )
            ),
        );
        tgt.content = tgt.content.filter(
          (doc) => !targetIds.has(String(doc._id)),
        );
        return state;
      },
    },
    flow_to_scope: {
      /**
       * Simulate routing documents into a scoped multi-collection. Kept in
       * lockstep with the mongodb applier:
       * - conflicts are keyed on the target `_id` ALONE (the primary key of the
       *   single physical scoped collection), not on `(_scope, _type, _id)`;
       * - a minted target id (when `map` drops `_id`) is DETERMINISTIC, so a
       *   replay lands on the same id instead of a fresh random one;
       * - `source: "consume"` removes only the source docs that ACTUALLY landed
       *   (inserted/merged). With `onConflict: "skip"`, skipped docs never land,
       *   so the whole-source drop fast path is used only when nothing was
       *   skipped — otherwise skipped docs are left in the source.
       *
       * Skip/landed bookkeeping is tracked PER SOURCE — one instance's skipped
       * doc must not stop a sibling instance from being consumed away. The
       * mongodb applier processes each discovered source independently, so a
       * `skip` in instance A drops only A's landed docs while a clean instance B
       * is still dropped whole; the simulation mirrors that per-source decision.
       *
       * Idempotent replay: deterministic ids make a retry land on the same id,
       * but under the DEFAULT `onConflict: "error"` a replay throws a conflict on
       * a doc a crashed run already flowed. Clean re-application (retry / C8
       * catch-up) therefore requires `onConflict: "skip"` or `"merge"`.
       */
      apply: (state, operation) => {
        const target =
          (state.scopedMultiCollections[operation.into.collection] ??= {
            content: [],
          });

        // Resolve the concrete source(s) and the docs each contributes. Each
        // source carries `dropWhole` — how to consume it when nothing was
        // skipped (drop the whole collection / instance key). `undefined` means
        // a filtered subset, where non-matching docs must survive, so a consume
        // deletes only the landed docs.
        type Item = {
          doc: Record<string, unknown>;
          ctx: {
            sourceCollection?: string;
            instanceName?: string;
            documentType?: string;
          };
          remove: () => void;
        };
        const sources: {
          srcColl: string;
          items: Item[];
          dropWhole?: () => void;
        }[] = [];
        const from = operation.from;
        if (from.kind === "collection") {
          const coll = state.collections[from.name];
          if (coll) {
            const items: Item[] = [];
            for (const doc of [...coll.content]) {
              if (matchesWhere(doc, from.where)) {
                items.push({
                  doc,
                  ctx: { sourceCollection: from.name },
                  remove: () => {
                    coll.content = coll.content.filter((d) => d !== doc);
                  },
                });
              }
            }
            sources.push({
              srcColl: from.name,
              items,
              // A fully-consumed collection is removed entirely (key dropped),
              // so a drained collection no longer "exists"; a `where` is a
              // filtered subset (non-matching docs stay).
              dropWhole: from.where
                ? undefined
                : () => delete state.collections[from.name],
            });
          }
        } else if (from.kind === "multiModelInstances") {
          for (
            const [instanceName, inst] of Object.entries(state.multiModels)
          ) {
            if (inst.modelType !== from.model) continue;
            // Instances are named `<model>:<id>` — same rule the mongodb
            // applier gets from `discoverMultiCollectionInstances`. Without it
            // a bare `<model>` registry entry reads as an instance, and its
            // name becomes a `_scope` value that no scope format accepts.
            if (!instanceName.startsWith(`${from.model}:`)) continue;
            const items: Item[] = [];
            for (const doc of [...inst.content]) {
              // Skip the multi-collection's internal bookkeeping docs
              // (`_information`/`_migrations`) — mongodbee plumbing, not real
              // sub-documents (mirrors the mongodb applier).
              if (
                typeof doc._type === "string" && doc._type.startsWith("_")
              ) continue;
              items.push({
                doc,
                ctx: { instanceName },
                remove: () => {
                  inst.content = inst.content.filter((d) => d !== doc);
                },
              });
            }
            sources.push({
              srcColl: instanceName,
              items,
              // A whole instance consolidated away is dropped entirely,
              // including its bookkeeping docs (the drop is not a filtered
              // subset), so a drained instance no longer "exists".
              dropWhole: () => delete state.multiModels[instanceName],
            });
          }
        } else {
          const coll = state.multiCollections[from.collectionName];
          if (coll) {
            const items: Item[] = [];
            for (const doc of [...coll.content]) {
              if (doc._type === from.documentType) {
                items.push({
                  doc,
                  ctx: { documentType: from.documentType },
                  remove: () => {
                    coll.content = coll.content.filter((d) => d !== doc);
                  },
                });
              }
            }
            // A single `_type` out of a shared multi-collection is a filtered
            // subset — other types must survive, so consume the landed docs only.
            sources.push({ srcColl: from.collectionName, items });
          }
        }

        const onConflict = operation.onConflict ?? "error";

        // Process each source INDEPENDENTLY so its skip/landed bookkeeping — and
        // therefore its consume decision — is scoped to that source, exactly as
        // the mongodb applier iterates its discovered sources.
        for (const source of sources) {
          // Removers for the source docs that actually landed in the target,
          // and whether any doc was skipped (so `consume` preserves it).
          const landedRemovers: (() => void)[] = [];
          let anySkipped = false;

          for (const { doc, ctx, remove } of source.items) {
            const scope = operation.scope(doc, ctx);
            const mapped = operation.map
              ? operation.map({ ...doc }, ctx)
              : { ...doc };
            const toType = operation.toType
              ? operation.toType(doc, ctx)
              : (mapped._type ?? doc._type) as string;
            let id = mapped._id;
            if (id === undefined || id === null) {
              // DETERMINISTIC id (not a random UUID) so a replay lands the same
              // id — matching the mongodb applier's retry behaviour.
              id = flowScopeTargetId(
                toType,
                migrationId,
                source.srcColl,
                String(doc._id),
              );
            }
            const outDoc = { ...mapped, _id: id, _type: toType, _scope: scope };

            // Conflict on `_id` alone — the primary key of the physical scoped
            // collection — exactly like the mongodb applier.
            const idx = target.content.findIndex((d) => d._id === id);
            if (idx >= 0) {
              if (onConflict === "error") {
                throw new Error(
                  `flow_to_scope: conflict on (${scope}, ${toType}, ${id})`,
                );
              }
              if (onConflict === "skip") {
                // Incoming doc dropped — it never lands, so its source stays.
                anySkipped = true;
                continue;
              }
              const merged = operation.merge
                ? operation.merge(target.content[idx], outDoc)
                : { ...target.content[idx], ...outDoc };
              target.content[idx] = {
                ...merged,
                _id: id,
                _type: toType,
                _scope: scope,
              };
            } else {
              target.content.push(outDoc);
            }
            landedRemovers.push(remove);
          }

          if (operation.sourceDisposition === "consume") {
            if (anySkipped || !source.dropWhole) {
              // Some docs were skipped (they must survive), OR the source is a
              // filtered subset (non-matching docs must survive) — remove ONLY
              // the docs that landed; everything else stays in place.
              for (const remove of landedRemovers) remove();
            } else {
              source.dropWhole();
            }
          }
        }
        return state;
      },
      reverse: (_state, _operation) => {
        throw new Error("flow_to_scope is irreversible — cannot roll back");
      },
    },
    update_indexes: {
      apply: (state) => {
        // Indexes are not modeled in this in-memory representation
        return state;
      },
      reverse: (state) => {
        // Indexes are not modeled in this in-memory representation
        return state;
      },
    },
    delete_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection =
          state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        // Remove all documents of this type
        multiCollection.content = multiCollection.content.filter(
          (doc) => doc._type !== operation.documentType,
        );
        return state;
      },
      reverse: (_state, _operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(
          `Cannot reverse delete_multicollection_type: operation is irreversible`,
        );
      },
    },
    delete_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        for (
          const [_instanceName, instance] of Object.entries(state.multiModels)
        ) {
          if (instance.modelType === modelType) {
            // Remove all documents of this type from this instance
            instance.content = instance.content.filter(
              (doc) => doc._type !== operation.documentType,
            );
          }
        }
        return state;
      },
      reverse: (_state, _operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(
          `Cannot reverse delete_multimodel_instances_type: operation is irreversible`,
        );
      },
    },
    delete_scoped_multicollection_type: {
      apply: (state, operation) => {
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(
            `Scoped multi-collection ${operation.collectionName} does not exist`,
          );
        }
        // One physical collection holds every scope: dropping the type is a
        // single filter across all scopes.
        coll.content = coll.content.filter(
          (doc) => doc._type !== operation.documentType,
        );
        return state;
      },
      reverse: (_state, _operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(
          `Cannot reverse delete_scoped_multicollection_type: operation is irreversible`,
        );
      },
    },
    rename_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection =
          state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        // Rename all documents from oldTypeName to newTypeName
        multiCollection.content = multiCollection.content.map((doc) => {
          if (doc._type === operation.oldTypeName) {
            return { ...doc, _type: operation.newTypeName };
          }
          return doc;
        });
        return state;
      },
      reverse: (state, operation) => {
        const multiCollection =
          state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        // Reverse: rename from newTypeName back to oldTypeName
        multiCollection.content = multiCollection.content.map((doc) => {
          if (doc._type === operation.newTypeName) {
            return { ...doc, _type: operation.oldTypeName };
          }
          return doc;
        });
        return state;
      },
    },
    create_scoped_multicollection: {
      apply: (state, operation) => {
        state.scopedMultiCollections[operation.collectionName] = {
          content: [],
        };
        return state;
      },
      reverse: (state, operation) => {
        delete state.scopedMultiCollections[operation.collectionName];
        return state;
      },
    },
    seed_scoped_multicollection_type: {
      apply: (state, operation) => {
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(
            `Scoped multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const sig =
          `${operation.collectionName}:${operation.scope}:${operation.documentType}`;
        coll.content.push(...operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          return {
            ...typedDoc,
            _id: seedId(
              typedDoc,
              operation.schema._id,
              operation.documentType,
              sig,
              i,
            ),
            _type: operation.documentType,
            _scope: operation.scope,
          };
        }));
        return state;
      },
      reverse: (state, operation) => {
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(
            `Scoped multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const sig =
          `${operation.collectionName}:${operation.scope}:${operation.documentType}`;
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(
              doc as Record<string, unknown>,
              operation.schema._id,
              operation.documentType,
              sig,
              i,
            )
          ),
        );
        coll.content = coll.content.filter(
          (doc) => !seededIds.has(String(doc._id)),
        );
        return state;
      },
    },
    transform_scoped_multicollection_type: {
      apply: (state, operation) => {
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(
            `Scoped multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const scopeSet =
          operation.scopeFilter && operation.scopeFilter.length > 0
            ? new Set(operation.scopeFilter)
            : null;
        coll.content = coll.content.map((doc) => {
          if (
            doc._type === operation.documentType &&
            (!scopeSet || scopeSet.has(doc._scope as string))
          ) {
            return {
              ...operation.up(doc as Record<string, unknown>),
              _type: doc._type,
              _scope: doc._scope,
              _id: doc._id,
            };
          }
          return doc;
        });
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(
            `Scoped multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const scopeSet =
          operation.scopeFilter && operation.scopeFilter.length > 0
            ? new Set(operation.scopeFilter)
            : null;
        coll.content = coll.content.map((doc) => {
          if (
            doc._type === operation.documentType &&
            (!scopeSet || scopeSet.has(doc._scope as string))
          ) {
            return {
              ...operation.down(doc as Record<string, unknown>),
              _type: doc._type,
              _scope: doc._scope,
              _id: doc._id,
            };
          }
          return doc;
        });
        return state;
      },
    },
    rename_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        const oldTypePrefix = `${operation.oldTypeName}:`;
        const newTypePrefix = `${operation.newTypeName}:`;

        for (
          const [_instanceName, instance] of Object.entries(state.multiModels)
        ) {
          if (instance.modelType === modelType) {
            // Rename all documents from oldTypeName to newTypeName
            // Also update _id if it starts with "oldTypeName:"
            instance.content = instance.content.map((doc) => {
              if (doc._type === operation.oldTypeName) {
                const oldId = doc._id;
                let newId = oldId;

                // If _id is a string starting with "oldTypeName:", replace the prefix
                if (
                  typeof oldId === "string" && oldId.startsWith(oldTypePrefix)
                ) {
                  newId = newTypePrefix + oldId.slice(oldTypePrefix.length);
                }

                return { ...doc, _type: operation.newTypeName, _id: newId };
              }
              return doc;
            });
          }
        }
        return state;
      },
      reverse: (state, operation) => {
        const modelType = operation.modelType;
        const oldTypePrefix = `${operation.oldTypeName}:`;
        const newTypePrefix = `${operation.newTypeName}:`;

        for (
          const [_instanceName, instance] of Object.entries(state.multiModels)
        ) {
          if (instance.modelType === modelType) {
            // Reverse: rename from newTypeName back to oldTypeName
            // Also restore _id prefix if it starts with "newTypeName:"
            instance.content = instance.content.map((doc) => {
              if (doc._type === operation.newTypeName) {
                const currentId = doc._id;
                let restoredId = currentId;

                // If _id is a string starting with "newTypeName:", replace the prefix back
                if (
                  typeof currentId === "string" &&
                  currentId.startsWith(newTypePrefix)
                ) {
                  restoredId = oldTypePrefix +
                    currentId.slice(newTypePrefix.length);
                }

                return {
                  ...doc,
                  _type: operation.oldTypeName,
                  _id: restoredId,
                };
              }
              return doc;
            });
          }
        }
        return state;
      },
    },
  };

  async function applyOperation(
    state: DatabaseState,
    operation: MigrationRule,
  ): Promise<DatabaseState> {
    const handler = migrations[operation.type]?.apply;
    if (!handler) {
      throw new Error(`No handler for operation type: ${operation.type}`);
    }
    // Type assertion is safe here because we're dispatching to the correct handler based on operation.type
    // deno-lint-ignore no-explicit-any
    return await handler(state, operation as any);
  }

  async function reverseOperation(
    state: DatabaseState,
    operation: MigrationRule,
  ): Promise<DatabaseState> {
    const handler = migrations[operation.type]?.reverse;
    if (!handler) {
      throw new Error(
        `No reverse handler for operation type: ${operation.type}`,
      );
    }
    // Type assertion is safe here because we're dispatching to the correct handler based on operation.type
    // deno-lint-ignore no-explicit-any
    return await handler(state, operation as any);
  }

  /**
   * Applies a complete migration (all operations)
   *
   * For memory applier, there's no schema synchronization needed since
   * this is just an in-memory simulation.
   *
   * @param state - Current database state
   * @param operations - Array of migration operations to apply
   * @param direction - 'up' for forward migration, 'down' for rollback
   * @returns Updated database state
   */
  async function applyMigration(
    state: DatabaseState,
    operations: MigrationRule[],
    direction: "up" | "down",
  ): Promise<DatabaseState> {
    let currentState = state;

    // Pre-scan: refuse to roll back if any operation is irreversible, BEFORE
    // mutating anything — otherwise we'd leave a partial rollback behind.
    if (direction === "down") {
      const irreversible = getIrreversibleOperations(operations);
      if (irreversible.length > 0) {
        throw new Error(
          `Cannot roll back: migration contains ${irreversible.length} ` +
            `irreversible operation(s) [${
              irreversible.map((o) => o.type).join(", ")
            }]. ` +
            `Rollback aborted before any changes were made.`,
        );
      }
    }

    // Rollback undoes operations in LIFO order — reverse the list for 'down'
    // so e.g. a `seed` is undone before the `create_collection` it depends on.
    const ordered = direction === "down"
      ? [...operations].reverse()
      : operations;

    for (const operation of ordered) {
      if (direction === "up") {
        currentState = await applyOperation(currentState, operation);
      } else {
        currentState = await reverseOperation(currentState, operation);
      }
    }

    // No synchronization needed for memory applier
    return currentState;
  }

  return {
    applyOperation,
    reverseOperation,
    applyMigration,
  };
}
