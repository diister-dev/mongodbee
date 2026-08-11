/**
 * @fileoverview Mock population engine for the simulation validator
 *
 * The single implementation of mock-state population behind both
 * `buildMockStateFromSchemas` and `prepareStateForNextMigration`. Its
 * invariants:
 *
 * - Emptiness policy is an explicit {@link PopulatePolicy} parameter.
 * - Volume arithmetic is two NAMED operations — populate (batches × all
 *   types) vs refresh (restore exactly the pre-retention size).
 * - Generation failures are recorded and surfaced, never swallowed.
 * - Multi-model population is decided per MODEL, not per whole bucket.
 * - Existing state entries are always preserved, never reassigned.
 * - Buckets are processed in the {@link DatabaseState} declaration order.
 *
 * @module
 */

import type {
  DatabaseState,
  MockGenerationFailure,
  MultiSchema,
  SchemaContent,
  SchemasDefinition,
  ScopedMultiSchema,
} from "../../types.ts";
import type { MockGenerationConfig } from "./config.ts";
import { INSTANCES_PER_MODEL } from "./config.ts";
import { generateMockDocument } from "./generator.ts";
import type { CorrelationSession } from "./correlation.ts";

/**
 * When a target collection receives new mock documents.
 *
 * - `always`: append regardless of current content. Used when building the
 *   hybrid initial state — real parent seeds PLUS mock supplements, so both
 *   seeded values and generated edge cases are exercised.
 * - `ifEmpty`: populate only a collection with zero documents. Used when
 *   propagating state between migrations — retention already resized
 *   non-empty collections, and growing them again would compound volume at
 *   every step of the chain.
 * - `ifSparse`: populate only below `DOCS_PER_COLLECTION_MIN`. Used to top
 *   up multi-model instances the migration itself just created, so schema
 *   validation has enough documents to be meaningful.
 */
export type PopulatePolicy = "always" | "ifEmpty" | "ifSparse";

/**
 * Shared context threaded through every population call: the volume
 * configuration and the failure collector the caller folds into its
 * validation result.
 */
export interface MockPopulateContext {
  config: MockGenerationConfig;

  /** Failure collector — appended in place, one entry per aborted target. */
  failures: MockGenerationFailure[];

  /**
   * Correlated-identity session shared by every call of ONE population run.
   * It owns the RNG (replayability), the identifier pools (references and
   * instance names that coincide with real ids), and the findings report the
   * entry points drain into {@link MockPopulateContext.failures}.
   */
  session: CorrelationSession;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Draws the per-collection document count from the configured range, through
 * the session RNG so a simulation replays identically. Every preset
 * currently has MIN === MAX, so the draw is deterministic — the range is
 * kept so a future spread works without touching call sites.
 */
function drawDocCount(ctx: MockPopulateContext): number {
  const { config } = ctx;
  return Math.floor(
    ctx.session.random() *
      (config.DOCS_PER_COLLECTION_MAX - config.DOCS_PER_COLLECTION_MIN + 1),
  ) + config.DOCS_PER_COLLECTION_MIN;
}

/**
 * Production creates one instance per root entity: a lower count orphans
 * roots, and a consolidation merging root with instance then produces
 * documents amputated of the fields only the instance carries.
 */
function drawInstanceCount(
  modelType: string,
  taken: ReadonlySet<string>,
  ctx: MockPopulateContext,
): number {
  const available = ctx.session.pooledIds(modelType)
    .filter((id) => !taken.has(id)).length;
  if (available === 0) return INSTANCES_PER_MODEL;

  const capped = Math.min(available, ctx.config.MAX_INSTANCES_PER_MODEL);
  if (capped < available) {
    ctx.failures.push({
      bucket: "multiModels",
      collection: modelType,
      modelType,
      kind: "correlation",
      space: modelType,
      message: `Identifier space "${modelType}" holds ${available} entities ` +
        `without an instance, but the power level caps synthetic instances ` +
        `at ${capped} — the remaining ${available - capped} entities stay ` +
        `instance-less, so a consolidation merging them with instance ` +
        `documents is only simulated on ${capped} of them.`,
    });
  }
  return capped;
}

function shouldPopulate(
  contentLength: number,
  policy: PopulatePolicy,
  config: MockGenerationConfig,
): boolean {
  switch (policy) {
    case "always":
      return true;
    case "ifEmpty":
      return contentLength === 0;
    case "ifSparse":
      return contentLength < config.DOCS_PER_COLLECTION_MIN;
  }
}

/**
 * Appends up to `count` mock documents to a plain collection.
 *
 * Failure semantics: on the first generation error the whole target is
 * aborted and ONE structured failure is recorded — a generator that cannot
 * produce a value for a schema fails identically on every retry. Nothing is
 * thrown: the caller decides severity when folding the failures into its
 * validation result.
 */
function appendPlainDocs(
  content: Record<string, unknown>[],
  schema: SchemaContent,
  count: number,
  ctx: MockPopulateContext,
  collectionName: string,
): void {
  // Mint every `_id` BEFORE generating any document, so reference fields of
  // the batch (self-references included) find the pool already filled.
  const ids = ctx.session.mintIds({
    bucket: "collections",
    collection: collectionName,
    scopes: Array.from({ length: count }, () => null),
  });
  for (let i = 0; i < count; i++) {
    try {
      content.push(generateMockDocument(
        schema,
        ctx.session.docOptions({
          bucket: "collections",
          collection: collectionName,
          scope: null,
          assignedId: ids?.[i],
        }),
      ));
    } catch (error) {
      ctx.failures.push({
        bucket: "collections",
        collection: collectionName,
        message: errorMessage(error),
      });
      return;
    }
  }
}

/**
 * The refresh cycling order as data: type names repeated in declaration
 * order until `count` positions exist. Materializing the sequence lets the
 * mint phase run per TYPE before any document generates.
 */
function cycledTypeNames(typeNames: string[], count: number): string[] {
  const sequence: string[] = [];
  while (sequence.length < count) {
    for (const typeName of typeNames) {
      if (sequence.length >= count) break;
      sequence.push(typeName);
    }
  }
  return sequence;
}

/**
 * Appends `batchCount` batches to a typed collection — a batch is one mock
 * document per declared type, so every type is equally represented
 * (total = batchCount × #types).
 */
function appendTypedBatches(
  content: Record<string, unknown>[],
  types: MultiSchema,
  batchCount: number,
  ctx: MockPopulateContext,
  bucket: "multiCollections" | "multiModels",
  collectionName: string,
  modelType?: string,
): void {
  // The correlation plan is static, so it keys multiModels on the MODEL —
  // the instance is where the documents land, hence the instance name is the
  // documents' scope (a reference inside an instance resolves within it).
  const planCollection = modelType ?? collectionName;
  const scope = bucket === "multiModels" ? collectionName : null;
  const typeNames = Object.keys(types);

  const scopes = Array.from({ length: batchCount }, () => scope);
  const minted = new Map<string, string[] | undefined>();
  for (const typeName of typeNames) {
    minted.set(
      typeName,
      ctx.session.mintIds({
        bucket,
        collection: planCollection,
        type: typeName,
        scopes,
      }),
    );
  }

  for (let i = 0; i < batchCount; i++) {
    for (const typeName of typeNames) {
      try {
        content.push({
          ...generateMockDocument(
            types[typeName],
            ctx.session.docOptions({
              bucket,
              collection: planCollection,
              type: typeName,
              scope,
              assignedId: minted.get(typeName)?.[i],
            }),
          ),
          _type: typeName,
        });
      } catch (error) {
        ctx.failures.push({
          bucket,
          collection: collectionName,
          modelType,
          message: `type "${typeName}": ${errorMessage(error)}`,
        });
        return;
      }
    }
  }
}

/**
 * Appends exactly `count` mock documents to a typed collection, cycling
 * through the declared types (refresh semantics — see
 * {@link retainAndRefreshBuckets}).
 */
function appendTypedDocs(
  content: Record<string, unknown>[],
  types: MultiSchema,
  count: number,
  ctx: MockPopulateContext,
  bucket: "multiCollections" | "multiModels",
  collectionName: string,
  modelType?: string,
): void {
  const typeNames = Object.keys(types);
  // A typed schema with zero types has nothing to generate; without this
  // guard the cycling sequence below could never reach `count`.
  if (typeNames.length === 0) return;

  const planCollection = modelType ?? collectionName;
  const scope = bucket === "multiModels" ? collectionName : null;

  const sequence = cycledTypeNames(typeNames, count);
  const minted = new Map<string, string[]>();
  for (const typeName of typeNames) {
    const perType = sequence.filter((t) => t === typeName).length;
    if (perType === 0) continue;
    const ids = ctx.session.mintIds({
      bucket,
      collection: planCollection,
      type: typeName,
      scopes: Array.from({ length: perType }, () => scope),
    });
    if (ids) minted.set(typeName, ids);
  }

  for (const typeName of sequence) {
    try {
      content.push({
        ...generateMockDocument(
          types[typeName],
          ctx.session.docOptions({
            bucket,
            collection: planCollection,
            type: typeName,
            scope,
            assignedId: minted.get(typeName)?.shift(),
          }),
        ),
        _type: typeName,
      });
    } catch (error) {
      ctx.failures.push({
        bucket,
        collection: collectionName,
        modelType,
        message: `type "${typeName}": ${errorMessage(error)}`,
      });
      return;
    }
  }
}

/**
 * Appends `batchCount` batches to a scoped multi-collection. Each batch
 * shares one `_scope` value REALIZED by the session — an existing scope-space
 * id when the pools hold any, a fresh value from the `scope` schema
 * otherwise — so simulated documents land in the same scopes real entities
 * inhabit.
 */
function appendScopedBatches(
  content: Record<string, unknown>[],
  scopedSchema: ScopedMultiSchema,
  batchCount: number,
  ctx: MockPopulateContext,
  collectionName: string,
): void {
  let scopes: string[];
  try {
    scopes = ctx.session.realizeScopes(
      collectionName,
      scopedSchema.scope,
      batchCount,
    );
  } catch (error) {
    ctx.failures.push({
      bucket: "scopedMultiCollections",
      collection: collectionName,
      message: `scope: ${errorMessage(error)}`,
    });
    return;
  }

  const typeNames = Object.keys(scopedSchema.types);
  const minted = new Map<string, string[] | undefined>();
  for (const typeName of typeNames) {
    minted.set(
      typeName,
      ctx.session.mintIds({
        bucket: "scopedMultiCollections",
        collection: collectionName,
        type: typeName,
        scopes,
      }),
    );
  }

  for (let i = 0; i < batchCount; i++) {
    for (const typeName of typeNames) {
      try {
        content.push({
          ...generateMockDocument(
            scopedSchema.types[typeName],
            ctx.session.docOptions({
              bucket: "scopedMultiCollections",
              collection: collectionName,
              type: typeName,
              scope: scopes[i],
              assignedId: minted.get(typeName)?.[i],
            }),
          ),
          _type: typeName,
          _scope: scopes[i],
        });
      } catch (error) {
        ctx.failures.push({
          bucket: "scopedMultiCollections",
          collection: collectionName,
          message: `type "${typeName}": ${errorMessage(error)}`,
        });
        return;
      }
    }
  }
}

/**
 * Appends exactly `count` scoped mock documents, cycling through the types;
 * one `_scope` value is realized per cycle (refresh counterpart of
 * {@link appendScopedBatches}).
 */
function appendScopedDocs(
  content: Record<string, unknown>[],
  scopedSchema: ScopedMultiSchema,
  count: number,
  ctx: MockPopulateContext,
  collectionName: string,
): void {
  const typeNames = Object.keys(scopedSchema.types);
  if (typeNames.length === 0) return;

  const cycles = Math.ceil(count / typeNames.length);
  let scopes: string[];
  try {
    scopes = ctx.session.realizeScopes(
      collectionName,
      scopedSchema.scope,
      cycles,
    );
  } catch (error) {
    ctx.failures.push({
      bucket: "scopedMultiCollections",
      collection: collectionName,
      message: `scope: ${errorMessage(error)}`,
    });
    return;
  }

  // Position i of the cycling sequence lives in scope `scopes[floor(i / n)]`
  // — one scope per cycle.
  const sequence = cycledTypeNames(typeNames, count);
  const minted = new Map<string, string[]>();
  for (const typeName of typeNames) {
    const perTypeScopes: string[] = [];
    sequence.forEach((t, i) => {
      if (t === typeName) {
        perTypeScopes.push(scopes[Math.floor(i / typeNames.length)]);
      }
    });
    if (perTypeScopes.length === 0) continue;
    const ids = ctx.session.mintIds({
      bucket: "scopedMultiCollections",
      collection: collectionName,
      type: typeName,
      scopes: perTypeScopes,
    });
    if (ids) minted.set(typeName, ids);
  }

  for (let i = 0; i < sequence.length; i++) {
    const typeName = sequence[i];
    const scope = scopes[Math.floor(i / typeNames.length)];
    try {
      content.push({
        ...generateMockDocument(
          scopedSchema.types[typeName],
          ctx.session.docOptions({
            bucket: "scopedMultiCollections",
            collection: collectionName,
            type: typeName,
            scope,
            assignedId: minted.get(typeName)?.shift(),
          }),
        ),
        _type: typeName,
        _scope: scope,
      });
    } catch (error) {
      ctx.failures.push({
        bucket: "scopedMultiCollections",
        collection: collectionName,
        message: `type "${typeName}": ${errorMessage(error)}`,
      });
      return;
    }
  }
}

/**
 * Populates plain collections declared in the schema.
 * Missing state entries are created; existing content is preserved
 * and only supplemented when the policy allows it.
 */
export function populateCollections(
  state: DatabaseState,
  collections: NonNullable<SchemasDefinition["collections"]>,
  policy: PopulatePolicy,
  ctx: MockPopulateContext,
): void {
  for (const [collectionName, schema] of Object.entries(collections)) {
    state.collections[collectionName] ??= { content: [] };
    const collection = state.collections[collectionName];
    if (!shouldPopulate(collection.content.length, policy, ctx.config)) {
      continue;
    }
    appendPlainDocs(
      collection.content,
      schema,
      drawDocCount(ctx),
      ctx,
      collectionName,
    );
  }
}

/**
 * Populates multi-collections declared in the schema (batch semantics:
 * docCount batches × all types — see {@link PopulatePolicy} for when).
 */
export function populateMultiCollections(
  state: DatabaseState,
  multiCollections: NonNullable<SchemasDefinition["multiCollections"]>,
  policy: PopulatePolicy,
  ctx: MockPopulateContext,
): void {
  for (const [collectionName, schema] of Object.entries(multiCollections)) {
    state.multiCollections[collectionName] ??= { content: [] };
    const collection = state.multiCollections[collectionName];
    if (!shouldPopulate(collection.content.length, policy, ctx.config)) {
      continue;
    }
    appendTypedBatches(
      collection.content,
      schema,
      drawDocCount(ctx),
      ctx,
      "multiCollections",
      collectionName,
    );
  }
}

/**
 * Populates SYNTHETIC multi-model instances for declared models.
 *
 * The decision is taken per MODEL — a model with no instance gets synthetic
 * ones even when other models already have instances; otherwise a
 * newly declared model would ride green on another model's documents, its
 * validation loops iterating nothing. An existing-but-empty instance counts
 * as "the model has an instance"; topping it up is
 * {@link populateExistingMultiModelInstances}'s job at validation time.
 *
 * An instance entry that already exists is never reassigned.
 */
export function populateSyntheticMultiModelInstances(
  state: DatabaseState,
  multiModels: NonNullable<SchemasDefinition["multiModels"]>,
  policy: PopulatePolicy,
  ctx: MockPopulateContext,
): void {
  if (policy === "ifSparse") {
    // Sparseness is a per-instance measure; it has no meaning for deciding
    // whether a synthetic instance should exist at all.
    throw new Error(
      'Populate policy "ifSparse" is not defined for synthetic multi-model ' +
        "instances — use populateExistingMultiModelInstances for top-ups.",
    );
  }

  for (const [modelType, schema] of Object.entries(multiModels)) {
    if (policy === "ifEmpty") {
      const modelHasInstance = Object.values(state.multiModels).some(
        (instance) => instance.modelType === modelType,
      );
      if (modelHasInstance) continue;
    }

    // Instance names are REALIZED, not invented: when the model-key space's
    // pool holds real root ids, the instance takes one of them — production
    // names instances `<model>:<entity id>`, so references and scopes can
    // coincide with them. Names never collide with existing entries, so
    // real instances are never reassigned.
    //
    // No bare `<model>` entry: production has instance collections only
    // (`<model>:<id>`), so inventing one gives the appliers a phantom
    // instance whose name is not a valid scope value.
    const taken = new Set(Object.keys(state.multiModels));
    const names = ctx.session.realizeInstanceNames(
      modelType,
      drawInstanceCount(modelType, taken, ctx),
      taken,
    );
    for (const collectionName of names) {
      state.multiModels[collectionName] ??= { modelType, content: [] };

      appendTypedBatches(
        state.multiModels[collectionName].content,
        schema,
        drawDocCount(ctx),
        ctx,
        "multiModels",
        collectionName,
        modelType,
      );
    }
  }
}

/**
 * Populates EXISTING multi-model instances (whatever created them — parent
 * simulation, the migration under validation, or a previous propagation)
 * whose model is declared in the schema. Used with `ifSparse` after a
 * migration ran, so instances it created have documents to validate against.
 */
export function populateExistingMultiModelInstances(
  state: DatabaseState,
  multiModels: NonNullable<SchemasDefinition["multiModels"]>,
  policy: PopulatePolicy,
  ctx: MockPopulateContext,
): void {
  // Existing documents and instance names feed the pools first, so topped-up
  // documents reference REAL post-migration identities. This entry point is
  // called standalone (after a migration ran), hence it harvests and drains
  // like the other two engine entry points.
  ctx.session.harvest(state);

  for (const [instanceName, instance] of Object.entries(state.multiModels)) {
    const schema = multiModels[instance.modelType];
    if (!schema) continue;
    if (!shouldPopulate(instance.content.length, policy, ctx.config)) {
      continue;
    }
    appendTypedBatches(
      instance.content,
      schema,
      drawDocCount(ctx),
      ctx,
      "multiModels",
      instanceName,
      instance.modelType,
    );
  }

  ctx.failures.push(...ctx.session.drainFindings());
}

/**
 * Populates scoped multi-collections declared in the schema (batch
 * semantics; each batch shares one generated `_scope` value).
 */
export function populateScopedMultiCollections(
  state: DatabaseState,
  scopedMultiCollections: NonNullable<
    SchemasDefinition["scopedMultiCollections"]
  >,
  policy: PopulatePolicy,
  ctx: MockPopulateContext,
): void {
  // A collection holding a root singleton mints its own scope ENTITIES
  // (`information._id === _scope`), so it must realize its scopes before
  // sibling scoped collections draw from the same space — otherwise the
  // siblings draw from an empty pool and invent parallel scopes. The sort is
  // stable, so declaration order is preserved within each group.
  const entries = Object.entries(scopedMultiCollections).sort(
    (a, b) =>
      Number(ctx.session.realizesOwnScopes(b[0])) -
      Number(ctx.session.realizesOwnScopes(a[0])),
  );
  for (const [collectionName, scopedSchema] of entries) {
    state.scopedMultiCollections[collectionName] ??= { content: [] };
    const collection = state.scopedMultiCollections[collectionName];
    if (!shouldPopulate(collection.content.length, policy, ctx.config)) {
      continue;
    }
    appendScopedBatches(
      collection.content,
      scopedSchema,
      drawDocCount(ctx),
      ctx,
      collectionName,
    );
  }
}

/**
 * Populates every bucket declared in the schemas, in the canonical order —
 * the {@link DatabaseState} declaration order: collections,
 * multiCollections, multiModels, scopedMultiCollections.
 */
export function populateDeclaredBuckets(
  state: DatabaseState,
  schemas: SchemasDefinition,
  policy: PopulatePolicy,
  ctx: MockPopulateContext,
): void {
  // Real identities first: parent seeds and pre-existing documents fill the
  // pools BEFORE anything generates, so fresh references and instance names
  // can coincide with them (harvest is idempotent — pools deduplicate).
  ctx.session.harvest(state);

  if (schemas.collections) {
    populateCollections(state, schemas.collections, policy, ctx);
  }
  if (schemas.multiCollections) {
    populateMultiCollections(state, schemas.multiCollections, policy, ctx);
  }
  if (schemas.multiModels) {
    populateSyntheticMultiModelInstances(
      state,
      schemas.multiModels,
      policy,
      ctx,
    );
  }
  if (schemas.scopedMultiCollections) {
    populateScopedMultiCollections(
      state,
      schemas.scopedMultiCollections,
      policy,
      ctx,
    );
  }

  // Correlation findings join the failure channel the caller folds into its
  // validation result.
  ctx.failures.push(...ctx.session.drainFindings());
}

/**
 * Applies the retention ratio to every bucket, then refreshes each entry
 * back to its pre-retention size with fresh mock data (canonical bucket
 * order).
 *
 * Volume semantics: refresh appends EXACTLY `originalCount - keepCount`
 * documents, cycling through the declared types — preparing a state never
 * grows a non-empty collection, so propagation cannot compound volume
 * across a migration chain. The last cycle is truncated when the count is
 * not a multiple of the type count.
 *
 * Retention applies even when the schema no longer declares the entry;
 * refresh requires a schema to generate against, so schema-less entries only
 * shrink — exactly what a dropped-from-schema collection should do.
 *
 * Retention runs over EVERY bucket before any refresh generates: the harvest
 * in between must only register identities of documents that actually
 * survive, or fresh references would point at ghosts.
 */
export function retainAndRefreshBuckets(
  state: DatabaseState,
  schemas: SchemasDefinition,
  ratio: number,
  ctx: MockPopulateContext,
): void {
  /** Slices a bucket's entries and returns the per-entry refresh need. */
  const applyRetention = (
    entries: Record<string, { content: Record<string, unknown>[] }>,
  ): Map<string, number> => {
    const refreshNeeds = new Map<string, number>();
    for (const [name, entry] of Object.entries(entries)) {
      const originalCount = entry.content.length;
      const keepCount = Math.floor(originalCount * ratio);
      entry.content = entry.content.slice(0, keepCount);
      refreshNeeds.set(name, originalCount - keepCount);
    }
    return refreshNeeds;
  };

  const collectionNeeds = applyRetention(state.collections);
  const multiCollectionNeeds = applyRetention(state.multiCollections);
  const multiModelNeeds = applyRetention(state.multiModels);
  const scopedNeeds = applyRetention(state.scopedMultiCollections);

  // Surviving documents feed the pools, so refreshed documents reference
  // retained identities instead of a disjoint fresh universe.
  ctx.session.harvest(state);

  for (const [collectionName, need] of collectionNeeds) {
    const schema = schemas.collections?.[collectionName];
    if (!schema) continue;
    appendPlainDocs(
      state.collections[collectionName].content,
      schema,
      need,
      ctx,
      collectionName,
    );
  }

  for (const [collectionName, need] of multiCollectionNeeds) {
    const schema = schemas.multiCollections?.[collectionName];
    if (!schema) continue;
    appendTypedDocs(
      state.multiCollections[collectionName].content,
      schema,
      need,
      ctx,
      "multiCollections",
      collectionName,
    );
  }

  for (const [instanceName, need] of multiModelNeeds) {
    const instance = state.multiModels[instanceName];
    const schema = schemas.multiModels?.[instance.modelType];
    if (!schema) continue;
    appendTypedDocs(
      instance.content,
      schema,
      need,
      ctx,
      "multiModels",
      instanceName,
      instance.modelType,
    );
  }

  // Same ordering rule as populateScopedMultiCollections: collections that
  // mint their own scope entities refresh first, so siblings draw realized
  // scopes instead of an empty pool.
  const orderedScoped = [...scopedNeeds.entries()].sort(
    (a, b) =>
      Number(ctx.session.realizesOwnScopes(b[0])) -
      Number(ctx.session.realizesOwnScopes(a[0])),
  );
  for (const [collectionName, need] of orderedScoped) {
    const scopedSchema = schemas.scopedMultiCollections?.[collectionName];
    if (!scopedSchema) continue;
    appendScopedDocs(
      state.scopedMultiCollections[collectionName].content,
      scopedSchema,
      need,
      ctx,
      collectionName,
    );
  }

  ctx.failures.push(...ctx.session.drainFindings());
}
