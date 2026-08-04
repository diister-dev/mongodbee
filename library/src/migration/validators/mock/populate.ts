/**
 * @fileoverview Mock population engine for the simulation validator
 *
 * ONE implementation of mock-state population, shared by the two call paths
 * that historically each had their own copy (`buildMockStateFromSchemas` and
 * `prepareStateForNextMigration`). The duplication had let six behavioral
 * divergences ship silently; each is now a single, documented decision:
 *
 * - Emptiness policy is an explicit {@link PopulatePolicy} parameter instead
 *   of three implicit behaviors.
 * - Volume arithmetic is two NAMED operations — populate (batches × all
 *   types) vs refresh (restore exactly the pre-retention size) — instead of
 *   two accidental formulas.
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
import { generateMockDocument, generateMockScopeValue } from "./generator.ts";

/**
 * When a target collection receives new mock documents (divergence D1).
 *
 * Three policies coexisted implicitly — unconditional append, populate only
 * when empty, populate below a minimum. Each is legitimate for its call
 * site, so the policy is now an explicit parameter rather than a hidden
 * property of whichever copy of the code ran:
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
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Draws the per-collection document count from the configured range.
 * Every preset currently has MIN === MAX, so the draw is deterministic —
 * the range is kept so a future spread works without touching call sites.
 */
function drawDocCount(config: MockGenerationConfig): number {
  return Math.floor(
    Math.random() *
      (config.DOCS_PER_COLLECTION_MAX - config.DOCS_PER_COLLECTION_MIN + 1),
  ) + config.DOCS_PER_COLLECTION_MIN;
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
 * Failure semantics (divergence D3): on the first generation error the whole
 * target is aborted and ONE structured failure is recorded. A generator that
 * cannot produce a value for a schema is a structural problem — retrying the
 * remaining iterations would fail identically, and the old per-scope
 * `catch { break }` variants only differed in how much wasted work they did
 * before going silent. Nothing is thrown: the caller decides severity when
 * folding the failures into its validation result.
 */
function appendPlainDocs(
  content: Record<string, unknown>[],
  schema: SchemaContent,
  count: number,
  ctx: MockPopulateContext,
  collectionName: string,
): void {
  for (let i = 0; i < count; i++) {
    try {
      content.push(generateMockDocument(schema));
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
 * Appends `batchCount` batches to a typed collection — a batch is one mock
 * document per declared type, so every type is equally represented
 * (populate semantics of divergence D2: total = batchCount × #types).
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
  for (let i = 0; i < batchCount; i++) {
    for (const typeName of Object.keys(types)) {
      try {
        content.push({
          ...generateMockDocument(types[typeName]),
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
 * through the declared types (refresh semantics of divergence D2 — see
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
  // guard the while-loop below could never make progress.
  if (typeNames.length === 0) return;

  let appended = 0;
  while (appended < count) {
    for (const typeName of typeNames) {
      if (appended >= count) break;
      try {
        content.push({
          ...generateMockDocument(types[typeName]),
          _type: typeName,
        });
        appended++;
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
 * Appends `batchCount` batches to a scoped multi-collection. Each batch
 * shares one mock `_scope` value generated from the `scope` schema, so
 * simulated documents exercise the same envelope (`_type` + `_scope`) the
 * appliers produce.
 */
function appendScopedBatches(
  content: Record<string, unknown>[],
  scopedSchema: ScopedMultiSchema,
  batchCount: number,
  ctx: MockPopulateContext,
  collectionName: string,
): void {
  for (let i = 0; i < batchCount; i++) {
    let scopeValue: unknown;
    try {
      scopeValue = generateMockScopeValue(scopedSchema.scope);
    } catch (error) {
      ctx.failures.push({
        bucket: "scopedMultiCollections",
        collection: collectionName,
        message: `scope: ${errorMessage(error)}`,
      });
      return;
    }
    for (const typeName of Object.keys(scopedSchema.types)) {
      try {
        content.push({
          ...generateMockDocument(scopedSchema.types[typeName]),
          _type: typeName,
          _scope: scopeValue,
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
 * one `_scope` value is generated per cycle (refresh counterpart of
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

  let appended = 0;
  while (appended < count) {
    let scopeValue: unknown;
    try {
      scopeValue = generateMockScopeValue(scopedSchema.scope);
    } catch (error) {
      ctx.failures.push({
        bucket: "scopedMultiCollections",
        collection: collectionName,
        message: `scope: ${errorMessage(error)}`,
      });
      return;
    }
    for (const typeName of typeNames) {
      if (appended >= count) break;
      try {
        content.push({
          ...generateMockDocument(scopedSchema.types[typeName]),
          _type: typeName,
          _scope: scopeValue,
        });
        appended++;
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
 * Populates plain collections declared in the schema.
 * Missing state entries are created; existing content is preserved
 * (divergence D5) and only supplemented when the policy allows it.
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
      drawDocCount(ctx.config),
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
      drawDocCount(ctx.config),
      ctx,
      "multiCollections",
      collectionName,
    );
  }
}

/**
 * Populates SYNTHETIC multi-model instances for declared models.
 *
 * Granularity (divergence D4): the decision is taken per MODEL — a model
 * with no instance gets a synthetic one even when other models already have
 * instances. The old whole-bucket check let a newly declared model ride
 * green on another model's documents: its validation loops iterated nothing.
 * An existing-but-empty instance counts as "the model has an instance";
 * topping it up is {@link populateExistingMultiModelInstances}'s job at
 * validation time.
 *
 * Preservation (divergence D5): an instance entry that already exists is
 * never reassigned — the old code cleared any real instance whose name
 * collided with the synthetic `<model>:instance<N>` naming.
 */
export function populateSyntheticMultiModelInstances(
  state: DatabaseState,
  multiModels: NonNullable<SchemasDefinition["multiModels"]>,
  policy: PopulatePolicy,
  ctx: MockPopulateContext,
): void {
  if (policy === "ifSparse") {
    // Sparseness is a per-instance measure; it has no meaning for deciding
    // whether a synthetic instance should exist at all. Failing loud beats
    // guessing a semantic no caller ever defined.
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

    for (let i = 0; i < INSTANCES_PER_MODEL; i++) {
      // `<model>:<id>`, the real instance-naming convention (see
      // `discoverMultiCollectionInstances`). A synthetic `@` separator made
      // the name fail any scope format a migration flows instances into —
      // `flowToScope` uses `ctx.instanceName` as the scope value.
      const collectionName = `${modelType}:instance${i + 1}`;

      // No bare `<model>` entry: production has instance collections only
      // (`<model>:<id>`), so inventing one gives the appliers a phantom
      // instance whose name is not a valid scope value.
      state.multiModels[collectionName] ??= { modelType, content: [] };

      appendTypedBatches(
        state.multiModels[collectionName].content,
        schema,
        drawDocCount(ctx.config),
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
  for (const [instanceName, instance] of Object.entries(state.multiModels)) {
    const schema = multiModels[instance.modelType];
    if (!schema) continue;
    if (!shouldPopulate(instance.content.length, policy, ctx.config)) {
      continue;
    }
    appendTypedBatches(
      instance.content,
      schema,
      drawDocCount(ctx.config),
      ctx,
      "multiModels",
      instanceName,
      instance.modelType,
    );
  }
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
  for (
    const [collectionName, scopedSchema] of Object.entries(
      scopedMultiCollections,
    )
  ) {
    state.scopedMultiCollections[collectionName] ??= { content: [] };
    const collection = state.scopedMultiCollections[collectionName];
    if (!shouldPopulate(collection.content.length, policy, ctx.config)) {
      continue;
    }
    appendScopedBatches(
      collection.content,
      scopedSchema,
      drawDocCount(ctx.config),
      ctx,
      collectionName,
    );
  }
}

/**
 * Populates every bucket declared in the schemas, in the canonical order —
 * the {@link DatabaseState} declaration order: collections,
 * multiCollections, multiModels, scopedMultiCollections (divergence D6).
 * The two historical copies disagreed on ordering; nothing observable
 * depended on it (only the RNG stream), but a single order means a failure
 * report always lists buckets consistently.
 */
export function populateDeclaredBuckets(
  state: DatabaseState,
  schemas: SchemasDefinition,
  policy: PopulatePolicy,
  ctx: MockPopulateContext,
): void {
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
}

/**
 * Applies the retention ratio to every bucket, then refreshes each entry
 * back to its pre-retention size with fresh mock data (canonical bucket
 * order — divergence D6).
 *
 * Volume semantics (divergence D2): refresh appends EXACTLY
 * `originalCount - keepCount` documents, cycling through the declared types.
 * This guarantees the invariant the propagation tests state — preparing a
 * state never grows a non-empty collection — for every type count. The old
 * code refreshed `ceil(newDocs / #types)` full batches, which could
 * overshoot the original size for multi-type collections; the last cycle is
 * now truncated instead (a slightly uneven type distribution is harmless,
 * compounding growth across a migration chain is not).
 *
 * Retention applies even when the schema no longer declares the entry;
 * refresh requires a schema to generate against, so schema-less entries only
 * shrink — exactly what a dropped-from-schema collection should do.
 */
export function retainAndRefreshBuckets(
  state: DatabaseState,
  schemas: SchemasDefinition,
  ratio: number,
  ctx: MockPopulateContext,
): void {
  for (
    const [collectionName, collection] of Object.entries(state.collections)
  ) {
    const originalCount = collection.content.length;
    const keepCount = Math.floor(originalCount * ratio);
    collection.content = collection.content.slice(0, keepCount);

    const schema = schemas.collections?.[collectionName];
    if (!schema) continue;
    appendPlainDocs(
      collection.content,
      schema,
      originalCount - keepCount,
      ctx,
      collectionName,
    );
  }

  for (
    const [collectionName, collection] of Object.entries(
      state.multiCollections,
    )
  ) {
    const originalCount = collection.content.length;
    const keepCount = Math.floor(originalCount * ratio);
    collection.content = collection.content.slice(0, keepCount);

    const schema = schemas.multiCollections?.[collectionName];
    if (!schema) continue;
    appendTypedDocs(
      collection.content,
      schema,
      originalCount - keepCount,
      ctx,
      "multiCollections",
      collectionName,
    );
  }

  for (const [instanceName, instance] of Object.entries(state.multiModels)) {
    const originalCount = instance.content.length;
    const keepCount = Math.floor(originalCount * ratio);
    instance.content = instance.content.slice(0, keepCount);

    const schema = schemas.multiModels?.[instance.modelType];
    if (!schema) continue;
    appendTypedDocs(
      instance.content,
      schema,
      originalCount - keepCount,
      ctx,
      "multiModels",
      instanceName,
      instance.modelType,
    );
  }

  for (
    const [collectionName, collection] of Object.entries(
      state.scopedMultiCollections,
    )
  ) {
    const originalCount = collection.content.length;
    const keepCount = Math.floor(originalCount * ratio);
    collection.content = collection.content.slice(0, keepCount);

    const scopedSchema = schemas.scopedMultiCollections?.[collectionName];
    if (!scopedSchema) continue;
    appendScopedDocs(
      collection.content,
      scopedSchema,
      originalCount - keepCount,
      ctx,
      collectionName,
    );
  }
}
