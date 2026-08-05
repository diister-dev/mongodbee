/**
 * @fileoverview Correlated identity generation for the simulation validator
 *
 * The mock engine used to populate every bucket independently: root documents
 * received random `_id`s, multi-model instances were named `<model>:instance1`,
 * and `_scope` values were invented per batch. No identity was ever SHARED
 * between buckets, so any migration whose riskiest line depends on keys
 * coinciding — `flowToScope` with `onConflict: "merge"` fusing a root document
 * with its per-instance counterpart — sailed through the gate without that
 * line ever executing.
 *
 * This module makes generated identities coincide the way production data
 * does. `refId(T)` / `dbId(T)` define a nominal identifier SPACE `T`; the
 * bucket whose `_id` schema is typed in `T` owns it (read from the `_id`
 * SCHEMA, never from the collection name — the type key and the space name
 * can differ). Owned identifiers accumulate in pools keyed on
 * (space × scope) — a scoped reference must come from the SAME `_scope`,
 * because a syntactically valid but cross-scope id would be semantically
 * absurd — and reference fields draw from those pools at generation time.
 *
 * Five conceptual phases, each a pure step driven by the population engine
 * in `populate.ts`:
 *
 *   plan      static read of the schemas — who owns which space, which
 *             fields reference one, which scoped types are root singletons
 *             (built once in {@link createCorrelationSession})
 *   mint      identifier values for owner documents fill the pools BEFORE
 *             any document is generated, so reference cycles (users ↔ orgs)
 *             need no topological order ({@link CorrelationSession.mintIds})
 *   realize   multi-model instance NAMES and scoped-batch `_scope` values
 *             are DRAWN from the pools instead of invented
 *             ({@link CorrelationSession.realizeInstanceNames} /
 *             {@link CorrelationSession.realizeScopes})
 *   populate  document generation — owned by `populate.ts`
 *   link      reference fields resolve against the pools through the
 *             generator's `resolve` hook
 *             ({@link CorrelationSession.docOptions})
 *
 * The report IS the lock. Every hole in the correlation — a referenced space
 * nobody mints, an ambiguous owner, an empty pool at draw time — surfaces as
 * a warning through the existing failure channel, replacing a silent hole
 * with a spoken one. The escape hatch is INVERTED: one declares what is
 * assumed NOT correlated ({@link CorrelationSessionOptions.uncorrelatedSpaces}),
 * which silences the warning — a forgotten declaration produces noise, never
 * a silent gap.
 *
 * @module
 */

import type {
  DatabaseState,
  MockGenerationFailure,
  SchemaContent,
  SchemasDefinition,
} from "../../types.ts";
import { extractIdPrefix, fnv1a32 } from "../../utils/seed-id.ts";
import { refId } from "../../../ids.ts";
import {
  generateMockScopeValue,
  type MockDocumentOptions,
} from "./generator.ts";
import { SKIP } from "@diister/valibot-mock";
import type { ResolveNode } from "@diister/valibot-mock";

/** Canonical bucket order — the {@link DatabaseState} declaration order. */
const BUCKET_ORDER: Record<keyof DatabaseState, number> = {
  collections: 0,
  multiCollections: 1,
  multiModels: 2,
  scopedMultiCollections: 3,
};

/**
 * One populate target the plan knows about: a (bucket, collection[, type])
 * whose `_id` schema is typed in an identifier space. For multiModels the
 * `collection` is the MODEL key (instances are dynamic, the plan is static).
 */
interface PlanTarget {
  readonly bucket: keyof DatabaseState;
  readonly collection: string;
  readonly type?: string;
  readonly space: string;
}

/**
 * The static read of the schemas — pure data, computed once per session.
 */
interface CorrelationPlan {
  /** Target key → target, for every `_id` typed in a correlated space. */
  readonly targets: Map<string, PlanTarget>;

  /**
   * Space → the ONE target whose minted ids feed the draw pool. When several
   * targets mint the same space (two real cases in the wild), a deterministic
   * tie-break picks the first in canonical bucket/name order and a report
   * line surfaces the ambiguity — no general multi-owner mechanism.
   */
  readonly contributors: Map<string, string>;

  /** Scoped collection name → identifier space of its `scope` schema. */
  readonly scopeSpaces: Map<string, string>;

  /**
   * Target keys of ROOT SINGLETONS: a scoped type whose `_id` space equals
   * its collection's `scope` space. Inferred, never declared — for such a
   * type the document IS the scope's canonical record, so `_id === _scope`.
   */
  readonly singletons: Set<string>;

  /** Report lines produced by the static read (ambiguities, ownerless spaces). */
  readonly findings: MockGenerationFailure[];
}

function targetKey(
  bucket: keyof DatabaseState,
  collection: string,
  type: string | undefined,
): string {
  return `${bucket}/${collection}/${type ?? ""}`;
}

/**
 * The identifier space of one target's `_id`. An explicit `_id` schema wins;
 * without one, the typed buckets auto-inject `_id: dbId(<type key>)` at
 * runtime (see `multi-collection.ts` / `scoped-multi-collection.ts`), so the
 * type key IS the space. `autoKey` is null for plain collections, which
 * never auto-inject.
 */
function targetIdSpace(
  idSchema: unknown,
  autoKey: string | null,
): string {
  if (idSchema !== undefined) return extractIdPrefix(idSchema);
  return autoKey ?? "";
}

/**
 * Depth-first walk over a Valibot schema collecting `^prefix:`-shaped leaf
 * schemas — the stored reference fields. Reads valibot internals (`entries`,
 * `wrapped`, `item`, `items`, `options`, `key`, `value`, `pipe`), so it is
 * deliberately defensive: anything unrecognized is simply not descended into.
 * `lazy` schemas are skipped (their getter may recurse).
 */
function walkReferences(
  schema: unknown,
  path: string,
  visit: (space: string, path: string) => void,
  depth: number,
): void {
  if (depth > 10 || schema === null || typeof schema !== "object") return;
  const prefix = extractIdPrefix(schema);
  if (prefix) {
    visit(prefix, path);
    return;
  }
  // deno-lint-ignore no-explicit-any
  const s = schema as Record<string, any>;
  if (s.type === "lazy") return;
  if (s.entries && typeof s.entries === "object") {
    for (const key of Object.keys(s.entries)) {
      walkReferences(
        s.entries[key],
        path ? `${path}.${key}` : key,
        visit,
        depth + 1,
      );
    }
  }
  if (s.wrapped) walkReferences(s.wrapped, path, visit, depth + 1);
  if (s.item) walkReferences(s.item, path, visit, depth + 1);
  if (Array.isArray(s.items)) {
    s.items.forEach((item: unknown, i: number) =>
      walkReferences(item, `${path}.${i}`, visit, depth + 1)
    );
  }
  if (Array.isArray(s.options)) {
    for (const option of s.options) {
      walkReferences(option, path, visit, depth + 1);
    }
  }
  if (s.key) walkReferences(s.key, path, visit, depth + 1);
  if (s.value) walkReferences(s.value, path, visit, depth + 1);
  // A pipe may wrap a whole base schema (v.pipe(v.object(...), ...)).
  if (Array.isArray(s.pipe) && s.pipe.length > 0) {
    walkReferences(s.pipe[0], path, visit, depth + 1);
  }
}

/** Where a reference to a space was first seen — for the report line. */
interface ReferenceSighting {
  readonly bucket: keyof DatabaseState;
  readonly collection: string;
  readonly path: string;
}

function buildCorrelationPlan(
  schemas: SchemasDefinition,
  uncorrelated: ReadonlySet<string>,
): CorrelationPlan {
  const targets = new Map<string, PlanTarget>();
  const referenced = new Map<string, ReferenceSighting>();

  const addTarget = (
    bucket: keyof DatabaseState,
    collection: string,
    type: string | undefined,
    fields: SchemaContent,
    autoKey: string | null,
  ) => {
    const space = targetIdSpace(fields._id, autoKey);
    if (space && !uncorrelated.has(space)) {
      targets.set(targetKey(bucket, collection, type), {
        bucket,
        collection,
        type,
        space,
      });
    }
    for (const [field, schema] of Object.entries(fields)) {
      if (field === "_id") continue;
      walkReferences(schema, field, (refSpace, refPath) => {
        if (!referenced.has(refSpace)) {
          referenced.set(refSpace, { bucket, collection, path: refPath });
        }
      }, 0);
    }
  };

  for (
    const [name, schema] of Object.entries(schemas.collections ?? {})
  ) {
    addTarget("collections", name, undefined, schema, null);
  }
  for (
    const [name, types] of Object.entries(schemas.multiCollections ?? {})
  ) {
    for (const [type, fields] of Object.entries(types)) {
      addTarget("multiCollections", name, type, fields, type);
    }
  }
  for (const [model, types] of Object.entries(schemas.multiModels ?? {})) {
    for (const [type, fields] of Object.entries(types)) {
      addTarget("multiModels", model, type, fields, type);
    }
  }

  const scopeSpaces = new Map<string, string>();
  const singletons = new Set<string>();
  for (
    const [name, scoped] of Object.entries(
      schemas.scopedMultiCollections ?? {},
    )
  ) {
    const scopeSpace = extractIdPrefix(scoped.scope);
    if (scopeSpace && !uncorrelated.has(scopeSpace)) {
      scopeSpaces.set(name, scopeSpace);
      // The scope schema is itself a stored reference to its space — a
      // scoped collection whose scope space nobody mints deserves the same
      // ownerless warning as any dangling reference field.
      if (!referenced.has(scopeSpace)) {
        referenced.set(scopeSpace, {
          bucket: "scopedMultiCollections",
          collection: name,
          path: "_scope",
        });
      }
    }
    for (const [type, fields] of Object.entries(scoped.types)) {
      addTarget("scopedMultiCollections", name, type, fields, type);
      const key = targetKey("scopedMultiCollections", name, type);
      const target = targets.get(key);
      if (target && target.space === scopeSpace) singletons.add(key);
    }
  }

  const findings: MockGenerationFailure[] = [];

  // Elect one pool contributor per space. Canonical bucket order, then name
  // order — deterministic, so two runs elect the same owner.
  const bySpace = new Map<string, string[]>();
  for (const [key, target] of targets) {
    const keys = bySpace.get(target.space) ?? [];
    keys.push(key);
    bySpace.set(target.space, keys);
  }
  const contributors = new Map<string, string>();
  for (const [space, keys] of bySpace) {
    keys.sort((a, b) => {
      const ta = targets.get(a)!;
      const tb = targets.get(b)!;
      const order = BUCKET_ORDER[ta.bucket] - BUCKET_ORDER[tb.bucket];
      return order !== 0 ? order : a.localeCompare(b);
    });
    const winner = keys[0];
    contributors.set(space, winner);
    if (keys.length > 1) {
      const target = targets.get(winner)!;
      findings.push({
        bucket: target.bucket,
        collection: target.collection,
        kind: "correlation",
        space,
        message:
          `Identifier space "${space}" is minted by ${keys.length} targets (${
            keys.join(", ")
          }) — correlated references draw from "${winner}" (deterministic ` +
          `tie-break). Declare "${space}" in uncorrelatedSpaces to silence ` +
          `this if the ambiguity is intended.`,
      });
    }
  }

  for (const [space, sighting] of referenced) {
    if (uncorrelated.has(space) || contributors.has(space)) continue;
    findings.push({
      bucket: sighting.bucket,
      collection: sighting.collection,
      kind: "correlation",
      space,
      message:
        `Identifier space "${space}" is referenced (e.g. field "${sighting.path}" ` +
        `of ${sighting.bucket} "${sighting.collection}") but no _id schema mints ` +
        `it — these references stay uncorrelated random values. Declare ` +
        `"${space}" in uncorrelatedSpaces to make that assumption explicit.`,
    });
  }

  return { targets, contributors, scopeSpaces, singletons, findings };
}

// ---------------------------------------------------------------------------
// Pools
// ---------------------------------------------------------------------------

/** The global (unscoped) pool key. */
const GLOBAL = "";

interface PoolEntry {
  readonly list: string[];
  readonly set: Set<string>;
}

/**
 * Identifier pools keyed on (space × scope) — NOT on the space alone. A
 * `participantId` inside scope `exposition:X` must come from a participant of
 * the SAME scope; a global pool per space would produce syntactically valid,
 * semantically absurd references and gain nothing. `scope: null` is the
 * global pool; a draw tries the scoped pool first, falls back to the global
 * one, and the caller signals when both are empty.
 */
function createIdPools() {
  const pools = new Map<string, Map<string, PoolEntry>>();

  function entry(space: string, scope: string | null): PoolEntry {
    let scopes = pools.get(space);
    if (!scopes) {
      scopes = new Map();
      pools.set(space, scopes);
    }
    const key = scope ?? GLOBAL;
    let e = scopes.get(key);
    if (!e) {
      e = { list: [], set: new Set() };
      scopes.set(key, e);
    }
    return e;
  }

  function register(space: string, scope: string | null, id: string): void {
    const e = entry(space, scope);
    if (!e.set.has(id)) {
      e.set.add(id);
      e.list.push(id);
    }
  }

  function listOf(space: string, scope: string | null): readonly string[] {
    return pools.get(space)?.get(scope ?? GLOBAL)?.list ?? [];
  }

  function draw(
    space: string,
    scope: string | null,
    pickIndex: (length: number) => number,
  ): string | undefined {
    const scoped = scope !== null ? listOf(space, scope) : [];
    const list = scoped.length > 0 ? scoped : listOf(space, null);
    if (list.length === 0) return undefined;
    return list[pickIndex(list.length)];
  }

  return { register, listOf, draw };
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

/** Small deterministic PRNG — replaces the engine's bare Math.random draws. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Options for {@link createCorrelationSession}. */
export interface CorrelationSessionOptions {
  /** The schemas whose population this session correlates. */
  schemas: SchemasDefinition;

  /**
   * Base seed for every random decision of the session — derive it from the
   * migration id so a simulation is stable per migration and different
   * between migrations. A simulation that cannot be replayed identically
   * cannot be diffed between two runs.
   */
  seed: number;

  /**
   * Identifier spaces assumed NOT correlated. The escape hatch is inverted
   * on purpose: declaring what to correlate would reproduce the original bug
   * the moment a declaration is forgotten, so one declares what is assumed
   * uncorrelated instead — the silence becomes a written choice, and every
   * undeclared hole keeps warning.
   */
  uncorrelatedSpaces?: readonly string[];
}

/** A mint request for one populate target (see {@link CorrelationSession.mintIds}). */
export interface MintRequest {
  bucket: keyof DatabaseState;

  /** Plan-level collection (the MODEL key for multiModels). */
  collection: string;

  type?: string;

  /** Per-document scope (null = unscoped), in append order. */
  scopes: readonly (string | null)[];
}

/** Target descriptor for {@link CorrelationSession.docOptions}. */
export interface DocTarget {
  bucket: keyof DatabaseState;

  /** Plan-level collection (the MODEL key for multiModels). */
  collection: string;

  type?: string;

  /** The scope the document lands in (null = unscoped). */
  scope: string | null;

  /** Pre-minted `_id` for this document, when the mint phase ran. */
  assignedId?: string;
}

/**
 * One correlated-generation session: the plan plus the mutable pools,
 * counters, and report accumulated while the population engine drives the
 * mint / realize / link phases. Created once per state-population operation.
 */
export interface CorrelationSession {
  /**
   * Registers the identifiers of EXISTING documents (real parent seeds,
   * retained documents) into the pools, so fresh references can point at
   * them and instance names can reuse real root ids. Idempotent — pools
   * deduplicate on registration.
   */
  harvest(state: DatabaseState): void;

  /**
   * Mints one `_id` per requested document for an owner target, registering
   * pool entries when the target is its space's contributor. Returns
   * `undefined` when the target's `_id` is not typed in a correlated space
   * (anonymous or declared-uncorrelated) — generation then proceeds as
   * before. For a root singleton the ids ARE the scopes.
   */
  mintIds(request: MintRequest): string[] | undefined;

  /**
   * Chooses `count` instance names for a model: real ids drawn from the
   * model-key space's pool when it holds any (that coincidence is what makes
   * a root↔instance merge branch executable), fresh `<model>:<id>` values
   * otherwise. Never returns a name in `taken`.
   */
  realizeInstanceNames(
    model: string,
    count: number,
    taken: ReadonlySet<string>,
  ): string[];

  /**
   * Chooses `count` `_scope` values for a scoped collection: existing
   * scope-space ids drawn (seeded shuffle, cycling when fewer exist than
   * asked), fresh values generated from the scope schema otherwise. Every
   * returned value is registered globally and as its own scope.
   */
  realizeScopes(
    collection: string,
    scopeSchema: unknown,
    count: number,
  ): string[];

  /**
   * True when the scoped collection contains a root-singleton type — the
   * collection mints its own scope entities, so its scopes must be realized
   * before sibling scoped collections draw from the same space.
   */
  realizesOwnScopes(collection: string): boolean;

  /**
   * Per-document generator options: a deterministic seed plus the `resolve`
   * hook implementing the link phase — `_id` injection and reference draws
   * from the (space × scope) pools.
   */
  docOptions(target: DocTarget): MockDocumentOptions;

  /** Seeded PRNG for the engine's volume draws (replaces Math.random). */
  random(): number;

  /**
   * Moves the accumulated report lines out of the session — plan findings on
   * the first drain, draw-time findings as they occur. The engine folds them
   * into the shared failure channel so they surface in
   * `ValidationResult.warnings`.
   */
  drainFindings(): MockGenerationFailure[];
}

/**
 * Stable fingerprint of a schemas definition — the seed input for
 * `prepareStateForNextMigration`, whose locked `(state, schemas)` signature
 * carries no migration id. Stable per migration; two migrations only collide
 * when they declare the exact same target keys, which consecutive migrations
 * in practice never do.
 */
export function schemasFingerprint(schemas: SchemasDefinition): string {
  const parts: string[] = [];
  for (const bucket of Object.keys(BUCKET_ORDER) as (keyof DatabaseState)[]) {
    const entries = schemas[bucket] ?? {};
    for (const name of Object.keys(entries).sort()) {
      if (bucket === "collections") {
        parts.push(`${bucket}:${name}`);
        continue;
      }
      const types = bucket === "scopedMultiCollections"
        ? (entries as NonNullable<SchemasDefinition["scopedMultiCollections"]>)[
          name
        ].types
        : (entries as NonNullable<SchemasDefinition["multiCollections"]>)[name];
      parts.push(`${bucket}:${name}(${Object.keys(types).sort().join(",")})`);
    }
  }
  return parts.join("|");
}

/**
 * Creates a correlated-generation session (see {@link CorrelationSession}).
 */
export function createCorrelationSession(
  options: CorrelationSessionOptions,
): CorrelationSession {
  const uncorrelated = new Set(options.uncorrelatedSpaces ?? []);
  const plan = buildCorrelationPlan(options.schemas, uncorrelated);
  const pools = createIdPools();
  const baseSeed = options.seed;
  const rng = mulberry32(baseSeed ^ 0x9e3779b9);

  /** Per-space set of every id ever minted or harvested — mint uniqueness. */
  const knownIds = new Map<string, Set<string>>();
  const counters = new Map<string, number>();
  const findings: MockGenerationFailure[] = [...plan.findings];
  const findingKeys = new Set<string>();

  function known(space: string): Set<string> {
    let set = knownIds.get(space);
    if (!set) {
      set = new Set();
      knownIds.set(space, set);
    }
    return set;
  }

  function bump(label: string): number {
    const n = counters.get(label) ?? 0;
    counters.set(label, n + 1);
    return n;
  }

  /**
   * Mints one fresh, unique id of a space through the ACTUAL refId shape —
   * seeded, so a replay mints the same values. Collisions are retried with a
   * salted seed; after the budget the (astronomically unlikely) duplicate is
   * accepted rather than looping forever.
   */
  function mintFreshId(space: string): string {
    const set = known(space);
    const n = bump(`mint:${space}`);
    let value = "";
    for (let attempt = 0; attempt < 8; attempt++) {
      const seed = fnv1a32(`${baseSeed}|mint|${space}|${n}|${attempt}`);
      value = String(generateMockScopeValue(refId(space), { seed }));
      if (!set.has(value)) break;
    }
    set.add(value);
    return value;
  }

  function reportEmptyPool(
    space: string,
    target: DocTarget,
    path: string,
  ): void {
    const key = `${space}|${target.bucket}|${target.collection}|${path}`;
    if (findingKeys.has(key)) return;
    findingKeys.add(key);
    findings.push({
      bucket: target.bucket,
      collection: target.collection,
      kind: "correlation",
      space,
      message: `Correlated draw found no "${space}" id for ${target.bucket} ` +
        `"${target.collection}" field "${path}"` +
        (target.scope !== null ? ` (scope "${target.scope}")` : "") +
        ` — an uncorrelated value was generated instead.`,
    });
  }

  function harvest(state: DatabaseState): void {
    const registerDoc = (
      key: string,
      scope: string | null,
      doc: Record<string, unknown>,
    ) => {
      const target = plan.targets.get(key);
      if (!target) return;
      const id = doc._id;
      if (typeof id !== "string" || !id.startsWith(`${target.space}:`)) return;
      known(target.space).add(id);
      if (plan.contributors.get(target.space) === key) {
        pools.register(target.space, scope, id);
      }
    };

    for (const [name, coll] of Object.entries(state.collections)) {
      const key = targetKey("collections", name, undefined);
      for (const doc of coll.content) registerDoc(key, null, doc);
    }
    for (const [name, coll] of Object.entries(state.multiCollections)) {
      for (const doc of coll.content) {
        registerDoc(
          targetKey("multiCollections", name, String(doc._type)),
          null,
          doc,
        );
      }
    }
    for (const [name, instance] of Object.entries(state.multiModels)) {
      // An instance name IS an entity id of the model-key space — a physical
      // `exposition:<id>` collection is the exposition. Register it so scope
      // realization and references can land on real instances.
      if (
        name.startsWith(`${instance.modelType}:`) &&
        !uncorrelated.has(instance.modelType)
      ) {
        known(instance.modelType).add(name);
        pools.register(instance.modelType, null, name);
        pools.register(instance.modelType, name, name);
      }
      for (const doc of instance.content) {
        registerDoc(
          targetKey("multiModels", instance.modelType, String(doc._type)),
          name,
          doc,
        );
      }
    }
    for (
      const [name, coll] of Object.entries(state.scopedMultiCollections)
    ) {
      const scopeSpace = plan.scopeSpaces.get(name);
      for (const doc of coll.content) {
        const scope = typeof doc._scope === "string" ? doc._scope : null;
        if (
          scopeSpace && scope !== null && scope.startsWith(`${scopeSpace}:`)
        ) {
          known(scopeSpace).add(scope);
          pools.register(scopeSpace, null, scope);
          pools.register(scopeSpace, scope, scope);
        }
        registerDoc(
          targetKey("scopedMultiCollections", name, String(doc._type)),
          scope,
          doc,
        );
      }
    }
  }

  function mintIds(request: MintRequest): string[] | undefined {
    const key = targetKey(request.bucket, request.collection, request.type);
    const target = plan.targets.get(key);
    if (!target) return undefined;
    const isContributor = plan.contributors.get(target.space) === key;
    const isSingleton = plan.singletons.has(key);
    const ids: string[] = [];
    for (const scope of request.scopes) {
      if (isSingleton && scope !== null) {
        // Root singleton: the document IS the scope's canonical record, so
        // its id is the scope value itself (already registered by realize).
        ids.push(scope);
        continue;
      }
      const id = mintFreshId(target.space);
      if (isContributor) pools.register(target.space, scope, id);
      ids.push(id);
    }
    return ids;
  }

  function seededShuffle(length: number): number[] {
    const order = Array.from({ length }, (_, i) => i);
    for (let i = length - 1; i > 0; i--) {
      const j = Math.floor(rng() * (i + 1));
      [order[i], order[j]] = [order[j], order[i]];
    }
    return order;
  }

  function realizeInstanceNames(
    model: string,
    count: number,
    taken: ReadonlySet<string>,
  ): string[] {
    const names: string[] = [];
    if (!uncorrelated.has(model)) {
      const candidates = pools.listOf(model, null).filter((id) =>
        !taken.has(id)
      );
      const order = seededShuffle(candidates.length);
      for (let i = 0; i < order.length && names.length < count; i++) {
        names.push(candidates[order[i]]);
      }
    }
    // Bounded: a degenerate id space (regex admitting a handful of values)
    // must not spin forever — past the budget, fall back to the historical
    // synthetic naming, which is always fresh.
    let attempts = 0;
    while (names.length < count && attempts++ < count * 20) {
      const fresh = mintFreshId(model);
      if (taken.has(fresh) || names.includes(fresh)) continue;
      if (!uncorrelated.has(model)) pools.register(model, null, fresh);
      names.push(fresh);
    }
    for (let i = 1; names.length < count; i++) {
      const fallback = `${model}:instance${i}`;
      if (!taken.has(fallback) && !names.includes(fallback)) {
        names.push(fallback);
      }
    }
    if (!uncorrelated.has(model)) {
      // Each realized name is its own scope: a reference to the model's
      // space INSIDE that instance must resolve to the enclosing instance.
      for (const name of names) pools.register(model, name, name);
    }
    return names;
  }

  function realizeScopes(
    collection: string,
    scopeSchema: unknown,
    count: number,
  ): string[] {
    const space = plan.scopeSpaces.get(collection) ?? "";
    const scopes: string[] = [];
    if (space) {
      const global = pools.listOf(space, null);
      if (global.length > 0) {
        const order = seededShuffle(global.length);
        for (let i = 0; i < count; i++) {
          scopes.push(global[order[i % order.length]]);
        }
      }
    }
    while (scopes.length < count) {
      const n = bump(`scope:${collection}`);
      let value = "";
      for (let attempt = 0; attempt < 8; attempt++) {
        const seed = fnv1a32(
          `${baseSeed}|scope|${collection}|${n}|${attempt}`,
        );
        // Generated from the ACTUAL scope schema so custom scope shapes stay
        // faithful; the refId case yields `space:<alnum>` like any real id.
        // deno-lint-ignore no-explicit-any
        value = String(generateMockScopeValue(scopeSchema as any, { seed }));
        if (!space || !known(space).has(value)) break;
      }
      if (space) known(space).add(value);
      scopes.push(value);
    }
    if (space) {
      for (const scope of scopes) {
        pools.register(space, null, scope);
        pools.register(space, scope, scope);
      }
    }
    return scopes;
  }

  function realizesOwnScopes(collection: string): boolean {
    const scopeSpace = plan.scopeSpaces.get(collection);
    if (!scopeSpace) return false;
    const contributor = plan.contributors.get(scopeSpace);
    if (!contributor) return false;
    const target = plan.targets.get(contributor);
    return target !== undefined &&
      target.bucket === "scopedMultiCollections" &&
      target.collection === collection &&
      plan.singletons.has(contributor);
  }

  function docOptions(target: DocTarget): MockDocumentOptions {
    const label = `${target.bucket}/${target.collection}/${target.type ?? ""}`;
    const seed = fnv1a32(`${baseSeed}|doc|${label}|${bump(`doc:${label}`)}`);

    const key = targetKey(target.bucket, target.collection, target.type);
    const planTarget = plan.targets.get(key);
    let assignedId = target.assignedId;
    if (assignedId === undefined && planTarget) {
      // Standalone engine calls skip the mint pre-pass; minting here keeps
      // their ids correlated too. The composite paths always pre-mint, so
      // this branch never double-mints there.
      assignedId = mintIds({
        bucket: target.bucket,
        collection: target.collection,
        type: target.type,
        scopes: [target.scope],
      })?.[0];
    }

    const resolve = (node: ResolveNode): unknown => {
      if (node.path === "_id") {
        return assignedId !== undefined ? assignedId : SKIP;
      }
      // Only bare piped strings are reference leaves; wrappers (optional,
      // nullable, union…) are SKIPped so their handlers keep deciding
      // presence — the inner string node is offered again afterwards.
      const nodeType = (node.schema as { type?: unknown })?.type;
      if (nodeType !== "string") return SKIP;
      const space = extractIdPrefix(node.schema);
      if (!space || !plan.contributors.has(space)) return SKIP;
      const id = pools.draw(
        space,
        target.scope,
        (length) => node.faker.number.int({ min: 0, max: length - 1 }),
      );
      if (id === undefined) {
        reportEmptyPool(space, target, node.path);
        return SKIP;
      }
      return id;
    };

    return { seed, resolve };
  }

  function drainFindings(): MockGenerationFailure[] {
    return findings.splice(0, findings.length);
  }

  return {
    harvest,
    mintIds,
    realizeInstanceNames,
    realizeScopes,
    realizesOwnScopes,
    docOptions,
    random: rng,
    drainFindings,
  };
}
