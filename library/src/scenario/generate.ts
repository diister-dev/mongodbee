import { type ResolveNode, SKIP } from "@diister/valibot-mock";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type MockGenerationFailure,
  type SchemaContent,
  type SchemasDefinition,
} from "../migration/types.ts";
import {
  createCorrelationSession,
  type DocTarget,
} from "../migration/validators/mock/correlation.ts";
import { generateMockDocument } from "../migration/validators/mock/generator.ts";
import { sanitizeForMongoDB } from "../sanitizer.ts";
import { extractIdPrefix, fnv1a32 } from "../migration/utils/seed-id.ts";
import {
  deterministicUlid,
  migrationTime,
} from "../migration/utils/transform-context.ts";
import {
  buildPrivacyPlan,
  type PrivacyPlan,
  type PrivacyTarget,
} from "../privacy/plan.ts";
import { KEEP, walkDocument } from "../privacy/walk.ts";
import * as v from "../schema.ts";
import type {
  ScenarioViolation,
  ScenarioViolationKind,
  SeedAnchors,
  SeedCount,
  SeedFinalize,
  SeedFinalizeContext,
  SeedFinalizeEntry,
  SeedLayer,
  SeedRandom,
  SeedRule,
  SeedScenario,
  SeedShapeContext,
  SeedShapeEntry,
  SeedWorld,
  SeedWorldQuery,
} from "./types.ts";
import {
  bucketContent,
  docsOf,
  fieldsOfTarget,
  resolveTargetKey,
  targetFields,
} from "./state.ts";
import { placeValueAt, setValueAt, valueAt } from "./doc-path.ts";
import {
  createDocLookup,
  mirrorExpectations,
  referencePathsTo,
} from "./mirror.ts";
import { partitionedDocs } from "./unique.ts";
import { scopeDimension, scopedDocs } from "./scope.ts";
import {
  isDeclaredPath,
  isRequiredPath,
  unknownKeyPaths,
} from "./schema-walk.ts";
import {
  type UniqueKey,
  type UniquePartition,
  uniqueEntriesOf,
  uniqueKeysOfTarget,
} from "../privacy/unique-keys.ts";

const UNIQUE_ATTEMPTS = 64;
const ON_DEMAND_COUNT = 1;
const BUCKET_ORDER: Record<keyof DatabaseState, number> = {
  collections: 0,
  multiCollections: 1,
  multiModels: 2,
  scopedMultiCollections: 3,
};

export interface GenerateScenarioOptions {
  readonly schemas: SchemasDefinition;
  readonly scenario: SeedScenario;
  readonly stage?: string;
  readonly defaultCount?: number;
  readonly defaultScopes?: number;
  readonly initial?: DatabaseState;
}

export interface GenerateScenarioResult {
  readonly state: DatabaseState;
  readonly plan: PrivacyPlan;
  readonly violations: ScenarioViolation[];
  readonly onDemand: Record<string, number>;
}

interface Batch {
  readonly scope: string | null;
  readonly parent?: Record<string, unknown>;
  readonly count: number;
}

interface PinnedReference {
  readonly path: string;
  readonly value: string;
  readonly fromParent: boolean;
}

interface Finalizer {
  readonly run: SeedFinalize;
  readonly deferred: boolean;
}

type FinalizeSite = Omit<SeedFinalizeContext, "doc">;

interface DeferredFinalize {
  readonly target: PrivacyTarget;
  readonly fields: SchemaContent;
  readonly doc: Record<string, unknown>;
  readonly content: Record<string, unknown>[];
  readonly site: FinalizeSite;
  readonly pinned: readonly PinnedReference[];
  readonly run: SeedFinalize;
}

function uniqueEntries(
  keys: readonly UniqueKey[],
  doc: Record<string, unknown>,
  partition: UniquePartition,
): string[] {
  return keys.flatMap((key) => {
    const result = uniqueEntriesOf(key, doc, partition);
    if (result.covered !== undefined) {
      return result.covered ? result.entries : [];
    }
    const { partialFilter: _unknown, ...unfiltered } = key;
    const conservative = uniqueEntriesOf(unfiltered, doc, partition);
    return conservative.covered ? conservative.entries : [];
  });
}

function injectAnchors(
  state: DatabaseState,
  anchors: SeedAnchors | undefined,
): void {
  if (!anchors) return;
  for (const [name, docs] of Object.entries(anchors.collections ?? {})) {
    state.collections[name] ??= { content: [] };
    state.collections[name].content.push(...docs.map((d) => ({ ...d })));
  }
  for (const [name, docs] of Object.entries(anchors.multiCollections ?? {})) {
    state.multiCollections[name] ??= { content: [] };
    state.multiCollections[name].content.push(...docs.map((d) => ({ ...d })));
  }
  for (const [name, docs] of Object.entries(
    anchors.scopedMultiCollections ?? {},
  )) {
    state.scopedMultiCollections[name] ??= { content: [] };
    state.scopedMultiCollections[name].content.push(
      ...docs.map((d) => ({ ...d })),
    );
  }
  for (const [name, instance] of Object.entries(anchors.multiModels ?? {})) {
    state.multiModels[name] ??= { modelType: instance.modelType, content: [] };
    state.multiModels[name].content.push(
      ...instance.content.map((d) => ({ ...d })),
    );
  }
}

function rank(target: PrivacyTarget): number {
  if (target.person) return target.delegatesTo.length > 0 ? 1 : 0;
  return 2 + target.owner.chain.length;
}

function orderedTargets(plan: PrivacyPlan): PrivacyTarget[] {
  const all = [...plan.targets.values()];
  const canonical = new Map(all.map((t, i) => [t.key, i]));
  return all.sort((a, b) => {
    const r = rank(a) - rank(b);
    return r !== 0 ? r : canonical.get(a.key)! - canonical.get(b.key)!;
  });
}

function countOf(count: SeedCount, context: SeedShapeContext): number {
  const n = typeof count === "function" ? count(context) : count;
  return Math.max(0, Math.floor(n));
}

function finalizerOf(entry: SeedFinalizeEntry): Finalizer {
  return typeof entry === "function"
    ? { run: entry, deferred: false }
    : { run: entry.run, deferred: true };
}

function layerOf(scenario: SeedScenario, stage: string | undefined): SeedLayer {
  if (stage === undefined) return scenario;
  const stages = scenario.stages ?? {};
  if (!Object.hasOwn(stages, stage)) {
    throw new Error(`scenario "${scenario.name}": no stage "${stage}"`);
  }
  return stages[stage];
}

function contributorOrder(a: PrivacyTarget, b: PrivacyTarget): number {
  const order = BUCKET_ORDER[a.bucket] - BUCKET_ORDER[b.bucket];
  return order !== 0 ? order : a.key.localeCompare(b.key);
}

export async function generateScenarioState(
  options: GenerateScenarioOptions,
): Promise<GenerateScenarioResult> {
  const { schemas, scenario, stage } = options;
  const layer = layerOf(scenario, stage);
  const label =
    stage === undefined
      ? `scenario "${scenario.name}"`
      : `scenario "${scenario.name}" stage "${stage}"`;
  const defaultCount =
    stage === undefined
      ? (scenario.defaultCount ?? options.defaultCount ?? 0)
      : 0;
  const defaultScopes = options.defaultScopes ?? 3;
  const plan = buildPrivacyPlan({ schemas });
  const state = options.initial ?? createEmptyDatabaseState();
  injectAnchors(state, layer.anchors);
  const lookup = createDocLookup(state, plan);

  const baseSeed = scenario.seed ?? fnv1a32(scenario.name);
  const seed =
    stage === undefined ? baseSeed : fnv1a32(`${baseSeed}|stage|${stage}`);
  const refDate =
    layer.refDate ?? new Date(migrationTime(stage ?? scenario.birth));
  const refTime = refDate.getTime();
  const session = createCorrelationSession({
    schemas,
    seed,
    ...(scenario.uncorrelatedSpaces !== undefined && {
      uncorrelatedSpaces: scenario.uncorrelatedSpaces,
    }),
    mintId: (space, index, attempt) =>
      `${space}:${deterministicUlid(
        `${seed}|${space}|${index}|${attempt}`,
        refTime + index,
      )}`,
  });
  session.harvest(state);

  const tally = new Map<string, ScenarioViolation>();
  const report = (
    kind: ScenarioViolationKind,
    target: string,
    message: string,
  ): void => {
    const key = `${kind}|${target}|${message}`;
    const seen = tally.get(key);
    tally.set(key, {
      kind,
      target,
      message,
      count: (seen?.count ?? 0) + 1,
    });
  };

  const targetOf = (key: string): PrivacyTarget =>
    plan.targets.get(resolveTargetKey(plan, key))!;
  const fieldsFor = (target: PrivacyTarget): SchemaContent => {
    const fields = fieldsOfTarget(schemas, target);
    if (!fields) throw new Error(`${label}: no schema for "${target.key}"`);
    return fields;
  };

  const shape = new Map<string, SeedShapeEntry>();
  for (const [key, entry] of Object.entries(layer.shape ?? {})) {
    shape.set(resolveTargetKey(plan, key), entry);
  }
  const rules = new Map<string, Record<string, SeedRule>>();
  for (const [key, entry] of Object.entries(layer.rules ?? {})) {
    const target = targetOf(key);
    const fields = fieldsFor(target);
    for (const path of Object.keys(entry)) {
      if (!isDeclaredPath(fields, path)) {
        throw new Error(
          `${label}: rule "${key}" names "${path}", which is not a path of ${target.key}`,
        );
      }
    }
    rules.set(target.key, entry);
  }
  const finalizers = new Map<string, Finalizer>();
  for (const [key, entry] of Object.entries(layer.finalize ?? {})) {
    finalizers.set(resolveTargetKey(plan, key), finalizerOf(entry));
  }

  const parentOf = (target: PrivacyTarget): PrivacyTarget | undefined => {
    const entry = shape.get(target.key);
    if (
      entry === undefined ||
      typeof entry !== "object" ||
      entry.per === "scope"
    ) {
      return undefined;
    }
    return targetOf(entry.per);
  };
  const parentLinkOf = new Map<string, string>();
  for (const key of shape.keys()) {
    const target = plan.targets.get(key)!;
    const parent = parentOf(target);
    if (parent === undefined || parent.key === target.key) continue;
    const link = parent.space
      ? referencePathsTo(target, parent.space)[0]
      : undefined;
    if (link === undefined) {
      throw new Error(
        `${label}: "${key}" is shaped per "${parent.key}" but has no reference path to "${parent.space || parent.key}"`,
      );
    }
    parentLinkOf.set(target.key, link);
  }

  const random = () => session.random();
  const helpers: SeedRandom = {
    random,
    int: (min, max) => min + Math.floor(random() * (max - min + 1)),
    chance: (probability) => random() < probability,
    oneOf: (values) => values[Math.floor(random() * values.length)],
    weighted: (choices) => {
      const total = choices.reduce((sum, [, weight]) => sum + weight, 0);
      let cursor = random() * total;
      for (const [value, weight] of choices) {
        cursor -= weight;
        if (cursor <= 0) return value;
      }
      return choices[choices.length - 1][0];
    },
    dateBetween: (from, to) => {
      const a = new Date(from).getTime();
      const b = new Date(to).getTime();
      return new Date(a + random() * (b - a));
    },
  };
  const worldFor = (
    current: PrivacyTarget | undefined,
    scope: string | null,
  ): SeedWorld => {
    const dimension =
      current === undefined ? undefined : scopeDimension(schemas, current);
    const docs = (key: string, query?: SeedWorldQuery) => {
      const target = targetOf(key);
      if (
        query?.acrossScopes === true ||
        scope === null ||
        dimension === undefined ||
        scopeDimension(schemas, target) !== dimension
      ) {
        return docsOf(state, target);
      }
      return scopedDocs(state, schemas, target)
        .filter((located) => located.scope?.value === scope)
        .map((located) => located.doc);
    };
    return {
      ...helpers,
      docs,
      pick: (key, filter, query) => {
        const all = docs(key, query);
        const list = filter ? all.filter(filter) : all;
        if (list.length === 0) return undefined;
        return list[Math.floor(random() * list.length)];
      },
    };
  };

  const scopeSpaceOf = (collection: string): string =>
    extractIdPrefix(schemas.scopedMultiCollections![collection].scope);
  const singletonOf = new Map<string, PrivacyTarget | undefined>();
  for (const collection of Object.keys(schemas.scopedMultiCollections ?? {})) {
    singletonOf.set(
      collection,
      [...plan.targets.values()].find(
        (t) =>
          t.bucket === "scopedMultiCollections" &&
          t.collection === collection &&
          t.space === scopeSpaceOf(collection),
      ),
    );
  }

  const active = new Set<string>();
  for (const target of plan.targets.values()) {
    if (shape.has(target.key) || defaultCount > 0) active.add(target.key);
  }
  const demanded = new Set<string>();
  const activateSingletons = (): void => {
    for (const key of [...active]) {
      const target = plan.targets.get(key)!;
      if (target.bucket !== "scopedMultiCollections") continue;
      const singleton = singletonOf.get(target.collection);
      if (singleton) active.add(singleton.key);
    }
  };
  const satisfied = (space: string): boolean => {
    for (const target of plan.targets.values()) {
      if (target.space !== space) continue;
      if (active.has(target.key) || docsOf(state, target).length > 0) {
        return true;
      }
    }
    for (const key of active) {
      const target = plan.targets.get(key)!;
      if (
        (target.bucket === "scopedMultiCollections" &&
          scopeSpaceOf(target.collection) === space) ||
        (target.bucket === "multiModels" && target.collection === space)
      ) {
        return true;
      }
    }
    return session.pooledIds(space).length > 0;
  };
  const minterOf = (space: string): PrivacyTarget | undefined =>
    [...plan.targets.values()]
      .filter((t) => t.space === space)
      .sort(contributorOrder)[0];
  for (let changed = true; changed; ) {
    changed = false;
    activateSingletons();
    for (const key of [...active]) {
      const target = plan.targets.get(key)!;
      const fields = fieldsOfTarget(schemas, target);
      if (!fields) continue;
      for (const cls of target.paths) {
        if (
          cls.role !== "reference" ||
          cls.path === "_id" ||
          cls.path.includes("*") ||
          cls.spaces.length === 0 ||
          !isRequiredPath(fields, cls.path) ||
          cls.spaces.some(satisfied)
        ) {
          continue;
        }
        const minter = cls.spaces.map(minterOf).find((t) => t !== undefined);
        if (minter === undefined || active.has(minter.key)) continue;
        active.add(minter.key);
        demanded.add(minter.key);
        changed = true;
      }
    }
  }

  const inScope = (
    parentTarget: PrivacyTarget,
    parent: Record<string, unknown>,
    scope: string,
  ): boolean => {
    if (parentTarget.bucket === "scopedMultiCollections") {
      return parent._scope === scope;
    }
    if (parentTarget.bucket === "multiModels") {
      return state.multiModels[scope]?.content.includes(parent) ?? false;
    }
    return true;
  };

  const batchesFor = (target: PrivacyTarget, scope: string | null): Batch[] => {
    const entry = shape.get(target.key);
    if (entry === undefined) {
      const count = demanded.has(target.key) ? ON_DEMAND_COUNT : defaultCount;
      return [{ scope, count }];
    }
    if (typeof entry === "number" || typeof entry === "function") {
      return [{ scope, count: countOf(entry, { scope, random }) }];
    }
    if (entry.per === "scope") {
      return [{ scope, count: countOf(entry.count, { scope, random }) }];
    }
    const parentTarget = targetOf(entry.per);
    const parents = docsOf(state, parentTarget).filter(
      (p) => scope === null || inScope(parentTarget, p, scope),
    );
    return parents.map((parent) => ({
      scope,
      parent,
      count: countOf(entry.count, { parent, scope, random }),
    }));
  };

  const pinnedFor = (
    target: PrivacyTarget,
    parent: Record<string, unknown> | undefined,
  ): PinnedReference[] => {
    const link = parentLinkOf.get(target.key);
    const parentTarget = parentOf(target);
    if (
      parent === undefined ||
      link === undefined ||
      parentTarget === undefined ||
      typeof parent._id !== "string"
    ) {
      return [];
    }
    const pinned: PinnedReference[] = [
      { path: link, value: parent._id, fromParent: true },
    ];
    for (const cls of target.paths) {
      if (
        cls.role !== "reference" ||
        cls.path === link ||
        cls.path === "_id" ||
        cls.path.includes("*")
      ) {
        continue;
      }
      const twin = parentTarget.paths.find(
        (p) => p.path === cls.path && p.role === "reference",
      );
      const value = twin ? valueAt(parent, cls.path) : undefined;
      if (
        typeof value === "string" &&
        cls.spaces.includes(value.split(":")[0])
      ) {
        pinned.push({ path: cls.path, value, fromParent: false });
      }
    }
    return pinned;
  };

  const verify = (
    target: PrivacyTarget,
    fields: SchemaContent,
    doc: Record<string, unknown>,
    pinned: readonly PinnedReference[],
    origin: string,
  ): Record<string, unknown> | undefined => {
    const unknown = unknownKeyPaths(fields, doc);
    if (unknown.length > 0) {
      report(
        "generation",
        target.key,
        `${origin} a document with unknown key(s) ${unknown
          .map((path) => `"${path}"`)
          .join(", ")}`,
      );
      return undefined;
    }
    const parsed = v.safeParse(v.object(fields as v.ObjectEntries), doc);
    if (!parsed.success) {
      const issue = parsed.issues[0];
      const where = issue.path?.map((p) => String(p.key)).join(".") ?? "";
      report(
        "generation",
        target.key,
        `${origin} an invalid document at "${where}": ${issue.message}`,
      );
      return undefined;
    }
    for (const pin of pinned) {
      if (pin.fromParent && valueAt(doc, pin.path) !== pin.value) {
        report(
          "generation",
          target.key,
          `${origin} a document whose "${pin.path}" no longer points at its "per" parent`,
        );
        return undefined;
      }
    }
    return doc;
  };

  const settle = async (
    target: PrivacyTarget,
    fields: SchemaContent,
    doc: Record<string, unknown>,
    run: SeedFinalize,
    site: FinalizeSite,
    pinned: readonly PinnedReference[],
  ): Promise<Record<string, unknown> | undefined> => {
    const finalized = (await run({ ...site, doc })) ?? doc;
    return verify(
      target,
      fields,
      sanitizeForMongoDB(finalized) as Record<string, unknown>,
      pinned,
      "finalize produced",
    );
  };

  const deferred: DeferredFinalize[] = [];

  const generateDoc = async (
    target: PrivacyTarget,
    fields: SchemaContent,
    docTarget: DocTarget,
    batch: Batch,
    pinned: readonly PinnedReference[],
    index: number,
    ordinal: number,
  ): Promise<
    { doc: Record<string, unknown>; site: FinalizeSite } | undefined
  > => {
    const base = session.docOptions(docTarget);
    const targetRules = rules.get(target.key);
    const world = worldFor(target, batch.scope);
    const pinnedAt = new Map(pinned.map((pin) => [pin.path, pin]));
    const site: FinalizeSite = {
      ...world,
      target: target.key,
      scope: batch.scope,
      parent: batch.parent,
      index,
      ordinal,
      count: batch.count,
    };
    const resolve = (node: ResolveNode): unknown => {
      (
        node.faker as { setDefaultRefDate?: (d: Date) => void }
      ).setDefaultRefDate?.(refDate);
      const rule = targetRules?.[node.path];
      const pin = pinnedAt.get(node.path);
      if (rule) {
        const value = rule({ ...site, path: node.path, faker: node.faker });
        if (value !== SKIP) {
          if (pin?.fromParent && value !== pin.value) {
            throw new Error(
              `rule "${node.path}" returned a value other than the id of the "per" parent it must point at`,
            );
          }
          return value;
        }
      }
      if (pin) return pin.value;
      return base.resolve ? base.resolve(node) : SKIP;
    };
    try {
      const doc = generateMockDocument(fields, { ...base, resolve });
      for (const pin of pinned) {
        if (
          valueAt(doc, pin.path) === undefined &&
          (pin.fromParent || !targetRules?.[pin.path])
        ) {
          placeValueAt(doc, pin.path, pin.value);
        }
      }
      for (const { path, candidates } of mirrorExpectations(
        target,
        doc,
        lookup,
      )) {
        if (valueAt(doc, path) !== undefined) {
          setValueAt(doc, path, candidates[0]);
        }
      }
      if (doc._id === undefined && docTarget.assignedId !== undefined) {
        doc._id = docTarget.assignedId;
      }
      const finalizer = finalizers.get(target.key);
      const settled =
        finalizer === undefined || finalizer.deferred
          ? verify(target, fields, doc, pinned, "the generator produced")
          : await settle(target, fields, doc, finalizer.run, site, pinned);
      return settled === undefined ? undefined : { doc: settled, site };
    } catch (error) {
      report(
        "generation",
        target.key,
        error instanceof Error ? error.message : String(error),
      );
      return undefined;
    }
  };

  interface Minted {
    readonly batch: Batch;
    readonly ids: string[] | undefined;
  }

  const mintBatches = (
    target: PrivacyTarget,
    batches: readonly Batch[],
  ): Minted[] =>
    batches.map((batch) => ({
      batch,
      ids:
        batch.count === 0
          ? []
          : session.mintIds({
              bucket: target.bucket,
              collection: target.collection,
              ...(target.type !== undefined && { type: target.type }),
              scopes: Array.from({ length: batch.count }, () => batch.scope),
            }),
    }));

  const ordinals = new Map<string, number>();
  const nextOrdinal = (key: string): number => {
    const n = ordinals.get(key) ?? 0;
    ordinals.set(key, n + 1);
    return n;
  };
  const produced = new Map<string, number>();

  const contentOf = (
    target: PrivacyTarget,
    scope: string | null,
  ): Record<string, unknown>[] =>
    target.bucket === "multiModels"
      ? state.multiModels[scope!].content
      : bucketContent(state, target);

  const partitionOf = (
    target: PrivacyTarget,
    scope: string | null,
  ): UniquePartition =>
    target.bucket === "multiModels"
      ? { instance: scope!, scope: "" }
      : { instance: "", scope: scope ?? "" };

  const generateBatches = async (
    target: PrivacyTarget,
    fields: SchemaContent,
    minted: readonly Minted[],
  ): Promise<void> => {
    const unique = uniqueKeysOfTarget(schemas, target);
    const taken = new Set(
      partitionedDocs(state, target).flatMap(({ doc, partition }) =>
        uniqueEntries(unique, doc, partition),
      ),
    );
    const finalizer = finalizers.get(target.key);
    for (const { batch, ids } of minted) {
      if (batch.count === 0) continue;
      const content = contentOf(target, batch.scope);
      const partition = partitionOf(target, batch.scope);
      const pinned = pinnedFor(target, batch.parent);
      for (let i = 0; i < batch.count; i++) {
        const ordinal = nextOrdinal(target.key);
        const generate = () =>
          generateDoc(
            target,
            fields,
            {
              bucket: target.bucket,
              collection: target.collection,
              ...(target.type !== undefined && { type: target.type }),
              scope: batch.scope,
              ...(ids?.[i] !== undefined && { assignedId: ids[i] }),
            },
            batch,
            pinned,
            i,
            ordinal,
          );
        let draft = await generate();
        for (
          let attempt = 1;
          draft &&
          attempt < UNIQUE_ATTEMPTS &&
          uniqueEntries(unique, draft.doc, partition).some((key) =>
            taken.has(key),
          );
          attempt++
        ) {
          draft = await generate();
        }
        if (!draft) continue;
        for (const key of uniqueEntries(unique, draft.doc, partition)) {
          taken.add(key);
        }
        const stored = {
          ...draft.doc,
          ...(target.type !== undefined && { _type: target.type }),
          ...(batch.scope !== null &&
            target.bucket !== "multiModels" && { _scope: batch.scope }),
        };
        content.push(stored);
        produced.set(target.key, (produced.get(target.key) ?? 0) + 1);
        if (finalizer?.deferred) {
          deferred.push({
            target,
            fields,
            doc: stored,
            content,
            site: draft.site,
            pinned,
            run: finalizer.run,
          });
        }
      }
    }
  };

  const runDeferred = async (): Promise<void> => {
    const dropped = new Map<Record<string, unknown>[], Set<unknown>>();
    const drop = (job: DeferredFinalize) => {
      const set = dropped.get(job.content) ?? new Set();
      set.add(job.doc);
      dropped.set(job.content, set);
      produced.set(job.target.key, (produced.get(job.target.key) ?? 1) - 1);
    };
    for (const job of deferred) {
      const { _type, _scope, ...own } = job.doc;
      let settled: Record<string, unknown> | undefined;
      try {
        settled = await settle(
          job.target,
          job.fields,
          own,
          job.run,
          job.site,
          job.pinned,
        );
      } catch (error) {
        report(
          "generation",
          job.target.key,
          error instanceof Error ? error.message : String(error),
        );
      }
      if (settled === undefined) {
        drop(job);
        continue;
      }
      if (settled._id !== own._id) {
        report(
          "generation",
          job.target.key,
          "a deferred finalize changed the document's _id",
        );
        drop(job);
        continue;
      }
      for (const key of Object.keys(job.doc)) delete job.doc[key];
      Object.assign(job.doc, settled, {
        ...(_type !== undefined && { _type }),
        ...(_scope !== undefined && { _scope }),
      });
    }
    for (const [content, docs] of dropped) {
      const kept = content.filter((doc) => !docs.has(doc));
      content.splice(0, content.length, ...kept);
    }
  };

  const ordered = (() => {
    const base = orderedTargets(plan);
    const out: PrivacyTarget[] = [];
    const placed = new Set<string>();
    const place = (target: PrivacyTarget, trail: Set<string>) => {
      if (placed.has(target.key)) return;
      if (trail.has(target.key)) {
        throw new Error(
          `${label}: circular "per" dependency through ${target.key}`,
        );
      }
      const dep = parentOf(target);
      if (dep !== undefined && dep.key !== target.key) {
        place(dep, new Set([...trail, target.key]));
      }
      placed.add(target.key);
      out.push(target);
    };
    for (const target of base) place(target, new Set());
    return out.filter((target) => active.has(target.key));
  })();

  const scopesOf = new Map<string, string[]>();
  const realizeAllScopes = (): void => {
    const collections = new Set(
      ordered
        .filter((t) => t.bucket === "scopedMultiCollections")
        .map((t) => t.collection),
    );
    for (const collection of collections) {
      const scoped = schemas.scopedMultiCollections![collection];
      const singleton = singletonOf.get(collection);
      let scopes = [...session.pooledIds(scopeSpaceOf(collection))];
      if (scopes.length === 0) {
        const entry = singleton ? shape.get(singleton.key) : undefined;
        const wanted =
          entry !== undefined &&
          (typeof entry === "number" || typeof entry === "function")
            ? countOf(entry, { scope: null, random })
            : defaultScopes;
        scopes = session.realizeScopes(collection, scoped.scope, wanted);
      }
      scopesOf.set(collection, scopes);
    }
  };

  const instancesOf = new Map<string, string[]>();
  const realizeAllInstances = (): void => {
    const models = new Set(
      ordered
        .filter((t) => t.bucket === "multiModels")
        .map((t) => t.collection),
    );
    for (const model of models) {
      const existing = Object.entries(state.multiModels)
        .filter(([, instance]) => instance.modelType === model)
        .map(([name]) => name);
      const taken = new Set(existing);
      const pooled = session.pooledIds(model).filter((id) => !taken.has(id));
      const wanted =
        pooled.length > 0
          ? pooled.length
          : existing.length > 0
            ? 0
            : defaultScopes;
      const names = session.realizeInstanceNames(model, wanted, taken);
      for (const name of names) {
        state.multiModels[name] = { modelType: model, content: [] };
      }
      instancesOf.set(model, [...existing, ...names]);
    }
  };

  const batchesOf = (target: PrivacyTarget): Batch[] => {
    if (target.bucket === "multiModels") {
      return (instancesOf.get(target.collection) ?? []).flatMap((name) =>
        batchesFor(target, name),
      );
    }
    if (target.bucket !== "scopedMultiCollections") {
      return batchesFor(target, null);
    }
    const scopes = scopesOf.get(target.collection) ?? [];
    if (target === singletonOf.get(target.collection)) {
      const present = new Set(docsOf(state, target).map((d) => d._scope));
      return scopes
        .filter((scope) => !present.has(scope))
        .map((scope) => ({ scope, count: 1 }));
    }
    return scopes.flatMap((scope) => batchesFor(target, scope));
  };

  const isSingleton = (t: PrivacyTarget): boolean =>
    t.bucket === "scopedMultiCollections" &&
    singletonOf.get(t.collection) === t;
  const scopedFirst = (a: PrivacyTarget, b: PrivacyTarget): number =>
    (isSingleton(a) ? 0 : 1) - (isSingleton(b) ? 0 : 1);
  const independent = ordered.filter((t) => parentOf(t) === undefined);
  const minted = new Map<string, Minted[]>();
  const instanceBound = (t: PrivacyTarget): boolean =>
    t.bucket === "scopedMultiCollections" || t.bucket === "multiModels";
  for (const target of independent.filter((t) => !instanceBound(t))) {
    minted.set(target.key, mintBatches(target, batchesOf(target)));
  }
  realizeAllScopes();
  realizeAllInstances();
  for (const target of independent.filter(instanceBound)) {
    minted.set(target.key, mintBatches(target, batchesOf(target)));
  }
  for (const target of [...ordered].sort(scopedFirst)) {
    const fields = targetFields(schemas, target);
    const batches =
      minted.get(target.key) ?? mintBatches(target, batchesOf(target));
    await generateBatches(target, fields, batches);
  }
  await runDeferred();

  await layer.after?.({ ...worldFor(undefined, null), state });

  const violations: ScenarioViolation[] = [...tally.values()];
  const findings = session
    .drainFindings()
    .filter((f) => keepFinding(f, state, schemas, plan));
  violations.push(
    ...findings.map((f) => ({
      kind: "correlation" as const,
      target: `${f.bucket}/${f.collection}/${f.modelType ?? ""}`,
      message: f.message,
      blocking: f.correlation !== "contested_space",
    })),
  );
  const onDemand: Record<string, number> = {};
  for (const key of demanded) onDemand[key] = produced.get(key) ?? 0;
  return { state, plan, violations, onDemand };
}

function keepFinding(
  finding: MockGenerationFailure,
  state: DatabaseState,
  schemas: SchemasDefinition,
  plan: PrivacyPlan,
): boolean {
  if (finding.correlation === "empty_pool") return false;
  if (
    finding.correlation === "contested_space" ||
    finding.space === undefined
  ) {
    return true;
  }
  return holdsReference(state, schemas, plan, finding.space);
}

function holdsReference(
  state: DatabaseState,
  schemas: SchemasDefinition,
  plan: PrivacyPlan,
  space: string,
): boolean {
  for (const [collection, scoped] of Object.entries(
    schemas.scopedMultiCollections ?? {},
  )) {
    if (
      extractIdPrefix(scoped.scope) === space &&
      (state.scopedMultiCollections[collection]?.content.length ?? 0) > 0
    ) {
      return true;
    }
  }
  for (const target of plan.targets.values()) {
    const paths = new Set(
      target.paths
        .filter((p) => p.role === "reference" && p.spaces.includes(space))
        .map((p) => p.path),
    );
    if (paths.size === 0) continue;
    const fields = fieldsOfTarget(schemas, target);
    if (!fields) continue;
    let found = false;
    for (const doc of docsOf(state, target)) {
      const { _id: _i, _scope: _s, _type: _t, ...rest } = doc;
      walkDocument(fields, rest, (leaf) => {
        if (paths.has(leaf.path) && leaf.value !== undefined) found = true;
        return KEEP;
      });
      if (found) return true;
    }
  }
  return false;
}
