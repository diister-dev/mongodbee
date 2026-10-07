import { type ResolveNode, SKIP } from "@diister/valibot-mock";
import {
  createEmptyDatabaseState,
  type DatabaseState,
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
import * as v from "../schema.ts";
import type {
  ScenarioViolation,
  SeedAnchors,
  SeedCount,
  SeedFinalize,
  SeedRandom,
  SeedRule,
  SeedScenario,
  SeedShapeContext,
  SeedShapeEntry,
  SeedWorld,
} from "./types.ts";
import {
  bucketContent,
  docsOf,
  resolveTargetKey,
  targetFields,
} from "./state.ts";
import { setValueAt, valueAt } from "./doc-path.ts";
import { createDocLookup, mirrorExpectations } from "./mirror.ts";
import {
  partitionedDocs,
  uniqueIndexesOf,
  uniqueKeysOf,
  type UniquePartition,
} from "./unique.ts";

const UNIQUE_ATTEMPTS = 64;

export interface GenerateScenarioOptions {
  readonly schemas: SchemasDefinition;
  readonly scenario: SeedScenario;
  readonly defaultCount?: number;
  readonly defaultScopes?: number;
  readonly initial?: DatabaseState;
}

export interface GenerateScenarioResult {
  readonly state: DatabaseState;
  readonly plan: PrivacyPlan;
  readonly violations: ScenarioViolation[];
}

interface Batch {
  readonly scope: string | null;
  readonly parent?: Record<string, unknown>;
  readonly count: number;
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

export function generateScenarioState(
  options: GenerateScenarioOptions,
): GenerateScenarioResult {
  const { schemas, scenario } = options;
  const defaultCount = options.defaultCount ?? 10;
  const defaultScopes = options.defaultScopes ?? 3;
  const plan = buildPrivacyPlan({ schemas });
  const state = options.initial ?? createEmptyDatabaseState();
  injectAnchors(state, scenario.anchors);
  const lookup = createDocLookup(state, plan);

  const seed = scenario.seed ?? fnv1a32(scenario.name);
  const refDate = scenario.refDate ?? new Date(migrationTime(scenario.birth));
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

  const violations: ScenarioViolation[] = [];
  const shape = new Map<string, SeedShapeEntry>();
  for (const [key, entry] of Object.entries(scenario.shape ?? {})) {
    shape.set(resolveTargetKey(plan, key), entry);
  }
  const rules = new Map<string, Record<string, SeedRule>>();
  for (const [key, entry] of Object.entries(scenario.rules ?? {})) {
    rules.set(resolveTargetKey(plan, key), entry);
  }

  const targetOf = (key: string): PrivacyTarget =>
    plan.targets.get(resolveTargetKey(plan, key))!;
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
  const world: SeedWorld = {
    ...helpers,
    docs: (key) => docsOf(state, targetOf(key)),
    pick: (key, filter) => {
      const all = docsOf(state, targetOf(key));
      const list = filter ? all.filter(filter) : all;
      if (list.length === 0) return undefined;
      return list[Math.floor(random() * list.length)];
    },
  };
  const finalizers = new Map<string, SeedFinalize>();
  for (const [key, entry] of Object.entries(scenario.finalize ?? {})) {
    finalizers.set(resolveTargetKey(plan, key), entry);
  }

  const referencePath = (
    target: PrivacyTarget,
    space: string,
  ): string | undefined => {
    const owned = target.owner.via.find((path) =>
      target.paths.find((p) => p.path === path)?.spaces.includes(space),
    );
    if (owned) return owned;
    return target.paths.find(
      (p) => p.role === "reference" && p.spaces.includes(space),
    )?.path;
  };

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

  const batchesFor = (
    entry: SeedShapeEntry | undefined,
    scope: string | null,
  ): Batch[] => {
    if (entry === undefined) return [{ scope, count: defaultCount }];
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

  const generateDoc = (
    target: PrivacyTarget,
    fields: SchemaContent,
    docTarget: DocTarget,
    parent: Record<string, unknown> | undefined,
    index: number,
    ordinal: number,
    count: number,
  ): Record<string, unknown> | undefined => {
    const base = session.docOptions(docTarget);
    const targetRules = rules.get(target.key);
    const resolve = (node: ResolveNode): unknown => {
      (
        node.faker as { setDefaultRefDate?: (d: Date) => void }
      ).setDefaultRefDate?.(refDate);
      const rule = targetRules?.[node.path];
      if (rule) {
        const value = rule({
          ...world,
          target: target.key,
          path: node.path,
          scope: docTarget.scope,
          parent,
          index,
          ordinal,
          count,
          faker: node.faker,
        });
        if (value !== SKIP) return value;
      }
      return base.resolve ? base.resolve(node) : SKIP;
    };
    try {
      const doc = generateMockDocument(fields, { ...base, resolve });
      if (parent && typeof parent._id === "string") {
        const parentSpace = String(parent._id).split(":")[0];
        const path = referencePath(target, parentSpace);
        if (
          path !== undefined &&
          !path.includes(".") &&
          typeof doc[path] === "string"
        ) {
          doc[path] = parent._id;
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
      const finalize = finalizers.get(target.key);
      if (!finalize) return doc;
      const finalized =
        finalize({
          ...world,
          target: target.key,
          scope: docTarget.scope,
          parent,
          index,
          ordinal,
          count,
          doc,
        }) ?? doc;
      const cleaned = sanitizeForMongoDB(finalized);
      const parsed = v.safeParse(v.object(fields as v.ObjectEntries), cleaned);
      if (!parsed.success) {
        const issue = parsed.issues[0];
        const where = issue.path?.map((p) => String(p.key)).join(".") ?? "";
        violations.push({
          kind: "generation",
          target: target.key,
          message: `finalize produced an invalid document at "${where}": ${issue.message}`,
        });
        return undefined;
      }
      return cleaned;
    } catch (error) {
      violations.push({
        kind: "generation",
        target: target.key,
        message: error instanceof Error ? error.message : String(error),
      });
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

  const generateBatches = (
    target: PrivacyTarget,
    fields: SchemaContent,
    minted: readonly Minted[],
  ): void => {
    const unique = uniqueIndexesOf(fields);
    const taken = new Set(
      partitionedDocs(state, target).flatMap(({ doc, partition }) =>
        uniqueKeysOf(unique, doc, partition),
      ),
    );
    for (const { batch, ids } of minted) {
      if (batch.count === 0) continue;
      const content = contentOf(target, batch.scope);
      const partition = partitionOf(target, batch.scope);
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
            batch.parent,
            i,
            ordinal,
            batch.count,
          );
        let doc = generate();
        for (
          let attempt = 1;
          doc &&
          attempt < UNIQUE_ATTEMPTS &&
          uniqueKeysOf(unique, doc, partition).some((key) => taken.has(key));
          attempt++
        ) {
          doc = generate();
        }
        if (!doc) continue;
        for (const key of uniqueKeysOf(unique, doc, partition)) taken.add(key);
        if (doc._id === undefined && ids?.[i] !== undefined) doc._id = ids[i];
        content.push({
          ...doc,
          ...(target.type !== undefined && { _type: target.type }),
          ...(batch.scope !== null &&
            target.bucket !== "multiModels" && { _scope: batch.scope }),
        });
      }
    }
  };

  const dependsOn = (target: PrivacyTarget): string | undefined => {
    const entry = shape.get(target.key);
    if (
      entry === undefined ||
      typeof entry !== "object" ||
      entry.per === "scope"
    ) {
      return undefined;
    }
    return resolveTargetKey(plan, entry.per);
  };
  const ordered = (() => {
    const base = orderedTargets(plan);
    const out: PrivacyTarget[] = [];
    const placed = new Set<string>();
    const place = (target: PrivacyTarget, trail: Set<string>) => {
      if (placed.has(target.key)) return;
      if (trail.has(target.key)) {
        throw new Error(
          `scenario: circular "per" dependency through ${target.key}`,
        );
      }
      const dep = dependsOn(target);
      if (dep !== undefined && dep !== target.key) {
        place(plan.targets.get(dep)!, new Set([...trail, target.key]));
      }
      placed.add(target.key);
      out.push(target);
    };
    for (const target of base) place(target, new Set());
    return out;
  })();

  const scopesOf = new Map<string, string[]>();
  const singletonOf = new Map<string, PrivacyTarget | undefined>();
  const realizeAllScopes = (): void => {
    const collections = new Set(
      ordered
        .filter((t) => t.bucket === "scopedMultiCollections")
        .map((t) => t.collection),
    );
    for (const collection of collections) {
      const scoped = schemas.scopedMultiCollections![collection];
      const scopeSpace = extractIdPrefix(scoped.scope);
      const types = ordered.filter(
        (t) =>
          t.bucket === "scopedMultiCollections" && t.collection === collection,
      );
      const singleton = types.find((t) => t.space === scopeSpace);
      singletonOf.set(collection, singleton);
      let scopes = [...session.pooledIds(scopeSpace)];
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
        batchesFor(shape.get(target.key), name),
      );
    }
    if (target.bucket !== "scopedMultiCollections") {
      return batchesFor(shape.get(target.key), null);
    }
    const scopes = scopesOf.get(target.collection) ?? [];
    const singleton = singletonOf.get(target.collection);
    return scopes.flatMap((scope) =>
      target === singleton
        ? [{ scope, count: 1 }]
        : batchesFor(shape.get(target.key), scope),
    );
  };

  const scopedFirst = (a: PrivacyTarget, b: PrivacyTarget): number => {
    const sa =
      a.bucket === "scopedMultiCollections" &&
      singletonOf.get(a.collection) === a
        ? 0
        : 1;
    const sb =
      b.bucket === "scopedMultiCollections" &&
      singletonOf.get(b.collection) === b
        ? 0
        : 1;
    return sa - sb;
  };
  const independent = ordered.filter((t) => dependsOn(t) === undefined);
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
    generateBatches(target, fields, batches);
  }

  scenario.after?.({ ...world, state });

  violations.push(
    ...session.drainFindings().map((f) => ({
      kind: "correlation" as const,
      target: `${f.bucket}/${f.collection}/${f.modelType ?? ""}`,
      message: f.message,
    })),
  );
  return { state, plan, violations };
}
