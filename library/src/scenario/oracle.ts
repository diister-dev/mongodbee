import * as v from "../schema.ts";
import type { DatabaseState, SchemasDefinition } from "../migration/types.ts";
import type { PrivacyPlan } from "../privacy/plan.ts";
import { KEEP, walkDocument } from "../privacy/walk.ts";
import type { ScenarioViolation, SeedInvariant } from "./types.ts";
import { docsOf, fieldsOfTarget, resolveTargetKey } from "./state.ts";
import { createDocLookup, mirrorExpectations } from "./mirror.ts";
import { valueAt } from "./doc-path.ts";
import { partitionedDocs } from "./unique.ts";
import {
  indexPath,
  uniqueEntriesOf,
  uniqueKeysOfTarget,
} from "../privacy/unique-keys.ts";
import { type DocScope, globalIndexPaths, scopedDocs } from "./scope.ts";

export interface CheckScenarioOptions {
  readonly state: DatabaseState;
  readonly schemas: SchemasDefinition;
  readonly plan: PrivacyPlan;
  readonly invariants?: readonly SeedInvariant[];
}

function outsideScope(
  scope: DocScope,
  targets: readonly (DocScope | undefined)[],
): boolean {
  const sameDimension = targets.filter(
    (t) => t === undefined || t.dimension === scope.dimension,
  );
  return (
    sameDimension.length > 0 &&
    sameDimension.every((t) => t !== undefined && t.value !== scope.value)
  );
}

export function checkScenarioState(
  options: CheckScenarioOptions,
): ScenarioViolation[] {
  const { state, schemas, plan } = options;
  const violations: ScenarioViolation[] = [];
  const ids = new Map<string, Map<string, (DocScope | undefined)[]>>();
  const owned = new Set<string>();
  const seenByCollection = new Map<string, Set<string>>();
  const lookup = createDocLookup(state, plan);

  for (const target of plan.targets.values()) {
    if (!target.space) continue;
    owned.add(target.space);
    const located = ids.get(target.space) ?? new Map();
    const spansScopes = globalIndexPaths(schemas, target).has("_id");
    for (const { doc, scope } of scopedDocs(state, schemas, target)) {
      if (typeof doc._id !== "string") continue;
      const scopes = located.get(doc._id) ?? [];
      scopes.push(spansScopes ? undefined : scope);
      located.set(doc._id, scopes);
    }
    ids.set(target.space, located);
  }

  for (const target of plan.targets.values()) {
    const fields = fieldsOfTarget(schemas, target);
    if (!fields) continue;
    const located = scopedDocs(state, schemas, target);
    const docs = located.map(({ doc }) => doc);
    if (docs.length === 0) continue;
    const crossing = globalIndexPaths(schemas, target);
    const byPath = new Map(target.paths.map((p) => [p.path, p]));
    const physical = `${target.bucket}/${target.collection}`;
    const seen = seenByCollection.get(physical) ?? new Set<string>();
    seenByCollection.set(physical, seen);
    let duplicates = 0;
    let invalid = 0;
    let mirrored = 0;
    const dangling = new Map<string, number>();
    const ownerMissing = new Map<string, number>();
    const crossScope = new Map<string, number>();

    for (const { doc, scope } of located) {
      if (doc._id !== undefined || target.space !== "") {
        const key =
          target.bucket === "scopedMultiCollections"
            ? `${doc._scope}|${doc._id}`
            : `${typeof doc._id}:${String(doc._id)}`;
        if (seen.has(key)) duplicates++;
        seen.add(key);
      }
      if (!v.safeParse(v.object(fields as v.ObjectEntries), doc).success) {
        invalid++;
      }
      for (const { path, candidates } of mirrorExpectations(
        target,
        doc,
        lookup,
      )) {
        const actual = valueAt(doc, path);
        if (
          actual !== undefined &&
          !candidates.some((c) => JSON.stringify(c) === JSON.stringify(actual))
        ) {
          mirrored++;
        }
      }
      const { _id: _i, _scope: _s, _type: _t, ...rest } = doc;
      walkDocument(fields, rest, (leaf) => {
        const cls = byPath.get(leaf.path);
        if (cls?.role !== "reference" || typeof leaf.value !== "string") {
          return KEEP;
        }
        const space = leaf.value.split(":")[0];
        if (!cls.spaces.includes(space) || !owned.has(space)) return KEEP;
        const targetScopes = ids.get(space)?.get(leaf.value);
        if (targetScopes) {
          if (
            scope !== undefined &&
            !crossing.has(indexPath(leaf.path)) &&
            outsideScope(scope, targetScopes)
          ) {
            crossScope.set(leaf.path, (crossScope.get(leaf.path) ?? 0) + 1);
          }
          return KEEP;
        }
        const bucket = target.owner.via.includes(leaf.path)
          ? ownerMissing
          : dangling;
        bucket.set(leaf.path, (bucket.get(leaf.path) ?? 0) + 1);
        return KEEP;
      });
    }
    if (duplicates > 0) {
      violations.push({
        kind: "duplicate_id",
        target: target.key,
        message: `${duplicates} duplicate _id`,
        count: duplicates,
      });
    }
    const unique = uniqueKeysOfTarget(schemas, target);
    if (unique.length > 0) {
      const seen = new Set<string>();
      let collisions = 0;
      let unchecked = 0;
      for (const { doc, partition } of partitionedDocs(state, target)) {
        for (const key of unique) {
          const result = uniqueEntriesOf(key, doc, partition);
          if (result.covered === undefined) unchecked++;
          if (result.covered !== true) continue;
          for (const entry of result.entries) {
            if (seen.has(entry)) collisions++;
            seen.add(entry);
          }
        }
      }
      if (collisions > 0) {
        violations.push({
          kind: "unique_index",
          target: target.key,
          message: `${collisions} value(s) break a unique index`,
          count: collisions,
        });
      }
      if (unchecked > 0) {
        violations.push({
          kind: "unique_unchecked",
          target: target.key,
          message: `${unchecked} document(s) under a unique index whose partial filter cannot be evaluated; uniqueness not checked`,
          count: unchecked,
        });
      }
    }
    if (mirrored > 0) {
      violations.push({
        kind: "mirror_mismatch",
        target: target.key,
        message: `${mirrored} mirrored value(s) differ from their source`,
        count: mirrored,
      });
    }
    if (invalid > 0) {
      violations.push({
        kind: "invalid_document",
        target: target.key,
        message: `${invalid} of ${docs.length} documents fail their schema`,
        count: invalid,
      });
    }
    for (const [path, count] of dangling) {
      violations.push({
        kind: "dangling_reference",
        target: target.key,
        message: `${path}: ${count} reference(s) point at no document`,
        count,
      });
    }
    for (const [path, count] of crossScope) {
      violations.push({
        kind: "cross_scope_reference",
        target: target.key,
        message: `${path}: ${count} reference(s) point at a document of another scope`,
        count,
      });
    }
    for (const [path, count] of ownerMissing) {
      violations.push({
        kind: "owner_unresolved",
        target: target.key,
        message: `${path}: ${count} owner reference(s) point at no document`,
        count,
      });
    }
  }

  for (const invariant of options.invariants ?? []) {
    const messages = invariant({
      state,
      schemas,
      docs: (key) =>
        docsOf(state, plan.targets.get(resolveTargetKey(plan, key))!),
    });
    for (const message of messages) {
      violations.push({ kind: "invariant", target: "*", message });
    }
  }
  return violations;
}
