import * as v from "../schema.ts";
import type { DatabaseState, SchemasDefinition } from "../migration/types.ts";
import type { PrivacyPlan } from "../privacy/plan.ts";
import { KEEP, walkDocument } from "../privacy/walk.ts";
import type { ScenarioViolation, SeedInvariant } from "./types.ts";
import { docsOf, fieldsOfTarget, resolveTargetKey } from "./state.ts";

export interface CheckScenarioOptions {
  readonly state: DatabaseState;
  readonly schemas: SchemasDefinition;
  readonly plan: PrivacyPlan;
  readonly invariants?: readonly SeedInvariant[];
}

export function checkScenarioState(
  options: CheckScenarioOptions,
): ScenarioViolation[] {
  const { state, schemas, plan } = options;
  const violations: ScenarioViolation[] = [];
  const ids = new Map<string, Set<string>>();
  const owned = new Set<string>();

  for (const target of plan.targets.values()) {
    if (!target.space) continue;
    owned.add(target.space);
    const set = ids.get(target.space) ?? new Set<string>();
    for (const doc of docsOf(state, target)) {
      if (typeof doc._id === "string") set.add(doc._id);
    }
    ids.set(target.space, set);
  }

  for (const target of plan.targets.values()) {
    const fields = fieldsOfTarget(schemas, target);
    if (!fields) continue;
    const docs = docsOf(state, target);
    if (docs.length === 0) continue;
    const byPath = new Map(target.paths.map((p) => [p.path, p]));
    const seen = new Set<string>();
    let duplicates = 0;
    let invalid = 0;
    const dangling = new Map<string, number>();
    const ownerMissing = new Map<string, number>();

    for (const doc of docs) {
      if (doc._id !== undefined || target.space !== "") {
        const key = target.bucket === "scopedMultiCollections"
          ? `${doc._scope}|${doc._id}`
          : String(doc._id);
        if (seen.has(key)) duplicates++;
        seen.add(key);
      }
      if (!v.safeParse(v.object(fields as v.ObjectEntries), doc).success) {
        invalid++;
      }
      const { _id: _i, _scope: _s, _type: _t, ...rest } = doc;
      walkDocument(fields, rest, (leaf) => {
        const cls = byPath.get(leaf.path);
        if (cls?.role !== "reference" || typeof leaf.value !== "string") {
          return KEEP;
        }
        const space = leaf.value.split(":")[0];
        if (!cls.spaces.includes(space) || !owned.has(space)) return KEEP;
        if (ids.get(space)?.has(leaf.value)) return KEEP;
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
