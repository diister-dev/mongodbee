import type { Db } from "../../../src/mongodb.ts";
import type { SchemasDefinition } from "../../../src/migration/types.ts";
import { looksLikeId, type PrivacyPlan } from "../../../src/privacy/mod.ts";
import { docsOf, readStateFromDatabase } from "../../../src/scenario/mod.ts";

export interface DanglingReference {
  readonly target: string;
  readonly docId: string;
  readonly path: string;
  readonly value: string;
}

function valuesAt(value: unknown, segments: readonly string[]): unknown[] {
  if (segments.length === 0) return [value];
  if (value === null || typeof value !== "object") return [];
  const [head, ...rest] = segments;
  if (head === "*") {
    const children = Array.isArray(value) ? value : Object.values(value);
    return children.flatMap((child) => valuesAt(child, rest));
  }
  return valuesAt((value as Record<string, unknown>)[head], rest);
}

export async function findDanglingReferences(
  db: Db,
  schemas: SchemasDefinition,
  plan: PrivacyPlan,
): Promise<DanglingReference[]> {
  const state = await readStateFromDatabase(db, schemas);
  const knownIds = new Set<string>();
  const collect = (docs: readonly Record<string, unknown>[]) => {
    for (const doc of docs) {
      if (typeof doc._id === "string") knownIds.add(doc._id);
    }
  };
  for (const bucket of [
    state.collections,
    state.multiCollections,
    state.scopedMultiCollections,
    state.multiModels,
  ]) {
    for (const [name, { content }] of Object.entries(bucket)) {
      collect(content);
      if (bucket === state.multiModels) knownIds.add(name);
    }
  }

  const dangling: DanglingReference[] = [];
  for (const target of plan.targets.values()) {
    const refPaths = target.paths
      .filter((p) => p.treatment.extract === "remap")
      .map((p) => p.path);
    for (const doc of docsOf(state, target)) {
      const docId = String(doc._id);
      const check = (path: string, candidate: unknown) => {
        if (!looksLikeId(candidate) || knownIds.has(candidate)) return;
        dangling.push({ target: target.key, docId, path, value: candidate });
      };
      if (doc._scope !== undefined) check("_scope", doc._scope);
      for (const path of refPaths) {
        for (const candidate of valuesAt(doc, path.split("."))) {
          check(path, candidate);
        }
      }
    }
  }
  return dangling;
}
