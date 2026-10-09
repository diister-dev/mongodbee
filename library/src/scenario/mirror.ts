import type { DatabaseState } from "../migration/types.ts";
import type {
  PrivacyPath,
  PrivacyPlan,
  PrivacyTarget,
} from "../privacy/plan.ts";
import { setValueAt, valueAt } from "./doc-path.ts";
import { docsOf } from "./state.ts";

export type DocLookup = (
  space: string,
  id: string,
) => Record<string, unknown> | undefined;

interface IndexedBucket {
  seen: number;
  readonly byId: Map<string, Record<string, unknown>>;
}

export function createDocLookup(
  state: DatabaseState,
  plan: PrivacyPlan,
): DocLookup {
  const buckets = new Map<string, IndexedBucket>();
  return (space, id) => {
    for (const target of plan.targets.values()) {
      if (target.space !== space) continue;
      const docs = docsOf(state, target);
      const bucket = buckets.get(target.key) ?? { seen: 0, byId: new Map() };
      buckets.set(target.key, bucket);
      for (; bucket.seen < docs.length; bucket.seen++) {
        const doc = docs[bucket.seen];
        if (typeof doc._id === "string") bucket.byId.set(doc._id, doc);
      }
      const found = bucket.byId.get(id);
      if (found) return found;
    }
    return undefined;
  };
}

export function normalizeMirrored(
  value: unknown,
  normalize: PrivacyPath["normalize"],
): unknown {
  if (typeof value !== "string" || normalize === undefined) return value;
  return normalize === "lowercase" ? value.toLowerCase() : value.trim();
}

export interface MirrorExpectation {
  readonly path: string;
  readonly candidates: readonly unknown[];
}

export function sourceInSameDocument(
  target: PrivacyTarget,
  space: string,
  sourcePath: string,
): boolean {
  return (
    target.space === space && target.paths.some((p) => p.path === sourcePath)
  );
}

export function referencePathsTo(
  target: PrivacyTarget,
  space: string,
): string[] {
  const references = target.paths.filter(
    (p) =>
      p.role === "reference" &&
      p.spaces.includes(space) &&
      !p.path.includes("*"),
  );
  const owned = references.filter((p) => target.owner.via.includes(p.path));
  return [...owned, ...references.filter((p) => !owned.includes(p))].map(
    (p) => p.path,
  );
}

export function mirrorExpectations(
  target: PrivacyTarget,
  doc: Record<string, unknown>,
  lookup: DocLookup,
): MirrorExpectation[] {
  const expectations: MirrorExpectation[] = [];
  for (const cls of target.paths) {
    if (cls.mirrorOf === undefined || cls.path.includes("*")) continue;
    const [space, ...rest] = cls.mirrorOf.split(".");
    const sourcePath = rest.join(".");
    if (sourceInSameDocument(target, space, sourcePath)) {
      const own = valueAt(doc, sourcePath);
      if (own !== undefined) {
        expectations.push({
          path: cls.path,
          candidates: [normalizeMirrored(own, cls.normalize)],
        });
      }
      continue;
    }
    const candidates: unknown[] = [];
    for (const refPath of referencePathsTo(target, space)) {
      const id = valueAt(doc, refPath);
      if (typeof id !== "string") continue;
      const source = lookup(space, id);
      const value = source && valueAt(source, sourcePath);
      if (value !== undefined) {
        candidates.push(normalizeMirrored(value, cls.normalize));
      }
    }
    if (candidates.length > 0) {
      expectations.push({ path: cls.path, candidates });
    }
  }
  return expectations;
}

export interface TransformedDocument {
  readonly target: PrivacyTarget;
  readonly input: Record<string, unknown>;
  readonly output: Record<string, unknown>;
}

export function hasCrossDocumentMirror(target: PrivacyTarget): boolean {
  return target.paths.some((cls) => {
    if (cls.mirrorOf === undefined || cls.path.includes("*")) return false;
    const [space, ...rest] = cls.mirrorOf.split(".");
    return !sourceInSameDocument(target, space, rest.join("."));
  });
}

export function copyMirrorsFromSources(
  state: DatabaseState,
  plan: PrivacyPlan,
  documents: readonly TransformedDocument[],
): Map<string, number> {
  const lookup = createDocLookup(state, plan);
  const unresolved = new Map<string, number>();
  for (const { target, input, output } of documents) {
    for (const cls of target.paths) {
      if (cls.mirrorOf === undefined || cls.path.includes("*")) continue;
      const [space, ...rest] = cls.mirrorOf.split(".");
      const sourcePath = rest.join(".");
      if (sourceInSameDocument(target, space, sourcePath)) continue;
      if (valueAt(input, cls.path) === undefined) continue;
      let source: Record<string, unknown> | undefined;
      for (const refPath of referencePathsTo(target, space)) {
        const id = valueAt(output, refPath);
        source = typeof id === "string" ? lookup(space, id) : undefined;
        if (source) break;
      }
      if (!source) {
        unresolved.set(target.key, (unresolved.get(target.key) ?? 0) + 1);
        continue;
      }
      const copied = valueAt(source, sourcePath);
      if (copied !== undefined) {
        setValueAt(output, cls.path, normalizeMirrored(copied, cls.normalize));
      }
    }
  }
  return unresolved;
}
