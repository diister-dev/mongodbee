import { extractIndexes } from "../indexes.ts";
import * as v from "../schema.ts";
import type { DatabaseState, SchemaContent } from "../migration/types.ts";
import type { PrivacyTarget } from "../privacy/plan.ts";
import { valueAt } from "./doc-path.ts";
import { docsOf } from "./state.ts";

export interface UniqueIndexSpec {
  readonly path: string;
  readonly global: boolean;
  readonly caseInsensitive: boolean;
}

const CASE_INSENSITIVE_STRENGTH = 2;

export function uniqueIndexesOf(fields: SchemaContent): UniqueIndexSpec[] {
  return extractIndexes(v.object(fields as v.ObjectEntries))
    .filter(
      ({ metadata }) =>
        metadata.unique === true &&
        metadata.partialFilterExpression === undefined,
    )
    .map(({ path, metadata }) => ({
      path,
      global: metadata.global === true,
      caseInsensitive:
        (metadata.collation?.strength ?? 3) <= CASE_INSENSITIVE_STRENGTH,
    }));
}

function canonicalValue(value: unknown, caseInsensitive: boolean): string {
  if (value === undefined || value === null) return "null";
  const text =
    value instanceof Date
      ? value.toISOString()
      : typeof value === "string" && caseInsensitive
        ? value.toLowerCase()
        : value;
  return JSON.stringify(text);
}

export interface UniquePartition {
  readonly instance: string;
  readonly scope: string;
}

export function uniqueKeysOf(
  indexes: readonly UniqueIndexSpec[],
  doc: Record<string, unknown>,
  partition: UniquePartition,
): string[] {
  return indexes.map((index) => {
    const scope = index.global ? "" : partition.scope;
    return `${index.path}|${partition.instance}|${scope}|${canonicalValue(
      valueAt(doc, index.path),
      index.caseInsensitive,
    )}`;
  });
}

export interface PartitionedDoc {
  readonly doc: Record<string, unknown>;
  readonly partition: UniquePartition;
}

export function partitionedDocs(
  state: DatabaseState,
  target: PrivacyTarget,
): PartitionedDoc[] {
  if (target.bucket === "multiModels") {
    return Object.entries(state.multiModels)
      .filter(([, instance]) => instance.modelType === target.collection)
      .flatMap(([name, instance]) =>
        instance.content
          .filter((doc) => doc._type === target.type)
          .map((doc) => ({ doc, partition: { instance: name, scope: "" } })),
      );
  }
  return docsOf(state, target).map((doc) => ({
    doc,
    partition: { instance: "", scope: String(doc._scope ?? "") },
  }));
}
