import * as v from "../schema.ts";
import { extractIndexes } from "../indexes.ts";
import type { DatabaseState, SchemasDefinition } from "../migration/types.ts";
import { extractIdPrefix } from "../migration/utils/seed-id.ts";
import type { PrivacyTarget } from "../privacy/plan.ts";
import { indexPath, sourceOfTarget } from "../privacy/unique-keys.ts";
import { fieldsOf, indexesOf } from "../type-definition.ts";
import { partitionedDocs } from "./unique.ts";

export interface DocScope {
  readonly dimension: string;
  readonly value: string;
}

export interface ScopedDoc {
  readonly doc: Record<string, unknown>;
  readonly scope: DocScope | undefined;
}

export function scopeDimension(
  schemas: SchemasDefinition,
  target: PrivacyTarget,
): string | undefined {
  if (target.bucket === "multiModels") return target.collection;
  if (target.bucket !== "scopedMultiCollections") return undefined;
  const scoped = schemas.scopedMultiCollections?.[target.collection];
  const space = scoped ? extractIdPrefix(scoped.scope) : undefined;
  return space || `scoped:${target.collection}`;
}

export function scopedDocs(
  state: DatabaseState,
  schemas: SchemasDefinition,
  target: PrivacyTarget,
): ScopedDoc[] {
  const dimension = scopeDimension(schemas, target);
  return partitionedDocs(state, target).map(({ doc, partition }) => {
    const value =
      target.bucket === "multiModels" ? partition.instance : partition.scope;
    return {
      doc,
      scope:
        dimension !== undefined && value !== ""
          ? { dimension, value }
          : undefined,
    };
  });
}

export function globalIndexPaths(
  schemas: SchemasDefinition,
  target: PrivacyTarget,
): ReadonlySet<string> {
  const source = sourceOfTarget(schemas, target);
  if (source === undefined) return new Set();
  const paths = new Set<string>();
  for (const { path, metadata } of extractIndexes(
    v.object(fieldsOf(source) as v.ObjectEntries),
  )) {
    if (metadata.global === true) paths.add(indexPath(path));
  }
  for (const index of indexesOf(source)) {
    if (index.global !== true) continue;
    for (const path of Object.keys(index.key)) paths.add(indexPath(path));
  }
  return paths;
}
