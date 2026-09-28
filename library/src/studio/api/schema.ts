import * as v from "../../schema.ts";
import type { CompositeIndexDescriptor } from "../../indexes.ts";
import { computedOf, fieldsOf, indexesOf } from "../../type-definition.ts";
import type { ComputedDescriptor } from "../../computed.ts";
import { COMPUTED_REVISION, COMPUTED_ROOT } from "../../computed-guard.ts";
import { toMongoValidator } from "../../validator.ts";
import type { TypeSource } from "../../migration/types.ts";
import type { CollectionKind } from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import {
  entriesToNodes,
  type SchemaNode,
  schemaToNode,
} from "../schema-tree.ts";
import { requireEntry } from "./documents.ts";
import { type ValidatorComparison, validatorFor } from "./drift.ts";

export interface TypeSchema {
  name: string;
  fields: Record<string, SchemaNode>;
  indexes: readonly CompositeIndexDescriptor[];
  jsonSchema?: unknown;
}

export interface CollectionSchema {
  collection: string;
  kind: CollectionKind;
  model?: string;
  implicitFields: string[];
  scope?: SchemaNode;
  types: TypeSchema[];
  validator?: ValidatorComparison;
}

const IMPLICIT_FIELDS: Record<CollectionKind, string[]> = {
  collection: ["_id"],
  multiCollection: ["_id", "_type"],
  multiModelInstance: ["_id", "_type"],
  scopedMultiCollection: ["_id", "_type", "_scope"],
  undeclared: [],
  internal: [],
};

function sourceLabel(source: ComputedDescriptor["source"]): string {
  return source.model ? `${source.model}.${source.type}` : source.type;
}

export function describeComputed(descriptor: ComputedDescriptor): string {
  const aggregate =
    descriptor.aggregate.kind === "count"
      ? "count of"
      : `${descriptor.aggregate.distinct ? "distinct " : ""}${descriptor.aggregate.path} of`;
  const parts = [
    `${aggregate} ${sourceLabel(descriptor.source)} by ${descriptor.by}`,
  ];
  const where = Object.keys(descriptor.where);
  if (where.length > 0) parts.push(`where ${where.join(", ")}`);
  if (descriptor.through) {
    parts.push(
      `through ${sourceLabel(descriptor.through.source)}.${descriptor.through.via}`,
    );
  }
  if (descriptor.sameScope) parts.push("in the same scope");
  return parts.join(", ");
}

export function typeFields(source: TypeSource): Record<string, SchemaNode> {
  const fields = entriesToNodes(fieldsOf(source));
  const root = fields[COMPUTED_ROOT];
  if (!root) return fields;
  root.system = "computed";
  const descriptors = computedOf(source);
  for (const [name, child] of Object.entries(root.entries ?? {})) {
    if (name === COMPUTED_REVISION) {
      child.system = "revision";
      child.description =
        "Bumped by every transaction that recomputes this document";
      continue;
    }
    child.system = "computed";
    const descriptor = descriptors[name];
    if (descriptor) child.computed = describeComputed(descriptor);
  }
  return fields;
}

export function typeJsonSchema(source: TypeSource): unknown {
  try {
    return toMongoValidator(v.object(fieldsOf(source))).$jsonSchema;
  } catch {
    return undefined;
  }
}

export async function getCollectionSchema(
  context: StudioContext,
  collectionName: string,
): Promise<CollectionSchema> {
  const entry = await requireEntry(context, collectionName);
  const result: CollectionSchema = {
    collection: entry.name,
    kind: entry.kind,
    implicitFields: IMPLICIT_FIELDS[entry.kind],
    types: Object.entries(entry.types).map(([name, source]) => ({
      name,
      fields: typeFields(source),
      indexes: indexesOf(source),
      jsonSchema: typeJsonSchema(source),
    })),
  };
  if (entry.model) result.model = entry.model;
  if (entry.scope) result.scope = schemaToNode(entry.scope);
  if (entry.kind !== "undeclared" && entry.kind !== "internal") {
    result.validator = await validatorFor(context, entry);
  }
  return result;
}
