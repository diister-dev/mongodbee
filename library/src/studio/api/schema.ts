import * as v from "../../schema.ts";
import type { CompositeIndexDescriptor } from "../../indexes.ts";
import { fieldsOf, indexesOf } from "../../type-definition.ts";
import { toMongoValidator } from "../../validator.ts";
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

export function typeJsonSchema(source: unknown): unknown {
  try {
    const validator = toMongoValidator(
      v.object(fieldsOf(source as never) as never),
    ) as { $jsonSchema?: unknown };
    return validator.$jsonSchema ?? validator;
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
      fields: entriesToNodes(fieldsOf(source) as Record<string, unknown>),
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
