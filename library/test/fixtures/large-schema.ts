/**
 * A synthetic schema shaped like a large real-world one: many collection
 * types that embed the same deep sub-schemas (a recursive filter tree of
 * variants, unions and arrays), so the expanded tree is much larger than the
 * set of distinct schema objects.
 *
 * Each call builds fresh objects, the way a migration snapshot and the
 * project schema are two separate object graphs describing the same thing.
 *
 * @module
 */

import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import { defineType } from "../../src/type-definition.ts";
import type { SchemasDefinition } from "../../src/migration/types.ts";

export interface LargeSchemaShape {
  /** Types in the scoped multi-collection. */
  readonly types: number;
  /** Levels of the shared filter tree. */
  readonly depth: number;
}

function filterTree(depth: number) {
  let level: v.GenericSchema = v.object({
    kind: v.literal("leaf"),
    field: v.pipe(v.string(), v.maxLength(80)),
    value: v.union([v.string(), v.number(), v.boolean(), v.null()]),
  });
  for (let i = 0; i < depth; i++) {
    level = v.variant("kind", [
      v.object({
        kind: v.literal("leaf"),
        field: v.pipe(v.string(), v.maxLength(80)),
        value: v.union([v.string(), v.number(), v.boolean(), v.null()]),
      }),
      v.object({
        kind: v.literal("all"),
        filters: v.array(level),
        label: v.optional(v.pipe(v.string(), v.maxLength(40))),
      }),
      v.object({
        kind: v.literal("weighted"),
        weight: v.nullable(v.pipe(v.number(), v.integer(), v.minValue(0))),
      }),
    ]);
  }
  return level;
}

export function largeSchemas(shape: LargeSchemaShape): SchemasDefinition {
  const filter = filterTree(shape.depth);
  const audit = v.object({
    at: v.date(),
    by: refId("user"),
    note: v.optional(v.string()),
  });
  const types: Record<string, unknown> = {};
  for (let i = 0; i < shape.types; i++) {
    types[`type_${i}`] = defineType({
      schema: v.object({
        _id: refId(`type_${i}`),
        name: withIndex(v.pipe(v.string(), v.maxLength(120)), {
          unique: i % 2 === 0,
        }),
        filter,
        saved: v.array(v.object({ title: v.string(), filter })),
        fallback: v.optional(filter),
        created: audit,
        updated: v.nullable(audit),
      }),
    });
  }
  return {
    collections: {
      users: { _id: refId("user"), email: v.string(), filter },
    },
    scopedMultiCollections: {
      "+things": { scope: refId("space"), types: types as any },
    },
  };
}
