import type * as m from "mongodb";
import * as v from "../schema.ts";
import { dbId } from "../ids.ts";
import {
  applyCollectionIndexes,
  applyMultiCollectionIndexes,
  applyScopedMultiCollectionIndexes,
  type IndexTarget,
} from "../indexes-applier.ts";
import { fieldsOf, indexesOf, normalizeTypes } from "../type-definition.ts";
import type { TypeSource } from "../migration/types.ts";

export type IndexPlanKind =
  | "collection"
  | "multiCollection"
  | "scopedMultiCollection";

export interface IndexPlanTarget {
  kind: IndexPlanKind;
  name: string;
  types: Readonly<Record<string, TypeSource>>;
  scope?: v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;
}

function recorder(name: string): {
  target: IndexTarget;
  planned: m.IndexDescription[];
} {
  const planned: m.IndexDescription[] = [];
  const target: IndexTarget = {
    collectionName: name,
    indexes: async () => [],
    dropIndex: async () => ({}),
    createIndexes: async (specs) => {
      planned.push(...specs);
      return specs.map((spec) => spec.name ?? "");
    },
  };
  return { target, planned };
}

export async function plannedIndexes(
  plan: IndexPlanTarget,
): Promise<m.IndexDescription[]> {
  const { target, planned } = recorder(plan.name);
  const types = Object.entries(plan.types);
  switch (plan.kind) {
    case "collection": {
      const source = plan.types[plan.name] ?? types[0]?.[1];
      if (!source) return [];
      await applyCollectionIndexes(target, v.object(fieldsOf(source)), {
        composites: indexesOf(source),
      });
      break;
    }
    case "multiCollection": {
      const schemas = Object.fromEntries(
        types.map(([name, source]) => [name, v.object(fieldsOf(source))]),
      );
      await applyMultiCollectionIndexes(target, schemas, {
        composites: normalizeTypes(plan.types).indexes,
      });
      break;
    }
    case "scopedMultiCollection": {
      const scope = plan.scope;
      if (!scope) {
        throw new TypeError(
          `a scoped multi-collection needs its scope schema to plan indexes for "${plan.name}"`,
        );
      }
      const schemas = Object.fromEntries(
        types.map(([name, source]) => [
          name,
          v.object({
            _id: dbId(name),
            _type: v.literal(name),
            _scope: scope,
            ...fieldsOf(source),
          }),
        ]),
      );
      await applyScopedMultiCollectionIndexes(target, schemas, {
        composites: normalizeTypes(plan.types).indexes,
      });
      break;
    }
  }
  return planned;
}
