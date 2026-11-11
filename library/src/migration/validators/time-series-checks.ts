/**
 * What a migration must do about its time series collections, checked from
 * its snapshot and its operations alone.
 *
 * A time series collection is the one kind whose options MongoDB fixes at
 * creation, and whose documents a migration cannot rewrite. So the snapshot
 * of a migration is compared with its parent's: a new collection must be
 * created, an option that changed must be applied by an operation or
 * reported as not applicable, and a measurement schema that changed is
 * called out, since nothing will bring the stored measurements to it.
 *
 * @module
 */

import type { MigrationDefinition, MigrationRule } from "../types.ts";
import { createSchemaComparer } from "../schema-validation.ts";
import {
  isTimeSeriesDefinition,
  planTimeSeriesChange,
} from "../../time-series.ts";
import { dirtyEquivalent } from "../../utils/object.ts";

const OTHER_BUCKETS = [
  "collections",
  "multiCollections",
  "scopedMultiCollections",
] as const;

export function timeSeriesChecks(
  definition: MigrationDefinition,
  operations: readonly MigrationRule[],
): { errors: string[]; warnings: string[] } {
  const errors: string[] = [];
  const warnings: string[] = [];
  const declared = definition.schemas.timeSeriesCollections ?? {};
  const parent = definition.parent?.schemas.timeSeriesCollections ?? {};

  const created = new Set<string>();
  const updated = new Set<string>();
  for (const operation of operations) {
    if (operation.type === "create_timeseries_collection") {
      created.add(operation.collectionName);
    } else if (operation.type === "update_timeseries_options") {
      updated.add(operation.collectionName);
    }
  }

  let missingCreate = false;
  for (const [name, current] of Object.entries(declared)) {
    const label = `Time series collection "${name}"`;
    if (!isTimeSeriesDefinition(current)) {
      errors.push(
        `${label} must be declared with defineTimeSeries({...}) in schemas.timeSeriesCollections`,
      );
      continue;
    }
    for (const bucket of OTHER_BUCKETS) {
      if (definition.schemas[bucket]?.[name] !== undefined) {
        errors.push(
          `${label} is also declared in schemas.${bucket}: one name is one physical collection, and MongoDB cannot convert a collection into a time series`,
        );
      }
    }

    const before = parent[name];
    if (before === undefined) {
      if (!created.has(name)) {
        errors.push(
          `${label} is declared in schema but not created in migrate()`,
        );
        missingCreate = true;
      }
      continue;
    }
    if (created.has(name)) {
      errors.push(
        `${label} is created in migrate() but already exists in the parent migration`,
      );
    }
    if (!isTimeSeriesDefinition(before)) continue;

    const plan = planTimeSeriesChange(before.options, current.options);
    if (plan.blocked.length > 0) {
      errors.push(
        `${label}: option change is not applicable. ${plan.blocked.join("; ")}. ` +
          `MongoDB fixes these when the collection is created: restore them, or declare a new time series collection and copy the measurements`,
      );
    } else if (plan.changes.length > 0 && !updated.has(name)) {
      errors.push(
        `${label}: ${plan.changes.join("; ")}, but migrate() never applies it. ` +
          `Add migration.timeSeriesCollection("${name}").updateOptions().end()`,
      );
    }

    const comparer = createSchemaComparer();
    if (!comparer.equal(before.schema.entries, current.schema.entries)) {
      warnings.push(
        `${label}: the measurement schema changed, and a migration cannot rewrite stored measurements (MongoDB has no replaceOne on a time series). ` +
          `Those already stored are read with the new schema as they are: keep it compatible (new fields optional, nothing narrowed), or reads fail with DocumentValidationError`,
      );
    }
    if (
      !dirtyEquivalent(before.indexes, current.indexes) &&
      plan.inSync &&
      comparer.equal(before.schema.entries, current.schema.entries)
    ) {
      // Index-only change: applied by the synchronisation that ends every
      // migration, nothing to declare. Stated so it is not a surprise.
      warnings.push(
        `${label}: its indexes changed and are rebuilt when this migration is applied`,
      );
    }
  }
  if (missingCreate) {
    errors.push(
      " 💡 Tip: Did you forget to call .createTimeSeriesCollection() in your migration?",
    );
  }

  for (const name of Object.keys(parent)) {
    if (declared[name] === undefined) {
      warnings.push(
        `Time series collection "${name}" left the schema; migrations do not drop it, the collection and its measurements stay in the database`,
      );
    }
  }
  for (const name of updated) {
    if (declared[name] === undefined) {
      errors.push(
        `Time series collection "${name}" has its options updated in migrate() but is not declared in schemas.timeSeriesCollections`,
      );
    }
  }

  return { errors, warnings };
}
