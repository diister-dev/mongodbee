import * as v from "../../schema.ts";
import { dbId } from "../../ids.ts";
import { fieldsOf } from "../../type-definition.ts";
import { toMongoValidator } from "../../validator.ts";
import { getAppliedMigrationIds } from "../../migration/history.ts";
import { createMetadataSchemas } from "../../migration/multicollection-registry.ts";
import {
  diffSchemas,
  flattenSchema,
  simplifySchema,
} from "../../migration/schema-validation.ts";
import type {
  MigrationDefinition,
  SchemasDefinition,
  TypeSource,
} from "../../migration/types.ts";
import { buildCatalog, type CatalogEntry } from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import { entryUnder, getIndexReport, type IndexStatus } from "./indexes.ts";

export type Change = "added" | "removed" | "changed";

export interface FieldChange {
  field: string;
  change: Change;
  details: string[];
}

export interface SchemaDriftRow {
  bucket:
    | "collections"
    | "multiCollections"
    | "multiModels"
    | "scopedMultiCollections";
  collection: string;
  type?: string;
  change: Change;
  fields: FieldChange[];
}

export interface ValidatorDriftRow {
  collection: string;
  status: "matching" | "different" | "missing" | "unexpected";
}

export interface IndexDriftRow {
  collection: string;
  summary: Record<IndexStatus, number>;
  problems: { name: string; status: IndexStatus; hint?: string }[];
}

export interface DriftReport {
  schema: {
    available: boolean;
    baseline?: { id: string; name: string };
    rows: SchemaDriftRow[];
    command?: string;
  };
  validators: {
    baseline?: { id: string; name: string };
    rows: ValidatorDriftRow[];
  };
  indexes: IndexDriftRow[];
}

function fieldChanges(
  before: TypeSource | undefined,
  after: TypeSource | undefined,
): FieldChange[] {
  const flatBefore = before
    ? flattenSchema(simplifySchema(fieldsOf(before)))
    : {};
  const flatAfter = after ? flattenSchema(simplifySchema(fieldsOf(after))) : {};
  const diffs = diffSchemas(flatBefore, flatAfter);
  const byField = new Map<string, FieldChange>();
  const rootsBefore = new Set(
    Object.keys(flatBefore).map((key) => key.split(".")[0]),
  );
  const rootsAfter = new Set(
    Object.keys(flatAfter).map((key) => key.split(".")[0]),
  );
  for (const diff of diffs) {
    const field = diff.key.split(".")[0];
    const change: Change = !rootsBefore.has(field)
      ? "added"
      : !rootsAfter.has(field)
        ? "removed"
        : "changed";
    const entry = byField.get(field) ?? { field, change, details: [] };
    if (entry.details.length < 6)
      entry.details.push(diff.key.slice(field.length + 1) || diff.key);
    byField.set(field, entry);
  }
  return [...byField.values()].sort((a, b) => a.field.localeCompare(b.field));
}

function typedDrift(
  bucket: SchemaDriftRow["bucket"],
  before: Record<string, Record<string, TypeSource>>,
  after: Record<string, Record<string, TypeSource>>,
  rows: SchemaDriftRow[],
): void {
  const names = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const collection of [...names].sort()) {
    const was = before[collection];
    const now = after[collection];
    if (!was || !now) {
      rows.push({
        bucket,
        collection,
        change: was ? "removed" : "added",
        fields: [],
      });
      continue;
    }
    const types = new Set([...Object.keys(was), ...Object.keys(now)]);
    for (const type of [...types].sort()) {
      if (!was[type] || !now[type]) {
        rows.push({
          bucket,
          collection,
          type,
          change: was[type] ? "removed" : "added",
          fields: fieldChanges(was[type], now[type]),
        });
        continue;
      }
      const fields = fieldChanges(was[type], now[type]);
      if (fields.length > 0)
        rows.push({ bucket, collection, type, change: "changed", fields });
    }
  }
}

export function schemaDrift(
  before: SchemasDefinition,
  after: SchemasDefinition,
): SchemaDriftRow[] {
  const rows: SchemaDriftRow[] = [];
  const collectionsBefore = before.collections ?? {};
  const collectionsAfter = after.collections ?? {};
  const names = new Set([
    ...Object.keys(collectionsBefore),
    ...Object.keys(collectionsAfter),
  ]);
  for (const collection of [...names].sort()) {
    const was = collectionsBefore[collection];
    const now = collectionsAfter[collection];
    if (!was || !now) {
      rows.push({
        bucket: "collections",
        collection,
        change: was ? "removed" : "added",
        fields: [],
      });
      continue;
    }
    const fields = fieldChanges(was, now);
    if (fields.length > 0)
      rows.push({
        bucket: "collections",
        collection,
        change: "changed",
        fields,
      });
  }
  typedDrift(
    "multiCollections",
    before.multiCollections ?? {},
    after.multiCollections ?? {},
    rows,
  );
  typedDrift(
    "multiModels",
    before.multiModels ?? {},
    after.multiModels ?? {},
    rows,
  );
  const scopedBefore = before.scopedMultiCollections ?? {};
  const scopedAfter = after.scopedMultiCollections ?? {};
  typedDrift(
    "scopedMultiCollections",
    Object.fromEntries(
      Object.entries(scopedBefore).map(([name, s]) => [name, s.types]),
    ),
    Object.fromEntries(
      Object.entries(scopedAfter).map(([name, s]) => [name, s.types]),
    ),
    rows,
  );
  for (const name of Object.keys(scopedAfter)) {
    const was = scopedBefore[name];
    if (!was) continue;
    const diffs = diffSchemas(
      flattenSchema(simplifySchema({ _scope: was.scope })),
      flattenSchema(simplifySchema({ _scope: scopedAfter[name].scope })),
    );
    if (diffs.length > 0) {
      rows.push({
        bucket: "scopedMultiCollections",
        collection: name,
        change: "changed",
        fields: [
          {
            field: "_scope",
            change: "changed",
            details: diffs.slice(0, 6).map((d) => d.key),
          },
        ],
      });
    }
  }
  return rows;
}

export function expectedValidator(entry: CatalogEntry): unknown {
  const types = Object.entries(entry.types);
  switch (entry.kind) {
    case "collection":
      return toMongoValidator(v.object(fieldsOf(types[0][1])));
    case "multiCollection":
    case "multiModelInstance": {
      const schemas = [
        ...types.map(([type, source]) =>
          v.object({ _type: v.literal(type), ...fieldsOf(source) }),
        ),
        ...createMetadataSchemas(),
      ];
      return toMongoValidator(
        schemas.length > 0 ? v.union(schemas) : v.object({ _type: v.string() }),
      );
    }
    case "scopedMultiCollection": {
      const schemas = types.map(([type, source]) =>
        v.object({
          _id: dbId(type),
          _type: v.literal(type),
          _scope: entry.scope!,
          ...fieldsOf(source),
        }),
      );
      return toMongoValidator(v.union(schemas));
    }
    default:
      return undefined;
  }
}

export async function actualValidator(
  context: StudioContext,
  name: string,
): Promise<unknown> {
  const [info] = await context.db.listCollections({ name }).toArray();
  return (info as { options?: { validator?: unknown } } | undefined)?.options
    ?.validator;
}

export type ValidatorStatus = ValidatorDriftRow["status"] | "not-applied";

export interface ValidatorComparison {
  status: ValidatorStatus;
  baseline?: { id: string; name: string };
  expected?: unknown;
  actual?: unknown;
}

export function compareValidators(
  expected: unknown,
  actual: unknown,
): ValidatorDriftRow["status"] {
  if (expected === undefined) return actual ? "unexpected" : "matching";
  if (!actual) return "missing";
  return JSON.stringify(actual) === JSON.stringify(expected)
    ? "matching"
    : "different";
}

export async function validatorFor(
  context: StudioContext,
  entry: CatalogEntry,
): Promise<ValidatorComparison> {
  const actual = entry.exists
    ? await actualValidator(context, entry.name)
    : undefined;
  const applied = new Set(await getAppliedMigrationIds(context.db));
  const lastApplied = [...context.migrations]
    .reverse()
    .find((m) => applied.has(m.id));
  if (!lastApplied) {
    return {
      status: "not-applied",
      expected: expectedValidator(entry),
      actual,
    };
  }
  const under = entryUnder(entry, lastApplied.schemas);
  const expected = under ? expectedValidator(under) : undefined;
  return {
    status: compareValidators(expected, actual),
    baseline: baselineOf(lastApplied),
    expected,
    actual,
  };
}

function baselineOf(migration: MigrationDefinition | undefined) {
  return migration ? { id: migration.id, name: migration.name } : undefined;
}

export async function getDrift(context: StudioContext): Promise<DriftReport> {
  const catalog = await buildCatalog(context);
  const last = context.migrations[context.migrations.length - 1];
  const applied = new Set(await getAppliedMigrationIds(context.db));
  const lastApplied = [...context.migrations]
    .reverse()
    .find((m) => applied.has(m.id));

  const schema: DriftReport["schema"] = {
    available: context.schemasSource === "project" && Boolean(last),
    baseline: baselineOf(last),
    rows: [],
  };
  if (schema.available && last) {
    schema.rows = schemaDrift(last.schemas, context.schemas);
    if (schema.rows.length > 0) schema.command = "mongodbee generate";
  }

  const validators: DriftReport["validators"] = {
    baseline: baselineOf(lastApplied),
    rows: [],
  };
  if (lastApplied) {
    for (const entry of catalog) {
      if (
        !entry.exists ||
        entry.kind === "internal" ||
        entry.kind === "undeclared"
      )
        continue;
      const under = entryUnder(entry, lastApplied.schemas);
      const actual = await actualValidator(context, entry.name);
      if (!under) {
        if (actual)
          validators.rows.push({
            collection: entry.name,
            status: "unexpected",
          });
        continue;
      }
      validators.rows.push({
        collection: entry.name,
        status: compareValidators(expectedValidator(under), actual),
      });
    }
  }

  const indexes: IndexDriftRow[] = [];
  for (const entry of catalog) {
    if (!entry.exists || entry.kind === "internal") continue;
    const report = await getIndexReport(context, entry.name);
    indexes.push({
      collection: entry.name,
      summary: report.summary,
      problems: report.rows
        .filter((row) => row.status !== "matching")
        .map((row) => ({
          name: row.name,
          status: row.status,
          hint: row.hint?.command,
        })),
    });
  }

  return { schema, validators, indexes };
}
