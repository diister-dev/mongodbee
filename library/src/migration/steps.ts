import { createHash } from "node:crypto";
import type { Db } from "../mongodb.ts";
import { migrationBuilder } from "./builder.ts";
import {
  getAllOperations,
  getMigrationOperationsCollection,
  type MigrationOperation,
} from "./history.ts";
import type { MigrationDefinition, MigrationRule } from "./types.ts";

export type MigrationStep = {
  readonly signature: string;
  readonly label: string;
};

export type StepDrift =
  | { readonly kind: "same" }
  | { readonly kind: "unrecorded" }
  | {
      readonly kind: "extended";
      readonly applied: readonly MigrationStep[];
      readonly added: readonly MigrationStep[];
    }
  | {
      readonly kind: "changed";
      readonly index: number;
      readonly was: MigrationStep | undefined;
      readonly now: MigrationStep | undefined;
    };

const UNSIGNED_KEYS = new Set(["schema", "parentSchema", "targetIdSchema"]);

function isValibotSchema(value: object): boolean {
  return "~standard" in value && "kind" in value;
}

function canonical(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  if (typeof value === "function") return "[function]";
  if (typeof value === "bigint") return value.toString();
  if (typeof value !== "object") return value;
  if (value instanceof Date) return "[date]";
  if (value instanceof RegExp) return value.toString();
  if (isValibotSchema(value)) return "[schema]";
  if (Array.isArray(value)) return value.map(canonical);
  const fields = value as Record<string, unknown>;
  return Object.fromEntries(
    Object.keys(fields)
      .filter((key) => !UNSIGNED_KEYS.has(key) && fields[key] !== undefined)
      .sort()
      .map((key) => {
        const field = fields[key];
        return key === "documents" && Array.isArray(field)
          ? [key, { count: field.length }]
          : [key, canonical(field)];
      }),
  );
}

export function operationLabel(operation: MigrationRule): string {
  const fields = operation as Record<string, unknown>;
  const from = fields.from as { collection?: string } | string | undefined;
  const into = fields.into as { collection?: string } | undefined;
  const target =
    (fields.collectionName as string | undefined) ??
    (fields.modelType as string | undefined) ??
    into?.collection ??
    (typeof from === "string" ? from : from?.collection) ??
    "";
  const documentType = fields.documentType
    ? `.${fields.documentType as string}`
    : "";
  return target
    ? `${operation.type}: ${target}${documentType}`
    : operation.type;
}

export function stepsOf(operations: readonly MigrationRule[]): MigrationStep[] {
  return operations.map((operation) => ({
    signature: createHash("sha256")
      .update(JSON.stringify(canonical(operation)))
      .digest("hex"),
    label: operationLabel(operation),
  }));
}

export function operationsOf(migration: MigrationDefinition): MigrationRule[] {
  return migration.migrate(migrationBuilder({ schemas: migration.schemas }))
    .operations;
}

export function compareSteps(
  applied: readonly MigrationStep[] | undefined,
  current: readonly MigrationStep[],
): StepDrift {
  if (applied === undefined) return { kind: "unrecorded" };
  const shared = Math.min(applied.length, current.length);
  for (let index = 0; index < shared; index++) {
    if (applied[index].signature !== current[index].signature) {
      return {
        kind: "changed",
        index,
        was: applied[index],
        now: current[index],
      };
    }
  }
  if (applied.length > current.length) {
    return {
      kind: "changed",
      index: current.length,
      was: applied[current.length],
      now: undefined,
    };
  }
  if (applied.length < current.length) {
    return {
      kind: "extended",
      applied,
      added: current.slice(applied.length),
    };
  }
  return { kind: "same" };
}

function stepsOfRecord(
  record: MigrationOperation | undefined,
): MigrationStep[] | undefined {
  return record?.operation === "applied" ? record.steps : undefined;
}

function lastSuccessByMigration(
  operations: readonly MigrationOperation[],
): Map<string, MigrationOperation> {
  const last = new Map<string, MigrationOperation>();
  for (const operation of operations) {
    if (operation.status === "success") {
      last.set(operation.migrationId, operation);
    }
  }
  return last;
}

export type AppliedMigrationSteps = {
  readonly migration: MigrationDefinition;
  readonly current: readonly MigrationStep[];
  readonly drift: StepDrift;
};

export async function appliedMigrationSteps(
  db: Db,
  chain: readonly MigrationDefinition[],
): Promise<AppliedMigrationSteps[]> {
  const last = lastSuccessByMigration(await getAllOperations(db));
  const applied: AppliedMigrationSteps[] = [];
  for (const migration of chain) {
    const record = last.get(migration.id);
    if (record?.operation !== "applied") continue;
    const current = stepsOf(operationsOf(migration));
    applied.push({
      migration,
      current,
      drift: compareSteps(stepsOfRecord(record), current),
    });
  }
  return applied;
}

async function latestAppliedRecord(db: Db, migrationId: string) {
  const [latest] = await getMigrationOperationsCollection(db)
    .find({ migrationId, status: "success" })
    .sort({ executedAt: -1 })
    .limit(1)
    .toArray();
  return latest?.operation === "applied" ? latest : undefined;
}

export async function recordAssumedSteps(
  db: Db,
  migrationId: string,
  steps: readonly MigrationStep[],
): Promise<void> {
  const latest = await latestAppliedRecord(db, migrationId);
  if (latest === undefined || latest.steps !== undefined) return;
  await getMigrationOperationsCollection(db).updateOne(
    { _id: latest._id, steps: { $exists: false } },
    { $set: { steps: [...steps] } },
  );
}

export class ConcurrentStepRecordError extends Error {
  constructor(migrationId: string, recorded: number) {
    super(
      `${migrationId}: its record no longer holds the ${recorded} operation(s) this run read; ` +
        "another migrate run is recording it. Stopped; run migrate again once it is done.",
    );
    this.name = "ConcurrentStepRecordError";
  }
}

export async function appendAppliedStep(
  db: Db,
  migrationId: string,
  recorded: number,
  step: MigrationStep,
): Promise<void> {
  const latest = await latestAppliedRecord(db, migrationId);
  const updated = latest
    ? await getMigrationOperationsCollection(db).updateOne(
        { _id: latest._id, steps: { $size: recorded } },
        { $push: { steps: step } },
      )
    : undefined;
  if (updated?.modifiedCount !== 1) {
    throw new ConcurrentStepRecordError(migrationId, recorded);
  }
}

function stepList(steps: readonly MigrationStep[], from: number): string {
  return steps
    .map((step, offset) => `#${from + offset + 1} ${step.label}`)
    .join(", ");
}

export function describeDrift(
  migration: MigrationDefinition,
  drift: StepDrift,
): string {
  const named = `${migration.name} (${migration.id})`;
  switch (drift.kind) {
    case "extended":
      return (
        `${named} was applied with ${drift.applied.length} operation(s) and now has ` +
        `${drift.applied.length + drift.added.length}: ` +
        `${stepList(drift.added, drift.applied.length)} added since`
      );
    case "changed":
      return (
        `${named} changed after it was applied: operation #${drift.index + 1} ` +
        `was ${drift.was ? `"${drift.was.label}"` : "absent"}, ` +
        `is now ${drift.now ? `"${drift.now.label}"` : "absent"}`
      );
    case "unrecorded":
      return `${named} was applied before mongodbee recorded its operations`;
    case "same":
      return `${named} is applied as it stands`;
  }
}
