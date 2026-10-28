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

export type InFlightStep = MigrationStep & { readonly index: number };

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
      readonly applied: readonly MigrationStep[];
      readonly index: number;
      readonly was: MigrationStep | undefined;
      readonly now: MigrationStep | undefined;
    };

const TARGET_KEYS = [
  "collectionName",
  "documentType",
  "modelType",
  "oldTypeName",
  "newTypeName",
  "field",
  "to",
] as const;

const SOURCE_KEYS = [
  "collection",
  "kind",
  "name",
  "model",
  "collectionName",
  "documentType",
] as const;

function namesOf(
  value: unknown,
  keys: readonly string[],
): Record<string, string> {
  if (typeof value !== "object" || value === null) return {};
  const fields = value as Record<string, unknown>;
  return Object.fromEntries(
    keys.flatMap((key) =>
      typeof fields[key] === "string" ? [[key, fields[key]]] : [],
    ),
  );
}

function identity(operation: MigrationRule): unknown {
  const fields = operation as Record<string, unknown>;
  return {
    type: operation.type,
    target: namesOf(fields, TARGET_KEYS),
    from:
      typeof fields.from === "string"
        ? fields.from
        : namesOf(fields.from, SOURCE_KEYS),
    into: namesOf(fields.into, ["collection"]),
  };
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
      .update(JSON.stringify(identity(operation)))
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
        applied,
        index,
        was: applied[index],
        now: current[index],
      };
    }
  }
  if (applied.length > current.length) {
    return {
      kind: "changed",
      applied,
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
  readonly inFlight?: InFlightStep;
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
      ...(record.inFlight ? { inFlight: record.inFlight } : {}),
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

export async function replaceRecordedSteps(
  db: Db,
  migrationId: string,
  read: number,
  steps: readonly MigrationStep[],
): Promise<void> {
  const latest = await latestAppliedRecord(db, migrationId);
  const updated = latest
    ? await getMigrationOperationsCollection(db).updateOne(
        { _id: latest._id, steps: { $size: read } },
        { $set: { steps: [...steps] } },
      )
    : undefined;
  if (updated?.matchedCount !== 1) {
    throw new ConcurrentStepRecordError(migrationId, read);
  }
}

export function acceptedSteps(
  drift: Extract<StepDrift, { kind: "changed" }>,
  current: readonly MigrationStep[],
): readonly MigrationStep[] {
  return current.slice(0, Math.min(drift.applied.length, current.length));
}

export function acceptDriftIds(raw: unknown): string[] {
  if (raw === undefined || raw === null || raw === false) return [];
  return (Array.isArray(raw) ? raw : [raw]).map((id) => String(id));
}

export function acceptDriftHint(
  command: "migrate" | "rollback",
  migrationId: string,
): string {
  return `If the migration did not really change (a signature that moved on its own), accept its current operations as the ones that ran: mongodbee ${command} --accept-drift ${migrationId}`;
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
        { $push: { steps: step }, $unset: { inFlight: "" } },
      )
    : undefined;
  if (updated?.modifiedCount !== 1) {
    throw new ConcurrentStepRecordError(migrationId, recorded);
  }
}

export async function markStepInFlight(
  db: Db,
  migrationId: string,
  recorded: number,
  step: MigrationStep,
): Promise<void> {
  const latest = await latestAppliedRecord(db, migrationId);
  const inFlight: InFlightStep = { ...step, index: recorded };
  const updated = latest
    ? await getMigrationOperationsCollection(db).updateOne(
        { _id: latest._id, steps: { $size: recorded } },
        { $set: { inFlight } },
      )
    : undefined;
  if (updated?.matchedCount !== 1) {
    throw new ConcurrentStepRecordError(migrationId, recorded);
  }
}

const RERUNNABLE_OPERATION_TYPES: ReadonlySet<MigrationRule["type"]> = new Set([
  "seed_collection",
  "seed_multicollection_type",
  "seed_scoped_multicollection_type",
  "seed_multimodel_instance_type",
  "seed_multimodel_instances_type",
]);

export function isRerunnable(operation: MigrationRule): boolean {
  return RERUNNABLE_OPERATION_TYPES.has(operation.type);
}

export function recordInFlightCommand(migrationId: string): string {
  return `mongodbee migrate --record-in-flight ${migrationId}`;
}

export function rerunInFlightCommand(migrationId: string): string {
  return `mongodbee migrate --rerun-in-flight ${migrationId}`;
}

export function describeInFlight(
  migration: MigrationDefinition,
  inFlight: InFlightStep,
): string {
  return (
    `${migration.name} (${migration.id}): a previous migrate stopped (a crash, or the operation or its record failed) while running operation #${inFlight.index + 1} "${inFlight.label}", ` +
    "added to this migration after it was applied, before recording it; it may be half-applied"
  );
}

export function inFlightWaysOut(migrationId: string): string {
  return (
    "Check its data, then run one of: " +
    `if it was fully applied, record it without running it: ${recordInFlightCommand(migrationId)}; ` +
    `if it was not applied, or you undid it, run it again: ${rerunInFlightCommand(migrationId)}`
  );
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
