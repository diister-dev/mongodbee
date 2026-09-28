import {
  getIrreversibleOperations,
  getLossyOperations,
  getMigrationSummary,
  migrationBuilder,
} from "../../migration/builder.ts";
import {
  getAllMigrationStates,
  type MigrationStateRecord,
} from "../../migration/state.ts";
import type {
  MigrationDefinition,
  MigrationRule,
} from "../../migration/types.ts";
import { MAX_DOCS_PER_COLLECTION } from "../../migration/cli/commands/check.ts";
import {
  DEFAULT_STATE_RETENTION_RATIO,
  presetDocsPerCollection,
} from "../../migration/validators/mock/config.ts";
import type { StudioContext } from "../context.ts";

export type MigrationStatus = MigrationStateRecord["status"];

export interface SimulationSettings {
  presets: Record<"quick" | "normal" | "hard", number>;
  maxDocs: number;
  defaultRetention: number;
}

export function simulationSettings(): SimulationSettings {
  return {
    presets: {
      quick: presetDocsPerCollection("quick"),
      normal: presetDocsPerCollection("normal"),
      hard: presetDocsPerCollection("hard"),
    },
    maxDocs: MAX_DOCS_PER_COLLECTION,
    defaultRetention: DEFAULT_STATE_RETENTION_RATIO,
  };
}

export type OperationFlag = "irreversible" | "lossy";

export interface OperationDescription {
  type: string;
  label: string;
  target?: string;
  scope?: string;
  documentCount?: number;
  flags: OperationFlag[];
  details: Record<string, unknown>;
}

export interface MigrationEntry {
  id: string;
  name: string;
  fileName?: string;
  parentId: string | null;
  position: number | null;
  status: MigrationStatus;
  appliedAt?: Date;
  revertedAt?: Date;
  duration?: number;
  error?: string;
  missingFile?: true;
  properties: string[];
  summary?: ReturnType<typeof getMigrationSummary>;
  operations: OperationDescription[];
  compileError?: string;
}

export interface MigrationsReport {
  total: number;
  applied: number;
  pending: number;
  migrations: MigrationEntry[];
  simulation: SimulationSettings;
}

const SKIPPED_KEYS = new Set([
  "type",
  "schema",
  "parentSchema",
  "collectionName",
  "modelType",
  "documentType",
  "newTypeName",
  "documents",
  "scope",
  "irreversible",
  "lossy",
]);

export function operationLabel(type: string): string {
  const words = type
    .split("_")
    .map((word) =>
      word === "multicollection"
        ? "multi-collection"
        : word === "multimodel"
          ? "multi-model"
          : word,
    )
    .join(" ")
    .replace("scoped multi-collection", "scoped collection");
  return words.charAt(0).toUpperCase() + words.slice(1);
}

function describeValue(key: string, value: unknown): unknown {
  if (typeof value === "function") return undefined;
  if (key === "documents" && Array.isArray(value)) {
    return { count: value.length };
  }
  if (key === "targetIdSchema") return undefined;
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) {
    return value.every((item) => item === null || typeof item !== "object")
      ? value
      : { count: value.length };
  }
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return value;
  const result: Record<string, unknown> = {};
  for (const [inner, innerValue] of Object.entries(value)) {
    const described = describeValue(inner, innerValue);
    if (described !== undefined) result[inner] = described;
  }
  return result;
}

function targetOf(rule: MigrationRule): string | undefined {
  const raw = rule as Record<string, any>;
  if (rule.type === "flow" || rule.type === "flow_to_scope") {
    const from =
      raw.from?.collection ??
      raw.from?.name ??
      raw.from?.model ??
      raw.from?.collectionName;
    return `${from} → ${raw.into?.collection}`;
  }
  if (rule.type === "rename_collection") return `${raw.from} → ${raw.to}`;
  const collection = raw.collectionName ?? raw.modelType;
  const type = raw.documentType ?? raw.newTypeName;
  if (collection && type) return `${collection}.${type}`;
  return collection;
}

export function describeOperation(
  rule: MigrationRule,
  flags: OperationFlag[] = [],
): OperationDescription {
  const raw = rule as Record<string, unknown>;
  const details: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(rule)) {
    if (SKIPPED_KEYS.has(key)) continue;
    const described = describeValue(key, value);
    if (described !== undefined) details[key] = described;
  }
  const description: OperationDescription = {
    type: rule.type,
    label: operationLabel(rule.type),
    flags,
    details,
  };
  const target = targetOf(rule);
  if (target) description.target = target;
  if (typeof raw.scope === "string") description.scope = raw.scope;
  if (Array.isArray(raw.documents)) {
    description.documentCount = raw.documents.length;
  }
  return description;
}

function compile(
  migration: MigrationDefinition,
): Pick<
  MigrationEntry,
  "properties" | "summary" | "operations" | "compileError"
> {
  try {
    const state = migration.migrate(
      migrationBuilder({
        schemas: migration.schemas,
        parentSchemas: migration.parent?.schemas,
      }),
    );
    const irreversible = new Set(getIrreversibleOperations(state.operations));
    const lossy = new Set(getLossyOperations(state.operations));
    const properties: OperationFlag[] = [];
    if (irreversible.size > 0) properties.push("irreversible");
    if (lossy.size > 0) properties.push("lossy");
    return {
      properties,
      summary: getMigrationSummary(state),
      operations: state.operations.map((rule) => {
        const flags: OperationFlag[] = [];
        if (irreversible.has(rule)) flags.push("irreversible");
        if (lossy.has(rule)) flags.push("lossy");
        return describeOperation(rule, flags);
      }),
    };
  } catch (error) {
    return {
      properties: [],
      operations: [],
      compileError: error instanceof Error ? error.message : String(error),
    };
  }
}

export async function getMigrationsReport(
  context: StudioContext,
): Promise<MigrationsReport> {
  const states = await getAllMigrationStates(context.db);
  const byId = new Map(states.map((state) => [state.id, state]));
  const known = new Set<string>();

  const migrations: MigrationEntry[] = context.migrations.map(
    (migration, position) => {
      known.add(migration.id);
      const state = byId.get(migration.id);
      const entry: MigrationEntry = {
        id: migration.id,
        name: migration.name,
        parentId: migration.parent?.id ?? null,
        position,
        status: state?.status ?? "pending",
        ...compile(migration),
      };
      const fileName = context.migrationFiles.get(migration.id);
      if (fileName) entry.fileName = fileName;
      if (state?.appliedAt) entry.appliedAt = state.appliedAt;
      if (state?.revertedAt) entry.revertedAt = state.revertedAt;
      if (state?.duration !== undefined) entry.duration = state.duration;
      if (state?.error) entry.error = state.error;
      return entry;
    },
  );

  for (const state of states) {
    if (known.has(state.id)) continue;
    const entry: MigrationEntry = {
      id: state.id,
      name: state.name,
      parentId: null,
      position: null,
      status: state.status,
      missingFile: true,
      properties: [],
      operations: [],
    };
    if (state.appliedAt) entry.appliedAt = state.appliedAt;
    if (state.revertedAt) entry.revertedAt = state.revertedAt;
    if (state.error) entry.error = state.error;
    migrations.push(entry);
  }

  const applied = migrations.filter((m) => m.status === "applied").length;
  return {
    total: context.migrations.length,
    applied,
    pending: migrations.filter(
      (m) => m.position !== null && m.status !== "applied",
    ).length,
    migrations,
    simulation: simulationSettings(),
  };
}
