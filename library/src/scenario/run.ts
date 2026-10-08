import { recomputeComputedFields } from "./computed.ts";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type MigrationDefinition,
} from "../migration/types.ts";
import { migrationBuilder } from "../migration/builder.ts";
import { createMemoryApplier } from "../migration/appliers/memory.ts";
import { buildPrivacyPlan } from "../privacy/plan.ts";
import { generateScenarioState } from "./generate.ts";
import { checkScenarioState } from "./oracle.ts";
import { docsOf } from "./state.ts";
import type {
  ScenarioReport,
  ScenarioRunResult,
  ScenarioViolation,
  SeedScenario,
} from "./types.ts";

export interface RunScenarioOptions {
  readonly migrations: readonly MigrationDefinition[];
  readonly scenario: SeedScenario;
  readonly at?: string;
  readonly defaultCount?: number;
  readonly defaultScopes?: number;
}

export interface ReplayResult {
  readonly state: DatabaseState;
  readonly applied: readonly string[];
}

export async function applyMigrationsInMemory(
  initial: DatabaseState,
  migrations: readonly MigrationDefinition[],
): Promise<ReplayResult> {
  let state = initial;
  const applied: string[] = [];
  for (const migration of migrations) {
    const operations = migration.migrate(
      migrationBuilder({
        schemas: migration.schemas,
        parentSchemas: migration.parent?.schemas,
      }),
    ).operations;
    const applier = createMemoryApplier(migration);
    for (const operation of operations) {
      state = await applier.applyOperation(state, operation);
    }
    applied.push(migration.id);
  }
  return { state, applied };
}

export function isBlockingViolation(violation: ScenarioViolation): boolean {
  return violation.blocking ?? violation.kind !== "unique_unchecked";
}

export interface ScenarioBirth {
  readonly birth: MigrationDefinition;
  readonly at: MigrationDefinition;
  readonly pending: readonly MigrationDefinition[];
  readonly state: DatabaseState;
  readonly violations: readonly ScenarioViolation[];
}

export async function generateScenarioAtBirth(
  options: RunScenarioOptions,
): Promise<ScenarioBirth> {
  const { migrations, scenario } = options;
  const ids = migrations.map((m) => m.id);
  const birthIndex = ids.indexOf(scenario.birth);
  if (birthIndex < 0) {
    throw new Error(
      `scenario "${scenario.name}": birth migration "${scenario.birth}" is not in the chain`,
    );
  }
  const atIndex =
    options.at === undefined ? migrations.length - 1 : ids.indexOf(options.at);
  if (atIndex < 0) {
    throw new Error(
      `scenario "${scenario.name}": migration "${options.at}" is not in the chain`,
    );
  }
  if (atIndex < birthIndex) {
    throw new Error(
      `scenario "${scenario.name}": cannot seed at "${
        ids[atIndex]
      }", it precedes the birth migration "${scenario.birth}"`,
    );
  }

  const birth = migrations[birthIndex];
  const lineage = await applyMigrationsInMemory(
    createEmptyDatabaseState(),
    migrations.slice(0, birthIndex + 1),
  );
  const generation = generateScenarioState({
    schemas: birth.schemas,
    scenario,
    initial: lineage.state,
    ...(options.defaultCount !== undefined && {
      defaultCount: options.defaultCount,
    }),
    ...(options.defaultScopes !== undefined && {
      defaultScopes: options.defaultScopes,
    }),
  });
  return {
    birth,
    at: migrations[atIndex],
    pending: migrations.slice(birthIndex + 1, atIndex + 1),
    state: generation.state,
    violations: generation.violations,
  };
}

export interface CheckScenarioWorldOptions {
  readonly scenario: SeedScenario;
  readonly birth: MigrationDefinition;
  readonly at: MigrationDefinition;
  readonly applied: readonly string[];
  readonly state: DatabaseState;
  readonly generationViolations: readonly ScenarioViolation[];
}

export function checkScenarioWorld(
  options: CheckScenarioWorldOptions,
): ScenarioReport {
  const { scenario, at, state } = options;
  const plan = buildPrivacyPlan({ schemas: at.schemas });
  const violations = [
    ...options.generationViolations,
    ...checkScenarioState({
      state,
      schemas: at.schemas,
      plan,
      invariants: scenario.invariants ?? [],
    }),
  ];
  const generated: Record<string, number> = {};
  for (const target of plan.targets.values()) {
    const n = docsOf(state, target).length;
    if (n > 0) generated[target.key] = n;
  }
  return {
    scenario: scenario.name,
    birth: options.birth.id,
    at: at.id,
    applied: options.applied,
    generated,
    violations,
    ok: violations.every((v) => !isBlockingViolation(v)),
  };
}

export async function runScenario(
  options: RunScenarioOptions,
): Promise<ScenarioRunResult> {
  const world = await generateScenarioAtBirth(options);
  const replay = await applyMigrationsInMemory(world.state, world.pending);
  const state = replay.state;
  recomputeComputedFields(state, world.at.schemas);
  const report = checkScenarioWorld({
    scenario: options.scenario,
    birth: world.birth,
    at: world.at,
    applied: replay.applied,
    state,
    generationViolations: world.violations,
  });
  return { state, report };
}

export function renderScenarioReport(report: ScenarioReport): string {
  const lines: string[] = [];
  lines.push(`scenario    ${report.scenario}`);
  lines.push(`birth       ${report.birth}`);
  lines.push(
    `at          ${report.at}${
      report.applied.length > 0
        ? ` (${report.applied.length} migration(s) applied)`
        : ""
    }`,
  );
  lines.push("");
  for (const [target, count] of Object.entries(report.generated)) {
    lines.push(`  ${target.padEnd(60)} ${String(count).padStart(7)}`);
  }
  lines.push("");
  const blocking = report.violations.filter(isBlockingViolation);
  const notes = report.violations.length - blocking.length;
  if (blocking.length === 0) {
    lines.push(`oracle      ok${notes > 0 ? ` (${notes} note(s))` : ""}`);
  } else {
    lines.push(
      `oracle      ${blocking.length} violation(s)${
        notes > 0 ? `, ${notes} note(s)` : ""
      }`,
    );
    for (const violation of report.violations) {
      lines.push(
        `  ${violation.kind.padEnd(
          20,
        )} ${violation.target}\n      ${violation.message}`,
      );
    }
  }
  return lines.join("\n");
}
