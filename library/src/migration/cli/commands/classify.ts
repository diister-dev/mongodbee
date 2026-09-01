import process from "node:process";
import { blue, bold, dim, red } from "@std/fmt/colors";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { loadProjectSchema } from "../../schema-validation.ts";
import { resolveMigrationRef } from "../utils/resolve-ref.ts";
import {
  buildPrivacyPlan,
  type PrivacyPlan,
  renderPrivacyReport,
} from "../../../privacy/mod.ts";
import type { SchemasDefinition } from "../../types.ts";

export interface ClassifyCommandOptions {
  configPath?: string;
  cwd?: string;
  at?: string;
  json?: boolean;
}

export function serializePrivacyPlan(
  plan: PrivacyPlan,
): Record<string, unknown> {
  return {
    persons: [...plan.persons.values()],
    targets: [...plan.targets.values()],
    findings: plan.findings,
    summary: plan.summary,
  };
}

export async function loadSchemasAt(
  cwd: string,
  config: { paths?: { migrations?: string; schemas?: string } },
  at: string | undefined,
): Promise<{ schemas: SchemasDefinition; label: string }> {
  if (at === undefined) {
    const schemaPath = path.resolve(
      cwd,
      config.paths?.schemas || "./schemas.ts",
    );
    return { schemas: await loadProjectSchema(schemaPath), label: schemaPath };
  }
  const migrationsDir = path.resolve(
    cwd,
    config.paths?.migrations || "./migrations",
  );
  const chain = buildMigrationChain(await loadAllMigrations(migrationsDir));
  const migration = resolveMigrationRef(chain, at);
  return { schemas: migration.schemas, label: `migration ${migration.id}` };
}

export async function classifyCommand(
  options: ClassifyCommandOptions = {},
): Promise<void> {
  const cwd = options.cwd || process.cwd();
  const config = await loadConfig({ configPath: options.configPath, cwd });
  const { schemas, label } = await loadSchemasAt(cwd, config, options.at);
  const plan = buildPrivacyPlan({ schemas });

  if (options.json) {
    console.log(JSON.stringify(serializePrivacyPlan(plan), null, 2));
  } else {
    console.log(bold(blue("🐝 Classifying personal data...")));
    console.log(dim(`Schemas: ${label}`));
    console.log();
    console.log(renderPrivacyReport(plan));
  }

  const errors = plan.findings.filter((f) => f.level === "error");
  if (errors.length > 0) {
    if (!options.json) {
      console.log(red(`\n✗ ${errors.length} classification error(s)`));
    }
    throw new Error(`Privacy classification has ${errors.length} error(s)`);
  }
}
