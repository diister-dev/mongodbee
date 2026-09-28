import process from "node:process";
import * as path from "node:path";
import { existsSync } from "node:fs";
import type { Db } from "@diister/mongodbee";
import { loadConfig } from "@diister/mongodbee/inspect";
import {
  buildMigrationChain,
  loadAllMigrations,
} from "@diister/mongodbee/inspect";
import { loadProjectSchema } from "@diister/mongodbee/inspect";
import type {
  MigrationDefinition,
  SchemasDefinition,
} from "@diister/mongodbee/inspect";

export type SchemasSource = "project" | "latest-migration" | "none";

export interface StudioContext {
  db: Db;
  schemas: SchemasDefinition;
  schemasSource: SchemasSource;
  migrations: readonly MigrationDefinition[];
  migrationFiles: ReadonlyMap<string, string>;
  warnings: readonly string[];
  paths?: { migrations?: string; schemas?: string };
  buildId?: string;
  project?: { cwd: string; configPath?: string };
  write?: boolean;
}

export interface StudioProject {
  root: { cwd: string; configPath?: string };
  connectionUri: string;
  dbName: string;
  schemas: SchemasDefinition;
  schemasSource: SchemasSource;
  migrations: readonly MigrationDefinition[];
  migrationFiles: ReadonlyMap<string, string>;
  warnings: string[];
  paths: { migrations: string; schemas: string };
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export interface StudioProjectOptions {
  configPath?: string;
  cwd?: string;
  uri?: string;
  dbName?: string;
  migrationsDir?: string;
  schemaPath?: string;
}

export function hasTargetOverrides(options: StudioProjectOptions): boolean {
  return Boolean(
    options.uri ||
      options.dbName ||
      options.migrationsDir ||
      options.schemaPath,
  );
}

async function loadOptionalConfig(
  options: StudioProjectOptions,
  cwd: string,
  warnings: string[],
): Promise<Awaited<ReturnType<typeof loadConfig>>> {
  if (!hasTargetOverrides(options) || options.configPath) {
    return loadConfig({ configPath: options.configPath, cwd });
  }
  try {
    return await loadConfig({ cwd });
  } catch (error) {
    warnings.push(
      `No configuration used (${messageOf(error)}); the command line options define the target`,
    );
    return {};
  }
}

export async function loadStudioProject(
  options: StudioProjectOptions = {},
): Promise<StudioProject> {
  const cwd = path.resolve(options.cwd ?? process.cwd());
  const warnings: string[] = [];
  const config = await loadOptionalConfig(options, cwd, warnings);

  const migrationsDir = path.resolve(
    cwd,
    options.migrationsDir || config.paths?.migrations || "./migrations",
  );
  const schemaPath = path.resolve(
    cwd,
    options.schemaPath || config.paths?.schemas || "./schemas.ts",
  );
  const connectionUri =
    options.uri ||
    config.database?.connection?.uri ||
    "mongodb://localhost:27017";
  const dbName = options.dbName || config.database?.name || "myapp";

  let migrations: MigrationDefinition[] = [];
  const migrationFiles = new Map<string, string>();
  if (existsSync(migrationsDir)) {
    try {
      const loaded = await loadAllMigrations(migrationsDir);
      for (const { fileName, migration } of loaded) {
        migrationFiles.set(migration.id, fileName);
      }
      migrations = buildMigrationChain(loaded);
    } catch (error) {
      warnings.push(`Migrations could not be loaded: ${messageOf(error)}`);
    }
  } else {
    warnings.push(`Migrations directory not found: ${migrationsDir}`);
  }

  let schemas: SchemasDefinition = {};
  let schemasSource: SchemasSource = "none";
  try {
    schemas = await loadProjectSchema(schemaPath);
    schemasSource = "project";
  } catch (error) {
    const latest = migrations[migrations.length - 1];
    if (latest) {
      schemas = latest.schemas;
      schemasSource = "latest-migration";
      warnings.push(
        `Project schemas not loaded (${messageOf(
          error,
        )}); using the latest migration's schemas instead`,
      );
    } else {
      warnings.push(`Project schemas not loaded: ${messageOf(error)}`);
    }
  }

  return {
    root: { cwd, configPath: options.configPath },
    connectionUri,
    dbName,
    schemas,
    schemasSource,
    migrations,
    migrationFiles,
    warnings,
    paths: { migrations: migrationsDir, schemas: schemaPath },
  };
}
