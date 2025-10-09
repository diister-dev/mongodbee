/**
 * Generate command for MongoDBee Migration CLI
 *
 * Generates new migration files
 *
 * @module
 */

import { dim, green, red } from "@std/fmt/colors";
import * as path from "@std/path";
import { existsSync } from "@std/fs";
import { generateMigrationId } from "../../definition.ts";
import { prettyText } from "../utils.ts";
import { loadConfig } from "../../config/loader.ts";

async function extractMigrationDefinitions(
  migrationsDir: string,
): Promise<any[]> {
  const migrations = Deno.readDirSync(migrationsDir);
  const migrationsPaths = [...migrations].map((mig) => mig.name)
    .filter((name) => name.endsWith(".ts"))
    .sort((a, b) => a.localeCompare(b))
    .map(async (name) => {
      const migrationPath = path.toFileUrl(path.join(migrationsDir, name)).href;
      return [name, await import(migrationPath)];
    });

  const migrationsModules = (await Promise.all(migrationsPaths)).map(
    ([name, mod]) => {
      const def = mod.default;
      return [name, def];
    },
  );

  return migrationsModules;
}

export type GenerateCommandOptions = {
  name?: string;
  configPath?: string;
  cwd?: string;
};

export const generateCommandOptions = {
  string: ["name"],
  alias: { n: "name" },
  default: { name: "" },
};

/**
 * Generate a new migration file
 */
export async function generateCommand(
  options: GenerateCommandOptions,
): Promise<void> {
  const cwd = options.cwd || Deno.cwd();
  const config = await loadConfig({ configPath: options.configPath, cwd });

  const migrationsDir = config.paths?.migrations || "./migrations";
  const migrationsDirPath = path.resolve(cwd, migrationsDir);
  if (!existsSync(migrationsDirPath)) {
    console.log(red(`Migrations directory does not exist: ${migrationsDir}`));
    return;
  }

  const migrationsDefinitions = await extractMigrationDefinitions(
    migrationsDirPath,
  );
  const lastMigration = migrationsDefinitions[migrationsDefinitions.length - 1];

  // Ensure a parent is never used twice
  const parents = new Set<string>();
  for (const [name, def] of migrationsDefinitions) {
    if (def.parent) {
      if (parents.has(def.parent.id)) {
        console.log(
          red(
            `Error: Migration ${name} has a parent ID that is already used by another migration: ${def.parent.id}`,
          ),
        );
        return;
      }
      parents.add(def.parent.id);
    }
  }

  const id = generateMigrationId(options.name);

  const generation = `
    /**
     * This migration was generated using MongoDBee CLI
     * Please edit the migration logic in the migrate() function.
     * @module
     */

    import { migrationDefinition } from "@diister/mongodbee/migration";${
    lastMigration
      ? `
    import parent from "./${lastMigration[0]}";`
      : ""
  }

    const id = "${id}";
    const name = "${options.name || "Migration Name"}";

    export default migrationDefinition(id, name, {
      parent: ${lastMigration ? "parent" : "null"},
      schemas: {
        collections: {${
    lastMigration?.[1]?.schemas?.collections
      ? `
          ...parent.schemas.collections,`
      : `
          // \"<collection_name>\" : {}`
  }
        },
        multiCollections: {${
    lastMigration?.[1]?.schemas?.multiCollections
      ? `
          ...parent.schemas.multiCollections,`
      : `
          // \"<collection_name>\" : {}`
  }
        },
        multiModels: {${
    lastMigration?.[1]?.schemas?.multiModels
      ? `
          ...parent.schemas.multiModels,`
      : `
          // \"<collection_type>\" : {}`
  }
        }
      },
      migrate(migration) {
        return migration.compile();
      },
    })
  `;

  const fileName = `${id.replace(/-/g, "_")}.ts`;
  const filePath = path.join(migrationsDirPath, fileName);
  await Deno.writeTextFile(filePath, prettyText(generation));

  console.log(`${green("Migration file created")}: ${filePath}`);
}
