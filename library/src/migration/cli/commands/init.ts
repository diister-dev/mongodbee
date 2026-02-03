/**
 * Init command for MongoDBee Migration CLI
 *
 * Initializes migration configuration
 *
 * @module
 */

import process from "node:process";
import * as fs from "node:fs/promises";
import { existsSync } from "node:fs";
import * as path from "@std/path";
import { bold, dim, green, yellow } from "@std/fmt/colors";
import { prettyText } from "../utils.ts";

export interface InitCommandOptions {
  force?: boolean;
  cwd?: string;
}

/**
 * Initialize migration configuration
 */
export async function initCommand(
  options: InitCommandOptions = {},
): Promise<void> {
  console.log(bold("🐝 Initializing MongoDBee configuration..."));
  console.log();

  const cwd = options.cwd || process.cwd();
  const configFilePath = path.resolve(cwd, "./mongodbee.config.ts");
  const schemasFilePath = path.resolve(cwd, "./schemas.ts");
  const migrationsDir = path.resolve(cwd, "./migrations");

  // Check if config already exists
  if (existsSync(configFilePath) && !options.force) {
    console.log(yellow("Configuration file already exists."));
    console.log(dim(`  ${configFilePath}`));
    console.log();
    console.log(dim("Use --force to overwrite."));
    return;
  }

  // Create migrations directory
  if (!existsSync(migrationsDir)) {
    await fs.mkdir(migrationsDir, { recursive: true });
    console.log(green(`✓ Created migrations directory`));
    console.log(dim(`  ${migrationsDir}`));
  }

  // Create schemas file
  if (!existsSync(schemasFilePath) || options.force) {
    await fs.writeFile(
      schemasFilePath,
      prettyText(`
      /**
       * Current database schema
       *
       * This file represents the current state of your database schema.
       * It should always match the schema of your last migration.
       *
       * @module
       */
      import { type SchemasDefinition } from "@diister/mongodbee";

      export const schemas = {
        collections: {
          // @see @TODO
        },
        multiCollections: {
          // @see @TODO
        },
        multiModels: {
          // @see @TODO
        }
      } satisfies SchemasDefinition;
    `),
      "utf-8",
    );

    console.log(green(`✓ Created schemas file`));
    console.log(dim(`  ${schemasFilePath}`));
  }

  // Write config file
  await fs.writeFile(
    configFilePath,
    prettyText(`
    import { defineConfig } from "@diister/mongodbee";

    export default defineConfig({
      database: {
        connection: {
          uri: "mongodb://localhost:27017"
        },
        name: "myapp"
      },
      paths: {
        migrations: "./migrations",
        schemas: "./schemas.ts"
      }
    });
  `),
    "utf-8",
  );

  console.log(green(`✓ Created configuration file`));
  console.log(dim(`  ${configFilePath}`));
  console.log();

  console.log(bold("Next steps:"));
  console.log(
    dim("  1. Update mongodbee.config.ts with your database details"),
  );
  console.log(dim("  2. Define your schemas in schemas.ts"));
  console.log(dim("  3. Generate your first migration:"));
  console.log(dim("     mongodbee generate --name initial"));
  console.log();
}
