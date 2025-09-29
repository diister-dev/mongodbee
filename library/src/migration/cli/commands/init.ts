/**
 * Init command for MongoDBee Migration CLI
 *
 * Initializes migration configuration
 *
 * @module
 */

import * as path from "@std/path";
import { existsSync } from "@std/fs";
import { green, yellow, red, dim, bold } from "@std/fmt/colors";
import { prettyText } from "../utils.ts";

export interface InitCommandOptions {
  force?: boolean;
}

/**
 * Initialize migration configuration
 */
export async function initCommand(options: InitCommandOptions = {}): Promise<void> {
  console.log(bold("🐝 Initializing MongoDBee configuration..."));
  console.log();

  const configFilePath = path.resolve("./mongodbee.config.ts");
  const schemasFilePath = path.resolve("./schemas.ts");
  const migrationsDir = path.resolve("./migrations");

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
    await Deno.mkdir(migrationsDir, { recursive: true });
    console.log(green(`✓ Created migrations directory`));
    console.log(dim(`  ${migrationsDir}`));
  }

  // Create schemas file
  if (!existsSync(schemasFilePath) || options.force) {
    await Deno.writeTextFile(schemasFilePath, prettyText(`
      /**
       * Current database schema
       *
       * This file represents the current state of your database schema.
       * It should always match the schema of your last migration.
       *
       * @module
       */

      export const schemas = {
        collections: {
          // Example:
          // "users": {
          //   _id: v.string(),
          //   name: v.string(),
          //   email: v.string(),
          // }
        },
        multiCollections: {
          // Example:
          // "catalog": {
          //   product: {
          //     _id: v.string(),
          //     name: v.string(),
          //     price: v.number(),
          //   }
          // }
        }
      };
    `));

    console.log(green(`✓ Created schemas file`));
    console.log(dim(`  ${schemasFilePath}`));
  }

  // Write config file
  await Deno.writeTextFile(configFilePath, prettyText(`
    import { defineConfig } from "@diister/mongodbee";
    import { env } from "node:process";

    export default defineConfig({
      schema: "./schemas.ts",
      db: {
        uri: "mongodb://localhost:27017",
        name: "myapp",
        username: env.MONGODBEE_USERNAME,
        password: env.MONGODBEE_PASSWORD,
      },
      paths: {
        migrationsDir: "./migrations",
      }
    });
  `));

  console.log(green(`✓ Created configuration file`));
  console.log(dim(`  ${configFilePath}`));
  console.log();

  console.log(bold("Next steps:"));
  console.log(dim("  1. Update mongodbee.config.ts with your database details"));
  console.log(dim("  2. Define your schemas in schemas.ts"));
  console.log(dim("  3. Generate your first migration:"));
  console.log(dim("     mongodbee generate --name initial"));
  console.log();
}