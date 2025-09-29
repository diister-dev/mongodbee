/**
 * Init command for MongoDBee Migration CLI
 * 
 * Initializes migration configuration
 * 
 * @module
 */

import * as path from "@std/path";
import { existsSync } from "@std/fs";
import { prettyText } from "../utils.ts";

export interface InitCommandOptions {
  configPath: string;
  force?: boolean;
}

/**
 * Initialize migration configuration
 */
export async function initCommand(options: InitCommandOptions): Promise<void> {
  const configFilePath = path.resolve("./mongodbee.config.ts");

  await Deno.writeTextFile(configFilePath, prettyText(`
    import { defineConfig } from "@diister/mongodbee"

    export default defineConfig({
      paths: {
        migrationsDir: "./migrations",
        schema: "./schemas.ts",
      }
    });
  `));
}