/**
 * Apply command for MongoDBee Migration CLI
 * 
 * Applies pending migrations to the database
 * 
 * @module
 */

import { yellow, red, dim } from "@std/fmt/colors";
import { existsSync } from "@std/fs";

import { loadConfig } from "../../config/loader.ts";

export interface ApplyCommandOptions {
  configPath: string;
  environment: string;
  target?: string;
  dryRun?: boolean;
}

/**
 * Apply pending migrations
 */
export async function applyCommand(options: ApplyCommandOptions): Promise<void> {
  const { configPath, environment } = options;

  const configResult = await loadConfig({ 
    configPath,
    environment 
  });

  console.log(configResult);
}