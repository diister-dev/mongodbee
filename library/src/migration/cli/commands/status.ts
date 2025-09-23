/**
 * Status command for MongoDBee Migration CLI
 * 
 * Shows migration status and information
 * 
 * @module
 */

import { yellow, red, dim, blue } from "@std/fmt/colors";
import { existsSync } from "@std/fs";

import { loadConfig } from "../../config/loader.ts";

export interface StatusCommandOptions {
  configPath: string;
  environment: string;
}

/**
 * Show migration status
 */
export async function statusCommand(options: StatusCommandOptions): Promise<void> {
  const { configPath, environment } = options;

  if (!existsSync(configPath)) {
    console.error(red("Error: Configuration file not found"));
    console.log(dim("Run 'mongodbee-migrate init' to initialize configuration"));
    Deno.exit(1);
  }

  try {
    // Load configuration
    const configResult = await loadConfig({ 
      configPath,
      environment 
    });
    const config = configResult.config;

    console.log(yellow("Migration Status"));
    console.log(dim(`Environment: ${environment}`));
    console.log(dim(`Database: ${config.database.name}`));
    console.log(dim(`URI: ${config.database.connection.uri}`));
    console.log();

    // Show configuration status
    console.log(blue("Configuration:"));
    console.log(dim(`  Config file: ${configResult.source}`));
    console.log(dim(`  Migrations path: ${config.paths.migrations}`));
    console.log(dim(`  Schemas path: ${config.paths.schemas}`));
    
    if (configResult.warnings.length > 0) {
      console.log();
      console.log(yellow("Warnings:"));
      for (const warning of configResult.warnings) {
        console.log(dim(`  ⚠ ${warning}`));
      }
    }

    // TODO: Add actual migration status checking
    // This would require implementing migration state tracking in MongoDB
    console.log();
    console.log(blue("Migration Status:"));
    console.log(dim("  Status checking not yet implemented"));
    console.log(dim("  Run migrations with: mongodbee-migrate apply"));

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red("Error checking status:"), message);
    Deno.exit(1);
  }
}