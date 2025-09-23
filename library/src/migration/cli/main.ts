#!/usr/bin/env -S deno run --allow-read --allow-write --allow-net --allow-env
/**
 * MongoDBee Migration CLI
 * 
 * CLI tool for managing MongoDB migrations with MongoDBee
 * 
 * @module
 */

import { parseArgs } from "@std/cli/parse-args";
import { blue, bold, green, red, yellow, dim } from "@std/fmt/colors";
import * as path from "@std/path";
import { existsSync } from "@std/fs";

import { generateCommand } from "./commands/generate.ts";
import { applyCommand } from "./commands/apply.ts";
import { rollbackCommand } from "./commands/rollback.ts";
import { statusCommand } from "./commands/status.ts";
import { initCommand } from "./commands/init.ts";
import { loadConfig } from "../config/loader.ts";

const VERSION = "0.1.0";

/**
 * Discover the first existing config file
 */
async function discoverConfigFile(): Promise<string | null> {
  const configFiles = [
    './mongodbee.config.ts',
    './mongodbee.config.js', 
    './mongodbee.config.json',
    './mongodbee.json',
    './.mongodbee.json',
    './config/mongodbee.json',
    './config/migrations.json',
  ];

  for (const configFile of configFiles) {
    try {
      await Deno.stat(configFile);
      return path.resolve(configFile);
    } catch {
      // File doesn't exist, continue to next
    }
  }
  return null;
}

/**
 * Display help information
 */
function showHelp(): void {
  console.log(`
${bold(blue("MongoDBee Migration CLI"))} v${VERSION}

${yellow("USAGE:")}
  mongodbee-migrate [COMMAND] [OPTIONS]

${yellow("COMMANDS:")}
  ${green("init")}      Initialize migration configuration
  ${green("generate")}  Generate a new migration file
  ${green("apply")}     Apply pending migrations
  ${green("rollback")}  Rollback the last applied migration
  ${green("status")}    Show migration status

${yellow("GLOBAL OPTIONS:")}
  -h, --help     Show this help message
  -v, --version  Show version information
  --config       Path to configuration file (default: mongodbee.config.json)
  --env          Environment to use (default: development)

${yellow("EXAMPLES:")}
  ${dim("# Initialize configuration")}
  mongodbee-migrate init

  ${dim("# Generate a new migration")}
  mongodbee-migrate generate --name add-users-collection --template create-collection

  ${dim("# Apply all pending migrations")}
  mongodbee-migrate apply

  ${dim("# Rollback the last migration")}
  mongodbee-migrate rollback

  ${dim("# Check migration status")}
  mongodbee-migrate status --env production
`);
}

/**
 * Display version information
 */
function showVersion(): void {
  console.log(`MongoDBee Migration CLI v${VERSION}`);
}

/**
 * Main CLI entry point
 */
async function main(): Promise<void> {
  const args = parseArgs(Deno.args, {
    boolean: ["help", "version", "force", "dry-run"],
    string: ["config", "env", "name", "template", "target"],
    alias: {
      h: "help",
      v: "version",
      n: "name",
      t: "template",
    },
    default: {
      env: "development",
      force: false,
      "dry-run": false,
    },
  });

  // Handle global flags
  if (args.help) {
    showHelp();
    return;
  }

  if (args.version) {
    showVersion();
    return;
  }

  const command = args._[0] as string;

  if (!command) {
    console.error(red("Error: No command specified"));
    showHelp();
    Deno.exit(1);
  }

  try {
    // Resolve config path - either from args or discover automatically
    let configPath: string;
    if (args.config) {
      configPath = path.isAbsolute(args.config)
        ? args.config
        : path.resolve(args.config);
    } else {
      const discoveredPath = await discoverConfigFile();
      if (!discoveredPath) {
        console.error(red("Error: No configuration file found."));
        console.error(dim("Expected files: mongodbee.config.ts, mongodbee.config.js, mongodbee.config.json"));
        console.error(dim("Run 'mongodbee init' to create a configuration file."));
        Deno.exit(1);
      }
      configPath = discoveredPath;
    }

    switch (command) {
      case "init":
        await initCommand({
          configPath,
          force: args.force,
        });
        break;

      case "generate":
        if (!args.name) {
          console.error(red("Error: --name is required for generate command"));
          Deno.exit(1);
        }

        await generateCommand({
          configPath,
          environment: args.env,
          name: args.name,
          template: args.template || "empty",
        });
        break;

      case "apply":
        await applyCommand({
          configPath,
          environment: args.env,
          target: args.target,
          dryRun: args["dry-run"],
        });
        break;

      case "rollback":
        await rollbackCommand({
          configPath,
          environment: args.env,
          target: args.target,
          dryRun: args["dry-run"],
        });
        break;

      case "status":
        await statusCommand({
          configPath,
          environment: args.env,
        });
        break;

      default:
        console.error(red(`Error: Unknown command "${command}"`));
        showHelp();
        Deno.exit(1);
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(red("Error:"), errorMessage);
    if (args.verbose || Deno.env.get("DEBUG")) {
      console.error(error instanceof Error ? error.stack : error);
    }
    Deno.exit(1);
  }
}

// Run main function if this is the main module
if (import.meta.main) {
  await main();
}

export { main };