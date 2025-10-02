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

import { generateCommand } from "./commands/generate.ts";
import { applyCommand } from "./commands/apply.ts";
import { rollbackCommand } from "./commands/rollback.ts";
import { statusCommand } from "./commands/status.ts";
import { historyCommand } from "./commands/history.ts";
import { initCommand } from "./commands/init.ts";

import packageInfo from "../../../deno.json" with { type: "json" };

const VERSION = packageInfo.version;

const commands = [
  {
    name: "help",
    description: "Show help information",
    handler: () => {
      showHelp();
    }
  },
  {
    name: "init",
    description: "Initialize migration configuration",
    handler: initCommand,
  },
  {
    name: "generate",
    description: "Generate a new migration file",
    handler: generateCommand,
  },
  {
    name: "apply",
    description: "Apply pending migrations",
    handler: applyCommand,
  },
  {
    name: "status",
    description: "Show migration status",
    handler: statusCommand,
  },
  {
    name: "rollback",
    description: "Rollback the last applied migration",
    handler: rollbackCommand,
  },
  {
    name: "history",
    description: "Show migration operation history",
    handler: historyCommand,
  }
]

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
${bold(blue("🐝 MongoDBee"))} v${VERSION}

${yellow("USAGE:")}
  mongodbee [COMMAND] [OPTIONS]

${yellow("COMMANDS:")}
  ${green("init")}      Initialize migration configuration
  ${green("generate")}  Generate a new migration file
  ${green("apply")}     Apply pending migrations
  ${green("status")}    Show migration status
  ${green("history")}   Show migration operation history
  ${green("rollback")}  Rollback the last applied migration

${yellow("GLOBAL OPTIONS:")}
  -h, --help     Show this help message
  -v, --version  Show version information
  --config       Path to configuration file (default: mongodbee.config.json)
  --env          Environment to use (default: development)
`);
}

/**
 * Display version information
 */
function showVersion(): void {
  console.log(`MongoDBee v${VERSION}`);
}

/**
 * Main CLI entry point
 */
async function main(): Promise<void> {
  const args = parseArgs(Deno.args, {
    boolean: ["version"],
  });

  if (args.version) {
    showVersion();
    return;
  }

  const command = args._[0] || "help";

  const cmd = commands.find(c => c.name === command);
  if(!cmd) {
    console.error(`${red("Error:")} Unknown command "${command}"`);
    Deno.exit(1);
  }

  await cmd.handler(args as any);
}

// Run main function if this is the main module
if (import.meta.main) {
  await main();
}

export { main };