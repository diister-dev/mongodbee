#!/usr/bin/env node
/**
 * MongoDBee Migration CLI
 *
 * CLI tool for managing MongoDB migrations with MongoDBee
 *
 * @module
 */

import process from "node:process";
import { parseArgs } from "@std/cli/parse-args";
import { blue, bold, green, red, yellow } from "@std/fmt/colors";

import { generateCommand } from "./commands/generate.ts";
import { migrateCommand } from "./commands/migrate.ts";
import { rollbackCommand } from "./commands/rollback.ts";
import { statusCommand } from "./commands/status.ts";
import { historyCommand } from "./commands/history.ts";
import { initCommand } from "./commands/init.ts";
import { checkCommand } from "./commands/check.ts";
import { syncCommand } from "./commands/sync.ts";

import packageInfo from "../../../deno.json" with { type: "json" };

const VERSION = packageInfo.version;

const commands = [
  {
    name: "help",
    description: "Show help information",
    handler: () => {
      showHelp();
    },
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
    name: "check",
    description: "Check migrations validity without applying",
    handler: checkCommand,
  },
  {
    name: "migrate",
    description: "Apply pending migrations",
    handler: migrateCommand,
  },
  {
    name: "sync",
    description: "Synchronize schemas and indexes with latest migration",
    handler: syncCommand,
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
  },
];

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
  ${green("check")}     Check migrations validity without applying
  ${green("migrate")}   Apply pending migrations
  ${green("sync")}      Synchronize schemas and indexes with latest migration
  ${green("status")}    Show migration status
  ${green("history")}   Show migration operation history
  ${green("rollback")}  Rollback the last applied migration

${yellow("GLOBAL OPTIONS:")}
  -h, --help        Show this help message
  -v, --version     Show version information
  --config          Path to configuration file (default: mongodbee.config.json)
  --env             Environment to use (default: development)

${yellow("CHECK OPTIONS:")}
  -m, --mode        Simulation mode: quick, normal, hard (default: normal)
  -l, --last        Only validate the last N migrations
  --check-indexes   Check database indexes against schema (requires database connection)

${yellow("STATUS OPTIONS:")}
  --validate        Run schema and simulation validation checks
  -m, --mode        Simulation mode: quick, normal, hard (default: normal)
  -l, --last        Only validate the last N migrations

${yellow("MIGRATE OPTIONS:")}
  --dry-run         Simulate migration without applying changes
  --force           Skip all confirmations (use with caution!)
  --auto-sync       Automatically catch up orphaned multi-model instances
  --verbose         Show detailed migration information
  -m, --mode        Simulation mode: quick, normal, hard (default: normal)
  -l, --last        Only validate the last N migrations

${yellow("SYNC OPTIONS:")}
  --force           Sync even if pending migrations exist (not recommended)
  --verbose         Show detailed schema information
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
  const args = parseArgs(process.argv.slice(2), {
    boolean: ["version", "dry-run", "force", "auto-sync", "verbose", "help", "check-indexes", "validate"],
    string: ["config", "env", "name", "mode"],
    alias: {
      v: "version",
      h: "help",
      m: "mode",
      l: "last",
    },
  });

  if (args.version) {
    showVersion();
    return;
  }

  if (args.help && args._.length === 0) {
    showHelp();
    return;
  }

  const command = args._[0] || "help";

  const cmd = commands.find((c) => c.name === command);
  if (!cmd) {
    throw new Error(`Unknown command "${command}"`);
  }

  try {
    // deno-lint-ignore no-explicit-any
    await cmd.handler(args as any);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red(bold("Error:")), message);
    // deno-lint-ignore no-explicit-any
    const cause = (error as any).cause;
    if (cause) {
      // Errors:
      for (const err of cause.errors ?? []) {
        console.error(red(` - ${err}`));
      }
    }
  }
}

// Run main function if this is the main module
const isMain = import.meta.url === `file://${process.argv[1]?.replace(/\\/g, "/")}` ||
  (import.meta as any).main === true;

if (isMain) {
  try {
    await main();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red(bold("Error:")), message);
    process.exit(1);
  }
}

export { main };
