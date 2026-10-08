#!/usr/bin/env node
/**
 * MongoDBee Migration CLI
 *
 * CLI tool for managing MongoDB migrations with MongoDBee
 *
 * @module
 */

import process from "node:process";
import { parseArgs } from "../../utils/parse-args.ts";
import { blue, bold, green, red, yellow } from "../../utils/colors.ts";

import { generateCommand } from "./commands/generate.ts";
import { migrateCommand } from "./commands/migrate.ts";
import { rollbackCommand } from "./commands/rollback.ts";
import { statusCommand } from "./commands/status.ts";
import { historyCommand } from "./commands/history.ts";
import { initCommand } from "./commands/init.ts";
import { checkCommand } from "./commands/check.ts";
import { syncCommand } from "./commands/sync.ts";
import { baselineCommand } from "./commands/baseline.ts";
import { classifyCommand } from "./commands/classify.ts";
import { seedCommand } from "./commands/seed.ts";
import { extractCommand } from "./commands/extract.ts";
import { studioEntry } from "./commands/studio-entry.ts";

import { VERSION } from "../../version.ts";
import { isMainModule } from "../utils/platform.ts";
import { armExitGuard } from "./utils/exit-guard.ts";

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
    name: "baseline",
    description: "Record migrations as applied without running them",
    handler: baselineCommand,
  },
  {
    name: "history",
    description: "Show migration operation history",
    handler: historyCommand,
  },
  {
    name: "classify",
    description: "Report how the schemas classify personal data",
    handler: classifyCommand,
  },
  {
    name: "seed",
    description: "Generate a scenario world at a migration step and write it",
    handler: seedCommand,
  },
  {
    name: "extract",
    description: "Copy a database with personal data pseudonymised",
    handler: extractCommand,
  },
  {
    name: "studio",
    description: "Open a local, read-only database explorer",
    handler: studioEntry,
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
  ${green("baseline")}  Record migrations as applied without running them
  ${green("classify")}  Report how the schemas classify personal data
  ${green(
    "seed",
  )}      Generate a scenario world at a migration step and write it
  ${green("extract")}   Copy a database with personal data pseudonymised
  ${green("studio")}    Open a local, read-only database explorer

${yellow("GLOBAL OPTIONS:")}
  -h, --help        Show this help message
  -v, --version     Show version information
  --config          Path to configuration file (default: mongodbee.config.json)
  --env             Environment to use (default: development)

${yellow("CHECK OPTIONS:")}
  -m, --mode        Simulation mode: quick, normal, hard (default: normal)
  -l, --last        Only validate the last N migrations
  --docs            Mock documents per collection, 1 to 5000, overriding
                    the mode (quick 10, normal 100, hard 500)
  --retention       Share of each migration's documents carried into the
                    next one, 0 to 1 (default: 0.5)
  --verbose         Print every warning under the migration that raised it
                    (default: warnings are deduplicated into a single digest)
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
  --progress        Force the live progress line (auto-detected on a TTY; use --no-progress to disable)
  -m, --mode        Simulation mode: quick, normal, hard (default: normal)
  -l, --last        Also re-validate applied migrations: simulate at least
                    the last N (only the pending ones are simulated by default)
  --docs            Mock documents per collection, 1 to 5000 (as for check)
  --retention       Share of documents carried between migrations, 0 to 1
  --target          Stop after this migration (id, name, or unambiguous
                    substring); the later ones stay pending
  --skip-privilege-check
                    Do not verify the account's privileges before starting
                    (by default the run is refused when the account lacks an
                    action migrations need, e.g. collMod from dbAdmin)

${yellow("BASELINE OPTIONS:")}
  --target          Migration the database is already at, inclusive
                    (default: the last one in the chain)
  --force           Skip the confirmation

${yellow("ROLLBACK OPTIONS:")}
  --force           Skip all confirmations (use with caution!)
  --progress        Force the live progress line (auto-detected on a TTY; use --no-progress to disable)
  --skip-privilege-check
                    Do not verify the account's privileges before starting

${yellow("STUDIO OPTIONS:")}
  --port            Port to listen on (default: 4983)
  --host            Interface to bind (default: 127.0.0.1)
  --project         Project directory to open (config discovery and relative paths)
  --uri             MongoDB connection string (or set MONGODBEE_STUDIO_URI)
  --db              Database name
  --migrations      Migrations directory
  --schemas         Schemas file (schemas.ts)
  --write           Allow editing, creating and deleting documents (loopback only)

${yellow("CLASSIFY OPTIONS:")}
  --at              Classify the schemas frozen in this migration (default: the
                    last one, as extract does; schemas.ts when there is none)
  --posture         strict | personal (default: strict, as extract)
  --json            Print the plan as JSON

${yellow("SEED OPTIONS:")}
  --scenario        Scenario module exporting "scenario" (required)
  --at              Migration step to seed at (default: the last one)
  --uri, --db       Target database (default: the configured one); must be empty
                    unless --allow-non-empty
  --replay          memory | mongo (default: memory). memory replays the
                    migrations after the scenario's birth in memory and writes
                    the world at --at; mongo writes it at its birth, then runs
                    the real migrate up to --at on the database (irreversible
                    migrations included) and checks what it produced
  --allow-violations Write even when the oracle reports blocking violations
  --allow-non-empty Write next to other collections; refused when a collection
                    the schemas manage already exists or a migration ledger does
  --dry-run         Generate and check the world without writing (memory only)
  --json            Print the report as JSON
  A failed seed removes every collection it created, ledger included

${yellow("EXTRACT OPTIONS:")}
  --from, --from-db Source connection URI and database name (default: the configured ones)
  --to, --to-db     Target connection URI and database name; must be empty
                    unless --force, and never the source itself
  --from-migration  Migration the source is at; later ones are replayed in memory first
  --secret          Pseudonymisation secret: env:NAME is read from the environment
                    (recommended); a literal value is visible in ps and shell
                    history. Default: random, discarded
  --consistency     person | relationship | transaction (default: relationship)
  --posture         strict | personal (default: strict): strict fakes every value
                    the schemas do not declare, personal keeps what is not personal
  --shift-days      Shift every date and ulid timestamp by N days
  --scope           Only extract this scope of the scoped collections; unscoped
                    collections are still copied whole (listed in the summary)
  --allow-unknown   Proceed with UNKNOWN paths (they are dropped)
  --allow-violations Write even when the extracted documents break their schemas
                    (invalid_document, mirror_mismatch); default: refuse. Data
                    that breaks a unique index or repeats an _id is never
                    written, the index would reject it
  --force           Allow other collections in the target; a collection the
                    schemas manage is refused when it already holds documents
  --dry-run         Read and transform without writing
  --json            Print the summary as JSON
  Config hook       privacy: { resolveDynamic, recompute? } in mongodbee.config.ts
                    classifies dynamic subtrees from their data (resolveDynamic)
                    and recomputes derived values (recompute); both return the
                    SKIP_DYNAMIC / SKIP_RECOMPUTE symbols of
                    @diister/mongodbee/privacy to defer to the default

${yellow("SYNC OPTIONS:")}
  --force           Sync even if pending migrations exist (not recommended)
  --verbose         Show detailed schema information
  --skip-privilege-check
                    Do not verify the account's privileges before starting
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
    boolean: [
      "version",
      "dry-run",
      "force",
      "auto-sync",
      "verbose",
      "help",
      "check-indexes",
      "validate",
      "progress",
      "json",
      "allow-unknown",
      "allow-violations",
      "allow-non-empty",
      "skip-privilege-check",
      "write",
    ],
    // `progress` stays tri-state: `--progress` forces the live line on,
    // `--no-progress` forces it off, and omitting it leaves `undefined` so the
    // command falls back to TTY auto-detection.
    negatable: ["progress"],
    default: { progress: undefined },
    string: [
      "config",
      "env",
      "name",
      "mode",
      "target",
      "scenario",
      "replay",
      "at",
      "uri",
      "db",
      "from",
      "from-db",
      "to",
      "to-db",
      "secret",
      "consistency",
      "posture",
      "shift-days",
      "scope",
      "from-migration",
      "host",
      "project",
      "migrations",
      "schemas",
    ],
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

  if (args.help) {
    showHelp();
    return;
  }

  const command = args._[0] || "help";

  const cmd = commands.find((c) => c.name === command);
  if (!cmd) {
    throw new Error(`Unknown command "${command}"`);
  }

  try {
    // The flag is spelled `--config` but every command reads `configPath`.
    // Mapping it here is what makes it a GLOBAL option: each command used to
    // do the mapping itself, and only `migrate` actually did, so `--config`
    // was silently ignored by the six others and they ran against whichever
    // configuration file auto-discovery happened to find.
    const commandOptions = { ...args, configPath: args.config };
    await cmd.handler(commandOptions as any);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red(bold("Error:")), message);
    const cause = (error as any).cause;
    if (cause) {
      // Errors:
      for (const err of cause.errors ?? []) {
        console.error(red(` - ${err}`));
      }
    }
    // A failing subcommand MUST fail the process. This catch printed the error
    // and returned normally, so `main()` resolved, the outer handler never ran,
    // and EVERY subcommand exited 0 — `check` reported "Migration chain
    // validation failed" and returned success, `migrate` the same. No pipeline
    // step could gate on either. `exitCode` rather than `exit()`: the latter
    // would cut short an in-flight client teardown.
    process.exitCode = 1;
  }
}

// Run main function if this is the main module
if (isMainModule(import.meta)) {
  try {
    await main();
    armExitGuard(process.argv.slice(2));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red(bold("Error:")), message);
    process.exit(1);
  }
}

export { main };
