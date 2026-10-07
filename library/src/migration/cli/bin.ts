#!/usr/bin/env node
/**
 * MongoDBee Migration CLI executable
 *
 * This is the main CLI entry point that can be installed and run as a binary.
 *
 * @example
 * ```bash
 * # npm / Bun (the `bin` entry ships with the package)
 * npx mongodbee migrate
 * bunx mongodbee migrate
 *
 * # Deno
 * deno run -A jsr:@diister/mongodbee/migration/cli/bin migrate
 * ```
 *
 * @module
 */

import process from "node:process";
import { isMainModule } from "../utils/platform.ts";
import { main } from "./main.ts";
import { armExitGuard } from "./utils/exit-guard.ts";

// Run the CLI if this is the main module
if (isMainModule(import.meta)) {
  await main();
  armExitGuard(process.argv.slice(2));
}
