#!/usr/bin/env -S deno run --allow-read --allow-write --allow-net --allow-env
/**
 * MongoDBee Migration CLI executable
 * 
 * This is the main CLI entry point that can be installed and run as a binary.
 * 
 * @example
 * ```bash
 * # Install globally
 * deno install --allow-read --allow-write --allow-net --allow-env --name mongodbee-migrate jsr:@diister/mongodbee/migration/cli/bin
 * 
 * # Use the CLI
 * mongodbee-migrate init
 * mongodbee-migrate generate --name create-users
 * mongodbee-migrate apply
 * ```
 * 
 * @module
 */

import { main } from "./main.ts";

// Run the CLI if this is the main module
if (import.meta.main) {
  await main();
}