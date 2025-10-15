/**
 * CLI exports for MongoDBee Migration system
 *
 * @module
 */

export { main } from "./main.ts";
export { initCommand } from "./commands/init.ts";
export { generateCommand } from "./commands/generate.ts";
export { migrateCommand } from "./commands/migrate.ts";
export { rollbackCommand } from "./commands/rollback.ts";
export { statusCommand } from "./commands/status.ts";
export { checkCommand } from "./commands/check.ts";

export type { InitCommandOptions } from "./commands/init.ts";
export type { GenerateCommandOptions } from "./commands/generate.ts";
export type { MigrateCommandOptions } from "./commands/migrate.ts";
export type { RollbackCommandOptions } from "./commands/rollback.ts";
export type { StatusCommandOptions } from "./commands/status.ts";
export type { CheckCommandOptions } from "./commands/check.ts";
