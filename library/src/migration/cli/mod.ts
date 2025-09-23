/**
 * CLI exports for MongoDBee Migration system
 * 
 * @module
 */

export { main } from "./main.ts";
export { initCommand } from "./commands/init.ts";
export { generateCommand } from "./commands/generate.ts";
export { applyCommand } from "./commands/apply.ts";
export { rollbackCommand } from "./commands/rollback.ts";
export { statusCommand } from "./commands/status.ts";

export type { InitCommandOptions } from "./commands/init.ts";
export type { GenerateCommandOptions } from "./commands/generate.ts";
export type { ApplyCommandOptions } from "./commands/apply.ts";
export type { RollbackCommandOptions } from "./commands/rollback.ts";
export type { StatusCommandOptions } from "./commands/status.ts";