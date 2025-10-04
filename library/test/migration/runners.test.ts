/**
 * @fileoverview Tests for migration execution runners
 *
 * Tests the high-level runner system that coordinates:
 * - Migration execution with validation
 * - Retry logic for failed operations
 * - Rollback functionality
 * - Progress tracking and logging
 * - Batch migration execution
 */

import { assert, assertEquals, assertExists } from "@std/assert";
import {
  createConsoleLogger,
  createMigrationRunner,
  createNoOpLogger,
  DEFAULT_RUNNER_CONFIG,
  type MigrationExecutionContext,
  type MigrationLogger,
  type MigrationProgress,
  type MigrationValidator,
  type ValidationResult,
} from "../../src/migration/runners/execution.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import type { MigrationSystemConfig } from "../../src/migration/config/types.ts";
import type {
  MigrationApplier,
  MigrationDefinition,
  MigrationRule,
  MigrationState,
} from "../../src/migration/types.ts";
import * as v from "valibot";

// ============================================================
// Mock Applier for Testing
// ============================================================

function createMockApplier(): MigrationApplier & {
  appliedOperations: MigrationRule[];
  reversedOperations: MigrationRule[];
  shouldFail?: boolean;
} {
  const appliedOperations: MigrationRule[] = [];
  const reversedOperations: MigrationRule[] = [];

  return {
    appliedOperations,
    reversedOperations,
    shouldFail: false,

    async applyOperation(operation: MigrationRule): Promise<void> {
      if (this.shouldFail) {
        throw new Error(`Mock failure for operation ${operation.type}`);
      }
      appliedOperations.push(operation);
      // Simulate some async work
      await new Promise((resolve) => setTimeout(resolve, 10));
    },

    async applyReverseOperation(operation: MigrationRule): Promise<void> {
      if (this.shouldFail) {
        throw new Error(`Mock failure for reverse operation ${operation.type}`);
      }
      reversedOperations.push(operation);
      await new Promise((resolve) => setTimeout(resolve, 10));
    },
  };
}

// ============================================================
// Mock Validator for Testing
// ============================================================

function createMockValidator(options: {
  shouldFail?: boolean;
  errors?: string[];
  warnings?: string[];
} = {}): MigrationValidator {
  return {
    // deno-lint-ignore require-await
    async validateMigration(
      _definition: MigrationDefinition,
    ): Promise<ValidationResult> {
      return {
        success: !options.shouldFail,
        errors: options.errors ||
          (options.shouldFail ? ["Mock validation error"] : []),
        warnings: options.warnings || [],
        data: { validated: true },
      };
    },

    // deno-lint-ignore require-await
    async validateOperation(
      _operation: MigrationRule,
      _context: MigrationExecutionContext,
    ): Promise<ValidationResult> {
      return {
        success: !options.shouldFail,
        errors: options.shouldFail ? ["Mock operation validation error"] : [],
        warnings: [],
      };
    },

    async validateState(
      _state: MigrationState,
      _context: MigrationExecutionContext,
    ): Promise<ValidationResult> {
      return {
        success: !options.shouldFail,
        errors: options.shouldFail ? ["Mock state validation error"] : [],
        warnings: [],
      };
    },
  };
}

// ============================================================
// Mock Logger for Testing
// ============================================================

function createMockLogger(): MigrationLogger & {
  logs: Array<{ level: string; message: string; data?: any }>;
} {
  const logs: Array<{ level: string; message: string; data?: any }> = [];

  return {
    logs,
    debug: (message: string, data?: Record<string, unknown>) => {
      logs.push({ level: "debug", message, data });
    },
    info: (message: string, data?: Record<string, unknown>) => {
      logs.push({ level: "info", message, data });
    },
    warn: (message: string, data?: Record<string, unknown>) => {
      logs.push({ level: "warn", message, data });
    },
    error: (message: string, error?: Error, data?: Record<string, unknown>) => {
      logs.push({
        level: "error",
        message,
        data: { ...data, error: error?.message },
      });
    },
  };
}

// ============================================================
// Logger Tests
// ============================================================

Deno.test("createConsoleLogger - creates logger with correct level", () => {
  const logger = createConsoleLogger("info");
  assertExists(logger);
  assertExists(logger.info);
  assertExists(logger.warn);
  assertExists(logger.error);
  assertExists(logger.debug);
});

Deno.test("createNoOpLogger - creates logger that does nothing", () => {
  const logger = createNoOpLogger();

  // Should not throw
  logger.debug("test");
  logger.info("test");
  logger.warn("test");
  logger.error("test");
});

// ============================================================
// Basic Runner Creation Tests
// ============================================================

Deno.test("createMigrationRunner - creates runner with default config", () => {
  const applier = createMockApplier();
  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
  };

  const runner = createMigrationRunner(context);

  assertExists(runner);
  assertExists(runner.executeMigration);
  assertExists(runner.executeMigrations);
  assertExists(runner.rollbackMigration);
  assertExists(runner.validateMigration);
  assertEquals(runner.context, context);
});

Deno.test("createMigrationRunner - merges custom config with defaults", () => {
  const applier = createMockApplier();
  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    maxRetries: 5,
  });

  assertEquals(runner.config.validateBeforeExecution, false);
  assertEquals(runner.config.maxRetries, 5);
  // Should keep defaults for other options
  assertEquals(
    runner.config.continueOnWarnings,
    DEFAULT_RUNNER_CONFIG.continueOnWarnings,
  );
});

// ============================================================
// Migration Execution Tests
// ============================================================

Deno.test("executeMigration - executes simple migration successfully", async () => {
  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: {
            name: v.string(),
          },
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const logger = createMockLogger();

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    logger,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    validateAfterExecution: false,
  });

  const result = await runner.executeMigration(migration);

  assertEquals(result.success, true);
  assertEquals(result.appliedOperations, 1);
  assertEquals(applier.appliedOperations.length, 1);
  assertEquals(applier.appliedOperations[0].type, "create_collection");

  // Check logging
  assert(logger.logs.some((log) => log.message.includes("Starting migration")));
  assert(logger.logs.some((log) => log.message.includes("completed")));
});

Deno.test("executeMigration - executes migration with multiple operations", async () => {
  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create and seed users",
    {
      parent: null,
      schemas: {
        collections: {
          users: {
            name: v.string(),
            email: v.string(),
          },
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration
          .createCollection("users")
          .seed([
            { name: "Alice", email: "alice@example.com" },
            { name: "Bob", email: "bob@example.com" },
          ]);
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    validateAfterExecution: false,
  });

  const result = await runner.executeMigration(migration);

  assertEquals(result.success, true);
  assertEquals(result.appliedOperations, 2);
  assertEquals(applier.appliedOperations.length, 2);
  assertEquals(applier.appliedOperations[0].type, "create_collection");
  assertEquals(applier.appliedOperations[1].type, "seed_collection");
});

Deno.test("executeMigration - validates before execution when enabled", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  //   const migration = migrationDefinition({
  //     id: "2024_01_01_1200_test@users",
  //     name: "Create users",
  //     parent: null,
  //     schemas,
  //   }, {}, (builder: any) => {
  //     return builder
  //       .createCollection("users", schemas.users)
  //       .compile();
  //   });
  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const validator = createMockValidator({ warnings: ["Test warning"] });
  const logger = createMockLogger();

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    validator,
    logger,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: true,
    validateAfterExecution: false,
  });

  const result = await runner.executeMigration(migration);

  assertEquals(result.success, true);
  assertEquals(result.warnings.length, 1);
  assert(result.warnings[0].includes("Test warning"));
  assert(
    logger.logs.some((log) =>
      log.message.includes("Validating migration before execution")
    ),
  );
});

Deno.test("executeMigration - stops on validation error when continueOnErrors is false", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  //   const migration = migrationDefinition({
  //     id: "2024_01_01_1200_test@users",
  //     name: "Create users",
  //     parent: null,
  //     schemas,
  //   }, {}, (builder: any) => {
  //     return builder
  //       .createCollection("users", schemas.users)
  //       .compile();
  //   });

  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const validator = createMockValidator({
    shouldFail: true,
    errors: ["Validation failed"],
  });

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    validator,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: true,
    continueOnErrors: false,
  });

  const result = await runner.executeMigration(migration);

  assertEquals(result.success, false);
  assertEquals(result.appliedOperations, 0);
  assertEquals(applier.appliedOperations.length, 0);
  assert(result.errors.some((e) => e.includes("Validation failed")));
});

Deno.test("executeMigration - tracks progress with callback", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  // const migration = migrationDefinition({
  //   id: "2024_01_01_1200_test@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .collection("users")
  //       .seed([{ name: "Alice" }])
  //     .compile();
  // });
  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users")
          .seed([{ name: "Alice" }]);
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const progressUpdates: MigrationProgress[] = [];

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    onProgress: (progress) => {
      progressUpdates.push(progress);
    },
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    validateAfterExecution: false,
  });

  await runner.executeMigration(migration);

  // Should have progress updates for: validation, execution phases
  assert(
    progressUpdates.length > 0,
    `Expected progress updates, got ${progressUpdates.length}`,
  );
  assert(
    progressUpdates.some((p) => p.phase === "execution"),
    `Expected 'execution' phase, got: ${
      progressUpdates.map((p) => p.phase).join(", ")
    }`,
  );
  assert(
    progressUpdates.some((p) => p.phase === "completed"),
    `Expected 'completed' phase, got: ${
      progressUpdates.map((p) => p.phase).join(", ")
    }`,
  );
});

Deno.test("executeMigration - calls operation callbacks", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  // const migration = migrationDefinition({
  //   id: "2024_01_01_1200_test@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .compile();
  // });
  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const operationCallbacks: Array<{ phase: string; operation: MigrationRule }> =
    [];

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    onOperation: (operation, phase) => {
      operationCallbacks.push({ phase, operation });
    },
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    validateAfterExecution: false,
  });

  await runner.executeMigration(migration);

  // Should have before and after callbacks
  assert(operationCallbacks.some((cb) => cb.phase === "before"));
  assert(operationCallbacks.some((cb) => cb.phase === "after"));
  assertEquals(operationCallbacks[0].operation.type, "create_collection");
});

Deno.test("executeMigration - handles operation failure with retry", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  // const migration = migrationDefinition({
  //   id: "2024_01_01_1200_test@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .compile();
  // });
  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  applier.shouldFail = true;

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    maxRetries: 2,
    retryDelay: 10,
  });

  const result = await runner.executeMigration(migration);

  assertEquals(result.success, false);
  // Should have attempted 1 + 2 retries = 3 times
  assertEquals(applier.appliedOperations.length, 0); // All failed
  assert(
    result.errors.some((e) =>
      e.includes("failed after") && e.includes("attempts")
    ),
  );
});

Deno.test("executeMigration - respects operation timeout", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  // const migration = migrationDefinition({
  //   id: "2024_01_01_1200_test@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .compile();
  // });

  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  // Create an applier that takes too long
  let slowTimerId: number | undefined;
  const slowApplier: MigrationApplier = {
    async applyOperation(_operation: MigrationRule): Promise<void> {
      await new Promise((resolve) => {
        slowTimerId = setTimeout(resolve, 1000); // 1 second
      });
    },
    async applyReverseOperation(_operation: MigrationRule): Promise<void> {
      await new Promise((resolve) => {
        slowTimerId = setTimeout(resolve, 1000);
      });
    },
  };

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier: slowApplier,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    operationTimeout: 50, // 50ms timeout
    maxRetries: 0,
  });

  const result = await runner.executeMigration(migration);

  // Clean up the timer to prevent leaks
  if (slowTimerId !== undefined) {
    clearTimeout(slowTimerId);
  }

  assertEquals(result.success, false);
  assert(result.errors.some((e) => e.includes("timeout")));
});

Deno.test("executeMigration - runs in dry-run mode", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const logger = createMockLogger();

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    logger,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    dryRun: true,
  });

  const result = await runner.executeMigration(migration);

  assertEquals(result.success, true);
  // In dry-run mode, operations are not actually applied
  assertEquals(applier.appliedOperations.length, 0);
  assertEquals(result.metadata?.dryRun, true);
  assert(logger.logs.some((log) => log.message.includes("Dry-run mode")));
});

// ============================================================
// Batch Execution Tests
// ============================================================

Deno.test("executeMigrations - executes multiple migrations in sequence", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
    posts: {
      title: v.string(),
    },
  };

  // const m1 = migrationDefinition({
  //   id: "2024_01_01_1200_m1@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .compile();
  // });

  // const m2 = migrationDefinition({
  //   id: "2024_01_01_1300_m2@posts",
  //   name: "Create posts",
  //   parent: m1,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("posts", schemas.posts)
  //     .compile();
  // });

  const m1 = migrationDefinition("2024_01_01_1200_m1@users", "Create users", {
    parent: null,
    schemas: {
      collections: {
        users: schemas.users,
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("users");
      return migration.compile();
    },
  });

  const m2 = migrationDefinition("2024_01_01_1300_m2@posts", "Create posts", {
    parent: m1,
    schemas: {
      collections: {
        posts: schemas.posts,
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("posts");
      return migration.compile();
    },
  });

  const applier = createMockApplier();
  const logger = createMockLogger();

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    logger,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    validateAfterExecution: false,
  });

  const results = await runner.executeMigrations([m1, m2]);

  assertEquals(results.length, 2);
  assertEquals(results[0].success, true);
  assertEquals(results[1].success, true);
  assertEquals(applier.appliedOperations.length, 2);

  assert(logger.logs.some((log) => log.message.includes("batch migration")));
});

Deno.test("executeMigrations - stops on first failure when continueOnErrors is false", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  // const m1 = migrationDefinition({
  //   id: "2024_01_01_1200_m1@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .compile();
  // });

  // const m2 = migrationDefinition({
  //   id: "2024_01_01_1300_m2@posts",
  //   name: "Create posts",
  //   parent: m1,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("posts", schemas.users)
  //     .compile();
  // });

  const m1 = migrationDefinition("2024_01_01_1200_m1@users", "Create users", {
    parent: null,
    schemas: {
      collections: {
        users: schemas.users,
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("users");
      return migration.compile();
    },
  });

  const m2 = migrationDefinition("2024_01_01_1300_m2@posts", "Create posts", {
    parent: m1,
    schemas: {
      collections: {
        users: schemas.users, // Intentional error: using users schema for posts
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("posts");
      return migration.compile();
    },
  });

  const applier = createMockApplier();

  // Make the applier fail after first operation
  let callCount = 0;
  const originalApply = applier.applyOperation.bind(applier);
  applier.applyOperation = (op: MigrationRule) => {
    callCount++;
    if (callCount > 1) {
      throw new Error("Simulated failure");
    }
    return originalApply(op);
  };

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    continueOnErrors: false,
    maxRetries: 0,
  });

  const results = await runner.executeMigrations([m1, m2]);

  // First migration should succeed, second should fail
  assertEquals(results.length, 2);
  assertEquals(results[0].success, true);
  assertEquals(results[1].success, false);
});

// ============================================================
// Rollback Tests
// ============================================================

Deno.test("rollbackMigration - rolls back a migration", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  // const migration = migrationDefinition({
  //   id: "2024_01_01_1200_test@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .collection("users")
  //       .seed([{ name: "Alice" }])
  //     .compile();
  // });

  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users")
          .seed([{ name: "Alice" }]);
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const logger = createMockLogger();

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    logger,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    validateAfterExecution: false,
  });

  const result = await runner.rollbackMigration(migration);

  assertEquals(result.success, true);
  // Rollback should apply reverse operations
  assertEquals(applier.reversedOperations.length, 2);
  // Operations should be reversed in reverse order
  assert(logger.logs.some((log) => log.message.includes("rollback")));
});

// ============================================================
// Validation Tests
// ============================================================

Deno.test("validateMigration - validates migration without executing", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  // const migration = migrationDefinition({
  //   id: "2024_01_01_1200_test@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .compile();
  // });

  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const validator = createMockValidator({ warnings: ["Test warning"] });

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    validator,
  };

  const runner = createMigrationRunner(context);

  const result = await runner.validateMigration(migration);

  assertEquals(result.success, true);
  assertEquals(result.warnings.length, 1);
  // Applier should not be called during validation
  assertEquals(applier.appliedOperations.length, 0);
});

Deno.test("validateMigration - returns warning when no validator configured", async () => {
  const schemas = {
    users: {
      name: v.string(),
    },
  };

  // const migration = migrationDefinition({
  //   id: "2024_01_01_1200_test@users",
  //   name: "Create users",
  //   parent: null,
  //   schemas,
  // }, {}, (builder: any) => {
  //   return builder
  //     .createCollection("users", schemas.users)
  //     .compile();
  // });

  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users");
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();

  const context: MigrationExecutionContext = {
    config: {} as any,
    applier,
    // No validator
  };

  const runner = createMigrationRunner(context);

  const result = await runner.validateMigration(migration);

  assertEquals(result.success, true);
  assert(result.warnings.some((w) => w.includes("No validator configured")));
});

// ============================================================
// Integration Tests with SimulationApplier
// ============================================================

// Note: SimulationApplier has a different signature (stateful) than MigrationApplier
// So we test with the mock applier instead
Deno.test("executeMigration - executes successfully with proper tracking", async () => {
  const schemas = {
    users: {
      name: v.string(),
      email: v.string(),
    },
  };

  const migration = migrationDefinition(
    "2024_01_01_1200_test@users",
    "Create and seed users",
    {
      parent: null,
      schemas: {
        collections: {
          users: schemas.users,
        },
        multiCollections: {},
      },
      migrate(migration) {
        migration.createCollection("users")
          .seed([
            { name: "Alice", email: "alice@example.com" },
          ]);
        return migration.compile();
      },
    },
  );

  const applier = createMockApplier();
  const logger = createMockLogger();

  const context: MigrationExecutionContext = {
    config: {} as MigrationSystemConfig,
    applier,
    logger,
  };

  const runner = createMigrationRunner(context, {
    validateBeforeExecution: false,
    validateAfterExecution: false,
  });

  const result = await runner.executeMigration(migration);

  assertEquals(result.success, true);
  assertEquals(result.appliedOperations, 2);
  assertExists(result.executionTime);

  // Verify operations were applied
  assertEquals(applier.appliedOperations.length, 2);
  assertEquals(applier.appliedOperations[0].type, "create_collection");
  assertEquals(applier.appliedOperations[1].type, "seed_collection");
});
