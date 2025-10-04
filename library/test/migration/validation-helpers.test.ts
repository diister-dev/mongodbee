/**
 * Tests for application startup validation helpers
 */

import { assertEquals, assertExists, assertRejects } from "@std/assert";
import {
  assertMigrationsApplied,
  checkMigrationStatus,
  isLastMigrationApplied,
  validateDatabaseState,
  validateMigrationsForEnv,
} from "../../src/migration/validation-helpers.ts";
import { withDatabase } from "../+shared.ts";
import { recordOperation } from "../../src/migration/history.ts";

Deno.test("checkMigrationStatus - no migrations", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create a temporary empty migrations directory
    const tempDir = await Deno.makeTempDir();

    try {
      const status = await checkMigrationStatus(db, tempDir);

      assertEquals(status.isUpToDate, true);
      assertEquals(status.totalMigrations, 0);
      assertEquals(status.appliedCount, 0);
      assertEquals(status.pendingMigrations.length, 0);
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("checkMigrationStatus - all applied", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Record some applied migrations
    await recordOperation(db, "mig_001", "initial", "applied", 100);
    await recordOperation(db, "mig_002", "add_users", "applied", 150);

    // Create a temporary migrations directory with matching files
    const tempDir = await Deno.makeTempDir();

    try {
      // Create migration files that match the recorded migrations
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
export default {
  id: "mig_001",
  name: "initial",
  parent: null,
  migrate: async (builder) => builder.compile()
};
        `,
      );

      await Deno.writeTextFile(
        `${tempDir}/002_add_users.ts`,
        `
export default {
  id: "mig_002",
  name: "add_users",
  parent: { id: "mig_001", name: "initial" },
  migrate: async (builder) => builder.compile()
};
        `,
      );

      const status = await checkMigrationStatus(db, tempDir);

      assertEquals(status.isUpToDate, true);
      assertEquals(status.totalMigrations, 2);
      assertEquals(status.appliedCount, 2);
      assertEquals(status.pendingMigrations.length, 0);
      assertEquals(status.lastAppliedMigration, "mig_002");
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("checkMigrationStatus - pending migrations", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Record only the first migration
    await recordOperation(db, "mig_001", "initial", "applied", 100);

    const tempDir = await Deno.makeTempDir();

    try {
      // Create two migration files (one not applied)
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
export default {
  id: "mig_001",
  name: "initial",
  parent: null,
  migrate: async (builder) => builder.compile()
};
        `,
      );

      await Deno.writeTextFile(
        `${tempDir}/002_add_users.ts`,
        `
export default {
  id: "mig_002",
  name: "add_users",
  parent: { id: "mig_001", name: "initial" },
  migrate: async (builder) => builder.compile()
};
        `,
      );

      const status = await checkMigrationStatus(db, tempDir);

      assertEquals(status.isUpToDate, false);
      assertEquals(status.totalMigrations, 2);
      assertEquals(status.appliedCount, 1);
      assertEquals(status.pendingMigrations.length, 1);
      assertEquals(status.pendingMigrations[0], "mig_002");
      assertEquals(status.lastAppliedMigration, "mig_001");
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("isLastMigrationApplied - true when up-to-date", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Record migrations
    await recordOperation(db, "mig_001", "initial", "applied", 100);
    await recordOperation(db, "mig_002", "add_users", "applied", 150);

    const tempDir = await Deno.makeTempDir();

    try {
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
export default {
  id: "mig_001",
  name: "initial",
  parent: null,
  migrate: async (builder) => builder.compile()
};
        `,
      );

      await Deno.writeTextFile(
        `${tempDir}/002_add_users.ts`,
        `
export default {
  id: "mig_002",
  name: "add_users",
  parent: { id: "mig_001", name: "initial" },
  migrate: async (builder) => builder.compile()
};
        `,
      );

      const result = await isLastMigrationApplied(db, tempDir);

      assertEquals(result, true);
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("isLastMigrationApplied - false when pending", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Record only first migration
    await recordOperation(db, "mig_001", "initial", "applied", 100);

    const tempDir = await Deno.makeTempDir();

    try {
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
export default {
  id: "mig_001",
  name: "initial",
  parent: null,
  migrate: async (builder) => builder.compile()
};
        `,
      );

      await Deno.writeTextFile(
        `${tempDir}/002_add_users.ts`,
        `
export default {
  id: "mig_002",
  name: "add_users",
  parent: { id: "mig_001", name: "initial" },
  migrate: async (builder) => builder.compile()
};
        `,
      );

      const result = await isLastMigrationApplied(db, tempDir);

      assertEquals(result, false);
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("assertMigrationsApplied - succeeds when up-to-date", async (t) => {
  await withDatabase(t.name, async (db) => {
    const tempDir = await Deno.makeTempDir();

    try {
      // No migrations = considered up-to-date
      await assertMigrationsApplied(db, tempDir);
      // Should not throw
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("assertMigrationsApplied - throws when pending", async (t) => {
  await withDatabase(t.name, async (db) => {
    const tempDir = await Deno.makeTempDir();

    try {
      // Create a migration file but don't apply it
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
export default {
  id: "mig_001",
  name: "initial",
  parent: null,
  migrate: async (builder) => builder.compile()
};
        `,
      );

      await assertRejects(
        () => assertMigrationsApplied(db, tempDir),
        Error,
        "pending migration",
      );
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("validateMigrationsForEnv - development warns", async (t) => {
  await withDatabase(t.name, async (db) => {
    const tempDir = await Deno.makeTempDir();

    try {
      // Create a migration file but don't apply it
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
export default {
  id: "mig_001",
  name: "initial",
  parent: null,
  migrate: async (builder) => builder.compile()
};
        `,
      );

      // Should not throw in development (just warns to console)
      await validateMigrationsForEnv(db, "development", tempDir);

      // Test passes if no error thrown
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("validateMigrationsForEnv - production throws", async (t) => {
  await withDatabase(t.name, async (db) => {
    const tempDir = await Deno.makeTempDir();

    try {
      // Create a migration file but don't apply it
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
export default {
  id: "mig_001",
  name: "initial",
  parent: null,
  migrate: async (builder) => builder.compile()
};
        `,
      );

      await assertRejects(
        () => validateMigrationsForEnv(db, "production", tempDir),
        Error,
        "Production startup blocked",
      );
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("validateDatabaseState - all valid", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create a temporary migrations directory
    const tempDir = await Deno.makeTempDir();

    try {
      // Create a simple migration file
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
export default {
  id: "mig_001",
  name: "initial",
  parent: null,
  schemas: { collections: {}, multiCollections: {} },
  migrate: async (builder) => builder.compile()
};
        `,
      );

      // Record the migration as applied
      await recordOperation(db, "mig_001", "initial", "applied", 100);

      // Note: This test will skip schema validation since we don't have
      // a real mongodbee.config.ts and schemas/database.json
      // In a real scenario, the schema check would also run
      const result = await validateDatabaseState(db, {
        configPath: `${tempDir}/mongodbee.config.ts`,
        migrationsDir: tempDir,
      });

      // Migration check should pass
      assertEquals(result.migrations.isUpToDate, true);
      assertEquals(result.issues.length, 0);
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});

Deno.test("validateDatabaseState - pending migrations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const tempDir = await Deno.makeTempDir();

    try {
      // Create two migration files
      await Deno.writeTextFile(
        `${tempDir}/001_initial.ts`,
        `
const parent1 = null;
export default {
  id: "mig_001",
  name: "initial",
  parent: parent1,
  schemas: { collections: {}, multiCollections: {} },
  migrate: async (builder) => builder.compile()
};
        `,
      );

      await Deno.writeTextFile(
        `${tempDir}/002_add_users.ts`,
        `
import parent from "./001_initial.ts";
export default {
  id: "mig_002",
  name: "add_users",
  parent: parent,
  schemas: { collections: {}, multiCollections: {} },
  migrate: async (builder) => builder.compile()
};
        `,
      );

      // Only record the first migration as applied
      await recordOperation(db, "mig_001", "initial", "applied", 100);

      const result = await validateDatabaseState(db, {
        configPath: `${tempDir}/mongodbee.config.ts`,
        migrationsDir: tempDir,
        env: "development",
      });

      assertEquals(result.isValid, false);
      assertEquals(result.migrations.isUpToDate, false);
      assertEquals(result.migrations.pendingMigrations.length, 1);
      assertEquals(result.migrations.pendingMigrations[0], "mig_002");

      // Should have at least one issue about pending migration
      assertExists(
        result.issues.find((issue) => issue.includes("pending migration")),
      );
    } finally {
      await Deno.remove(tempDir, { recursive: true });
    }
  });
});
