/**
 * Tests for the check CLI command
 *
 * @module
 */

import { test } from "../../+harness.ts";
import process from "node:process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { assertEquals } from "../../+assert.ts";
import { checkCommand } from "../../../src/migration/cli/commands/check.ts";
import * as path from "node:path";
import { installLibrary } from "./shared.ts";

test("check - validates all migrations successfully", async () => {
  const testDir = await mkdtemp(path.join(tmpdir(), "mongodbee_"));
  await installLibrary(testDir);

  try {
    // Create test directory structure
    await mkdir(path.join(testDir, "migrations"), { recursive: true });

    // Create config
    await writeFile(
      path.join(testDir, "mongodbee.config.json"),
      JSON.stringify({
        paths: {
          migrations: "./migrations",
          schemas: "./schemas.ts",
        },
        database: {
          name: "test_check",
          connection: {
            uri: process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017",
          },
        },
      }),
    );

    // Create schemas
    await writeFile(
      path.join(testDir, "schemas.ts"),
      `import * as v from "valibot";

export const schemas = {
  collections: {
    users: {
      _id: v.string(),
      name: v.string(),
      email: v.pipe(v.string(), v.email()),
    },
  },
  multiModels: {},
};
`,
    );

    // Create valid migration
    await writeFile(
      path.join(testDir, "migrations", "2025_01_01_000000_create_users.ts"),
      `import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";

export default migrationDefinition("2025_01_01_000000", "create_users", {
  parent: null,
  schemas: {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
        email: v.pipe(v.string(), v.email()),
      },
    },
    multiModels: {},
  },
  migrate(migration) {
    migration.createCollection("users");
    return migration.compile();
  },
});
`,
    );

    // Run check command
    await checkCommand({ cwd: testDir });
  } finally {
    await rm(testDir, { recursive: true, force: true });
  }
});

test("check - detects invalid migration", async () => {
  const testDir = await mkdtemp(path.join(tmpdir(), "mongodbee_"));
  await installLibrary(testDir);

  try {
    // Create test directory structure
    await mkdir(path.join(testDir, "migrations"), { recursive: true });

    // Create config
    await writeFile(
      path.join(testDir, "mongodbee.config.json"),
      JSON.stringify({
        paths: {
          migrations: "./migrations",
          schemas: "./schemas.ts",
        },
        database: {
          name: "test_check",
          connection: {
            uri: process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017",
          },
        },
      }),
    );

    // Create schemas
    await writeFile(
      path.join(testDir, "schemas.ts"),
      `import * as v from "valibot";

export const schemas = {
  collections: {
    users: {
      _id: v.string(),
      name: v.string(),
      email: v.pipe(v.string(), v.email()),
      age: v.number(),
    },
  },
  multiModels: {},
};
`,
    );

    // Create root migration
    await writeFile(
      path.join(testDir, "migrations", "2025_01_01_000000_create_users.ts"),
      `import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";

export default migrationDefinition("2025_01_01_000000", "create_users", {
  parent: null,
  schemas: {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
        email: v.pipe(v.string(), v.email()),
      },
    },
    multiModels: {},
  },
  migrate(migration) {
    migration.createCollection("users");
    migration.collection("users").seed([
      { _id: "1", name: "Alice", email: "alice@example.com" }
    ]);
    return migration.compile();
  },
});
`,
    );

    // Create invalid migration (schema change without transformation)
    await writeFile(
      path.join(testDir, "migrations", "2025_01_02_000000_add_age.ts"),
      `import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";
import rootMigration from "./2025_01_01_000000_create_users.ts";

export default migrationDefinition("2025_01_02_000000", "add_age", {
  parent: rootMigration,
  schemas: {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
        email: v.pipe(v.string(), v.email()),
        age: v.number(), // NEW REQUIRED FIELD without transformation
      },
    },
    multiModels: {},
  },
  migrate(migration) {
    // Missing transformation!
    return migration.compile();
  },
});
`,
    );

    // Run check command - should throw
    let errorThrown = false;
    try {
      await checkCommand({ cwd: testDir });
    } catch (_error) {
      errorThrown = true;
    }

    assertEquals(errorThrown, true, "Check should fail for invalid migration");
  } finally {
    await rm(testDir, { recursive: true, force: true });
  }
});

test("check - detects schema mismatch", async () => {
  const testDir = await mkdtemp(path.join(tmpdir(), "mongodbee_"));
  await installLibrary(testDir);

  try {
    // Create test directory structure
    await mkdir(path.join(testDir, "migrations"), { recursive: true });

    // Create config
    await writeFile(
      path.join(testDir, "mongodbee.config.json"),
      JSON.stringify({
        paths: {
          migrations: "./migrations",
          schemas: "./schemas.ts",
        },
        database: {
          name: "test_check",
          connection: {
            uri: process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017",
          },
        },
      }),
    );

    // Create schemas with DIFFERENT schema than last migration
    await writeFile(
      path.join(testDir, "schemas.ts"),
      `import * as v from "valibot";

export const schemas = {
  collections: {
    users: {
      _id: v.string(),
      name: v.string(),
      email: v.pipe(v.string(), v.email()),
      extraField: v.string(), // This field is NOT in the migration
    },
  },
  multiModels: {},
};
`,
    );

    // Create migration
    await writeFile(
      path.join(testDir, "migrations", "2025_01_01_000000_create_users.ts"),
      `import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";

export default migrationDefinition("2025_01_01_000000", "create_users", {
  parent: null,
  schemas: {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
        email: v.pipe(v.string(), v.email()),
      },
    },
    multiModels: {},
  },
  migrate(migration) {
    migration.createCollection("users");
    return migration.compile();
  },
});
`,
    );

    // Run check command - should throw due to schema mismatch
    let errorThrown = false;
    try {
      await checkCommand({ cwd: testDir });
    } catch (_error) {
      errorThrown = true;
    }

    assertEquals(
      errorThrown,
      true,
      "Check should fail when schemas don't match",
    );
  } finally {
    await rm(testDir, { recursive: true, force: true });
  }
});

test("check - handles empty migrations directory", async () => {
  const testDir = await mkdtemp(path.join(tmpdir(), "mongodbee_"));
  await installLibrary(testDir);

  try {
    // Create test directory structure
    await mkdir(path.join(testDir, "migrations"), { recursive: true });

    // Create config
    await writeFile(
      path.join(testDir, "mongodbee.config.json"),
      JSON.stringify({
        paths: {
          migrations: "./migrations",
          schemas: "./schemas.ts",
        },
        database: {
          name: "test_check",
          connection: {
            uri: process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017",
          },
        },
      }),
    );

    // Create schemas
    await writeFile(
      path.join(testDir, "schemas.ts"),
      `import * as v from "valibot";

export const schemas = {
  collections: {},
  multiModels: {},
};
`,
    );

    // Run check command - should succeed with warning
    await checkCommand({ cwd: testDir });
  } finally {
    await rm(testDir, { recursive: true, force: true });
  }
});
