/**
 * Tests for the check CLI command
 *
 * @module
 */

import { test, expect } from "vitest";
import * as fsp from "node:fs/promises";
import { checkCommand } from "../../../src/migration/cli/commands/check.ts";
import * as path from "node:path";
import { setupTempNodeModules } from "./shared.ts";

test("check - validates all migrations successfully", async () => {
  const testDir = await fsp.mkdtemp(path.join((await import("node:os")).tmpdir(), "mongodbee_test_"));

  try {
    await setupTempNodeModules(testDir);
    // Create test directory structure
    await fsp.mkdir(path.join(testDir, "migrations"), { recursive: true });

    // Create config
    await fsp.writeFile(
      path.join(testDir, "mongodbee.config.json"),
      JSON.stringify({
        paths: {
          migrations: "./migrations",
          schemas: "./schemas.ts",
        },
        database: {
          name: "test_check",
          connection: {
            uri: "mongodb://127.0.0.1:27017",
          },
        },
      }),
      "utf-8",
    );

    // Create schemas
    await fsp.writeFile(
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
      "utf-8",
    );

    // Create valid migration
    await fsp.writeFile(
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
      "utf-8",
    );

    // Run check command
    await checkCommand({ cwd: testDir });
  } finally {
    await fsp.rm(testDir, { recursive: true });
  }
});

test("check - detects invalid migration", async () => {
  const testDir = await fsp.mkdtemp(path.join((await import("node:os")).tmpdir(), "mongodbee_test_"));

  try {
    await setupTempNodeModules(testDir);
    // Create test directory structure
    await fsp.mkdir(path.join(testDir, "migrations"), { recursive: true });

    // Create config
    await fsp.writeFile(
      path.join(testDir, "mongodbee.config.json"),
      JSON.stringify({
        paths: {
          migrations: "./migrations",
          schemas: "./schemas.ts",
        },
        database: {
          name: "test_check",
          connection: {
            uri: "mongodb://127.0.0.1:27017",
          },
        },
      }),
      "utf-8",
    );

    // Create schemas
    await fsp.writeFile(
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
      "utf-8",
    );

    // Create root migration
    await fsp.writeFile(
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
      "utf-8",
    );

    // Create invalid migration (schema change without transformation)
    await fsp.writeFile(
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
      "utf-8",
    );

    // Run check command - should throw
    let errorThrown = false;
    try {
      await checkCommand({ cwd: testDir });
    } catch (_error) {
      errorThrown = true;
    }

    expect(errorThrown).toEqual(true);
  } finally {
    await fsp.rm(testDir, { recursive: true });
  }
});

test("check - detects schema mismatch", async () => {
  const testDir = await fsp.mkdtemp(path.join((await import("node:os")).tmpdir(), "mongodbee_test_"));

  try {
    await setupTempNodeModules(testDir);
    // Create test directory structure
    await fsp.mkdir(path.join(testDir, "migrations"), { recursive: true });

    // Create config
    await fsp.writeFile(
      path.join(testDir, "mongodbee.config.json"),
      JSON.stringify({
        paths: {
          migrations: "./migrations",
          schemas: "./schemas.ts",
        },
        database: {
          name: "test_check",
          connection: {
            uri: "mongodb://127.0.0.1:27017",
          },
        },
      }),
      "utf-8",
    );

    // Create schemas with DIFFERENT schema than last migration
    await fsp.writeFile(
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
      "utf-8",
    );

    // Create migration
    await fsp.writeFile(
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
      "utf-8",
    );

    // Run check command - should throw due to schema mismatch
    let errorThrown = false;
    try {
      await checkCommand({ cwd: testDir });
    } catch (_error) {
      errorThrown = true;
    }

    expect(
      errorThrown,
    ).toEqual(true);
  } finally {
    await fsp.rm(testDir, { recursive: true });
  }
});

test("check - handles empty migrations directory", async () => {
  const testDir = await fsp.mkdtemp(path.join((await import("node:os")).tmpdir(), "mongodbee_test_"));

  try {
    await setupTempNodeModules(testDir);
    // Create test directory structure
    await fsp.mkdir(path.join(testDir, "migrations"), { recursive: true });

    // Create config
    await fsp.writeFile(
      path.join(testDir, "mongodbee.config.json"),
      JSON.stringify({
        paths: {
          migrations: "./migrations",
          schemas: "./schemas.ts",
        },
        database: {
          name: "test_check",
          connection: {
            uri: "mongodb://127.0.0.1:27017",
          },
        },
      }),
      "utf-8",
    );

    // Create schemas
    await fsp.writeFile(
      path.join(testDir, "schemas.ts"),
      `import * as v from "valibot";

export const schemas = {
  collections: {},
  multiModels: {},
};
`,
      "utf-8",
    );

    // Run check command - should succeed with warning
    await checkCommand({ cwd: testDir });
  } finally {
    await fsp.rm(testDir, { recursive: true });
  }
});
