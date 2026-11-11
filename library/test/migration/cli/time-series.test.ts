/**
 * A time series collection through the commands a project runs: `check`
 * compares schemas.ts with the last migration (options included), `migrate`
 * creates the collection, `rollback` drops it.
 *
 * @module
 */

import { test } from "../../+harness.ts";
import { writeFile } from "node:fs/promises";
import { assert, assertEquals, assertStringIncludes } from "../../+assert.ts";
import { TEST_URI, timeSeriesUnsupported } from "../../+shared.ts";
import { MongoClient } from "../../../src/mongodb.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import { runCheck } from "../../../src/migration/cli/commands/check.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { rollbackCommand } from "../../../src/migration/cli/commands/rollback.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { inspectTimeSeries } from "../../../src/utils/ensure-time-series.ts";
import {
  getMigrationPath,
  getMigrationsDir,
  listMigrationFiles,
  readFile,
  withTempDir,
} from "./shared.ts";

const needs8 = await timeSeriesUnsupported();

function declaration(expireAfterSeconds: number): string {
  return `defineTimeSeries({
            schema: v.object({
              at: v.date(),
              meta: v.object({ exposition: v.string() }),
              scanId: v.string(),
            }),
            timeField: "at",
            metaField: "meta",
            granularity: "minutes",
            expireAfterSeconds: ${expireAfterSeconds},
            indexes: (f) => [index(f.scanId)],
          })`;
}

function schemasFile(pkg: string, expireAfterSeconds: number): string {
  return `import { defineTimeSeries, index } from "${pkg}";
import * as v from "${pkg}/schema";

export const schemas = {
  timeSeriesCollections: {
    scans: ${declaration(expireAfterSeconds)},
  },
};
`;
}

/** A project whose only migration creates `scans` with a one-hour retention. */
async function project(
  tempDir: string,
  dbName: string,
  schemasExpireAfterSeconds: number,
): Promise<void> {
  await initCommand({ cwd: tempDir });
  await writeFile(
    `${tempDir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: "${TEST_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await generateCommand({ name: "scans", cwd: tempDir });

  const migrationPath = getMigrationPath(
    tempDir,
    listMigrationFiles(getMigrationsDir(tempDir))[0],
  );
  const generated = await readFile(migrationPath);
  assert(generated !== null);
  const pkg = /from "(.+)\/migration"/.exec(generated)?.[1];
  assert(pkg !== undefined);
  await writeFile(
    migrationPath,
    generated
      .replace(
        /^(\s*import \{ migrationDefinition \}.*)$/m,
        `$1\nimport { defineTimeSeries, index } from "${pkg}";\nimport * as v from "${pkg}/schema";`,
      )
      .replace(
        "schemas: {",
        `schemas: {\n        timeSeriesCollections: { scans: ${declaration(3600)} },`,
      )
      .replace(
        "return migration.compile();",
        'return migration.createTimeSeriesCollection("scans").end().compile();',
      ),
  );
  await writeFile(
    `${tempDir}/schemas.ts`,
    schemasFile(pkg, schemasExpireAfterSeconds),
  );
}

function databaseName(): string {
  return `mongodbee_test_ts_cli_${crypto.randomUUID().slice(0, 8)}`;
}

test({
  name: "cli: check reports a time series option that schemas.ts changed without a migration",
  ignore: needs8,
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (tempDir) => {
      await project(tempDir, databaseName(), 60);
      const report = await runCheck(
        { cwd: tempDir, mode: "quick" },
        { log: () => {} },
      );
      assertEquals(report.valid, false);
      assertStringIncludes(
        (report.schema?.errors ?? []).join("\n"),
        'Time series collection "scans" options differ (last migration → project schema): expireAfterSeconds goes from 3600s to 60s',
      );
    });
  },
});

test({
  name: "cli: a time series collection is checked, migrated and rolled back",
  ignore: needs8,
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (tempDir) => {
      const dbName = databaseName();
      const client = new MongoClient(TEST_URI);
      const db = client.db(dbName);
      try {
        await project(tempDir, dbName, 3600);

        const lines: string[] = [];
        const report = await runCheck(
          { cwd: tempDir, mode: "quick" },
          { log: (line) => lines.push(line) },
        );
        assertEquals(report.valid, true, lines.join("\n"));

        await migrateCommand({ cwd: tempDir, force: true });
        assertEquals((await getAppliedMigrationIds(db)).length, 1);
        const inspection = await inspectTimeSeries(db, "scans", {
          timeField: "at",
          metaField: "meta",
          granularity: "minutes",
          expireAfterSeconds: 3600,
        });
        assert(inspection.state === "exists");
        assertEquals(inspection.plan.inSync, true);
        assertEquals(
          (await db.collection("scans").indexes()).map((i) => i.name).sort(),
          ["_idx_scanId_asc", "meta_1_at_1"],
        );

        await rollbackCommand({ cwd: tempDir, force: true });
        assertEquals((await getAppliedMigrationIds(db)).length, 0);
        assertEquals(
          (await db.listCollections({ name: "scans" }).toArray()).length,
          0,
        );
      } finally {
        await db.dropDatabase();
        await client.close();
      }
    });
  },
});
