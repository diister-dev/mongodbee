import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import {
  getLastAppliedMigration,
  getMigrationOperationsCollection,
  type MigrationOperation,
} from "../../src/migration/history.ts";

// One `migrate` run applies its migrations back to back, so several land on
// the same executedAt millisecond. The last applied one used to be whichever
// the state Map yielded first among the tie — and `rollback` reverts the last
// applied one. Ids carry the chain order and must break the tie.

const FIRST = "2026_09_10_2230_6Q3EA9AAAA@first";
const SECOND = "2026_09_10_2230_6Q3EAEBJN2@second";
const THIRD = "2026_09_10_2230_6Q3EARD78Q@third";

function applied(
  migrationId: string,
  executedAt: Date,
): Omit<MigrationOperation, "_id"> {
  return {
    migrationId,
    migrationName: migrationId.split("@")[1] ?? migrationId,
    operation: "applied",
    executedAt,
    status: "success",
    mongodbeeVersion: "test",
  };
}

test("getLastAppliedMigration: same-millisecond applies resolve to the latest id", async () => {
  await withDatabase("history-tie-same-ms", async (db) => {
    const at = new Date("2026-09-10T22:30:00.000Z");
    // Inserted latest-first so that insertion order alone would pick THIRD's
    // predecessor if the tie were left to the Map.
    await getMigrationOperationsCollection(db).insertMany([
      applied(THIRD, at),
      applied(FIRST, at),
      applied(SECOND, at),
    ]);

    const last = await getLastAppliedMigration(db);
    assertEquals(last?.migrationId, THIRD);
  });
});

test("getLastAppliedMigration: a later execution still wins over a later id", async () => {
  await withDatabase("history-tie-later-execution", async (db) => {
    // SECOND was rolled back and re-applied after THIRD: it is the one to
    // revert next, whatever its id says.
    await getMigrationOperationsCollection(db).insertMany([
      applied(THIRD, new Date("2026-09-10T22:30:00.000Z")),
      applied(SECOND, new Date("2026-09-10T22:31:00.000Z")),
    ]);

    const last = await getLastAppliedMigration(db);
    assertEquals(last?.migrationId, SECOND);
  });
});
