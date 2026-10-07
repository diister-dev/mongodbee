/**
 * Detects and repairs validators an interrupted migration left disabled.
 *
 * @module
 */

import type { Db } from "../../../mongodb.ts";
import { green, red, yellow } from "../../../utils/colors.ts";
import { createMongodbApplier } from "../../appliers/mongodb.ts";
import { getLastAppliedMigration } from "../../state.ts";
import type { MigrationDefinition } from "../../types.ts";
import {
  clearValidatorSuspension,
  describeValidatorSuspension,
  getValidatorSuspension,
} from "../../validator-guard.ts";

/**
 * When a previous run died with validators off, restores them to the
 * schemas of the migration the database is recorded at, and says so.
 *
 * The repair targets the APPLIED head, not the interrupted migration: that
 * one was never recorded as applied, so the next `migrate` runs it again
 * from the start. A collection the interrupted run had created, and that the
 * head does not declare, gets its validation level back with no rules.
 *
 * @returns whether a repair was needed
 */
export async function repairSuspendedValidators(
  db: Db,
  migrations: MigrationDefinition[],
  log: (line: string) => void = (line) => console.log(line),
): Promise<boolean> {
  const suspension = await getValidatorSuspension(db);
  if (!suspension) return false;

  log(red(`⚠ ${describeValidatorSuspension(suspension)}`));

  const last = await getLastAppliedMigration(db);
  const head = last ? migrations.find((m) => m.id === last.id) : undefined;
  if (last && !head) {
    throw new Error(
      `Cannot repair the validators: the applied migration ${last.id} is not in the migrations folder`,
    );
  }

  const declared = new Set<string>([
    ...Object.keys(head?.schemas.collections ?? {}),
    ...Object.keys(head?.schemas.multiCollections ?? {}),
    ...Object.keys(head?.schemas.scopedMultiCollections ?? {}),
  ]);
  const undeclared: string[] = [];
  for (const name of suspension.collections) {
    if (declared.has(name)) continue;
    const exists =
      (await db.listCollections({ name }, { nameOnly: true }).toArray())
        .length > 0;
    // Multi-model instances are restored with their model below; anything
    // else the head does not know only needs its validation level back.
    if (exists && !(await isModelInstance(db, name))) {
      await db.command({ collMod: name, validationLevel: "strict" });
      undeclared.push(name);
    }
  }

  if (head) {
    // An empty migration: switches the head's collections off and back on
    // with the head's validators and indexes, then clears what it restored.
    await createMongodbApplier(db, head).applyMigration([], "up");
  }
  await clearValidatorSuspension(db, suspension.collections);

  log(
    green(
      `✓ Validators restored${head ? ` to ${head.id}` : ""}` +
        (undeclared.length > 0
          ? ` (validation level only: ${undeclared.join(", ")})`
          : ""),
    ),
  );
  log(
    yellow(
      `  The interrupted ${suspension.direction === "up" ? "migration" : "rollback"} of ${suspension.migrationId} did not complete; check its data before re-running it.`,
    ),
  );
  log("");
  return true;
}

/** A multi-model instance carries an `_information` document. */
async function isModelInstance(db: Db, name: string): Promise<boolean> {
  const info = await db
    .collection(name)
    .findOne({ _id: "_information" as never }, { projection: { _id: 1 } });
  return info !== null;
}
