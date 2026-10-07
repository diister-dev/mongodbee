/**
 * Durable record of validators a migration has switched off.
 *
 * A migration disables the validators of every collection it touches, runs
 * its operations, then restores them. When the process dies in between (a
 * timeout killing the CLI, Ctrl-C, a crash), nothing restored them and
 * nothing said so: the collections silently accepted any document until a
 * later `ensureValidator` happened to notice (`validation was off/error,
 * restoring strict/error`).
 *
 * The applier now writes this record BEFORE disabling anything and deletes
 * it only once every validator is back. A record found by a later command
 * is therefore proof of an interrupted run, and names what to repair.
 *
 * @module
 */

import type { Db } from "../mongodb.ts";
import { primaryCollection } from "../read-preference.ts";

/** Collection holding the record; next to the migration history. */
export const VALIDATOR_GUARD_COLLECTION = "__dbee_validator_guard__";

const SUSPENSION_ID = "validators_suspended";

/** What an interrupted run left behind. */
export interface ValidatorSuspension {
  /** The migration being applied or rolled back when validators went off. */
  migrationId: string;
  direction: "up" | "down";
  /** Collections whose validator was switched off. */
  collections: string[];
  since: Date;
}

type SuspensionDocument = ValidatorSuspension & { _id: string };

function guard(db: Db) {
  return primaryCollection<SuspensionDocument>(db, VALIDATOR_GUARD_COLLECTION);
}

/** Records that `collections` are about to lose their validator. */
export async function recordValidatorSuspension(
  db: Db,
  suspension: Omit<ValidatorSuspension, "since">,
): Promise<void> {
  await guard(db).updateOne(
    { _id: SUSPENSION_ID },
    {
      $set: {
        migrationId: suspension.migrationId,
        direction: suspension.direction,
        since: new Date(),
      },
      // A record left by an earlier interrupted run keeps its collections:
      // they are still unrepaired until a full restore clears the record.
      $addToSet: { collections: { $each: suspension.collections } },
    },
    { upsert: true },
  );
}

/**
 * Removes `collections` from the record once their validators are back, and
 * the record itself when nothing is left. Collections an EARLIER interrupted
 * run left behind stay listed until they are restored too.
 */
export async function clearValidatorSuspension(
  db: Db,
  collections: string[],
): Promise<void> {
  await guard(db).updateOne(
    { _id: SUSPENSION_ID },
    { $pull: { collections: { $in: collections } } },
  );
  const { deletedCount } = await guard(db).deleteOne({
    _id: SUSPENSION_ID,
    collections: { $size: 0 },
  });
  // Nothing left to track: the collection itself goes, so a database a
  // migration (or its rollback) went through keeps exactly the shape the
  // schemas give it, with no bookkeeping collection behind.
  if (deletedCount > 0 && (await guard(db).estimatedDocumentCount()) === 0) {
    try {
      await guard(db).drop();
    } catch (error) {
      const e = error as { code?: number; codeName?: string };
      if (e?.code !== 26 && e?.codeName !== "NamespaceNotFound") throw error;
    }
  }
}

/** The record of an interrupted run, if any. */
export async function getValidatorSuspension(
  db: Db,
): Promise<ValidatorSuspension | null> {
  const document = await guard(db).findOne({ _id: SUSPENSION_ID });
  if (!document) return null;
  const { _id: _ignored, ...suspension } = document;
  return suspension;
}

/** One line for an operator. */
export function describeValidatorSuspension(
  suspension: ValidatorSuspension,
): string {
  return (
    `validators were left disabled by an interrupted ${suspension.direction === "up" ? "migration" : "rollback"} ` +
    `of ${suspension.migrationId} (since ${suspension.since.toISOString()}) on: ` +
    suspension.collections.join(", ")
  );
}
