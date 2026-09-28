import type { Document } from "mongodb";
import type { Db } from "../mongodb.ts";
import { dirtyEquivalent } from "./object.ts";
import { createLogger } from "./logger.ts";
import { PRIMARY } from "../read-preference.ts";
import { isRecord } from "./guards.ts";

const log = createLogger("validator");

/** MongoDB's error code when `create` targets a namespace that already exists. */
const NAMESPACE_EXISTS = 48;

/**
 * Brings `collectionName` to `validator`: creates the collection when it is
 * missing, otherwise `collMod`s it when the stored validator differs.
 *
 * The check and the create are two round trips, so two processes booting
 * against the same empty database (an api and a worker, two replicas of one
 * rollout) can both see the collection missing and both try to create it.
 * MongoDB 6 rejects the loser with NamespaceExists — 8.0 only does when the
 * options differ. Losing that race is not an error: the collection exists
 * now, so the loser takes the same path as any other existing collection.
 */
export async function ensureValidator(
  db: Db,
  collectionName: string,
  validator: Document,
): Promise<void> {
  const existing = await db
    .listCollections({ name: collectionName }, { readPreference: PRIMARY })
    .toArray();

  if (existing.length === 0) {
    log.debug(`ensureValidator(${collectionName}): createCollection`);
    try {
      await db.createCollection(collectionName, { validator });
      return;
    } catch (error) {
      if (!isNamespaceExists(error)) throw error;
      log.debug(
        `ensureValidator(${collectionName}): created concurrently, comparing validator`,
      );
    }
  }

  const options = await db.command({
    listCollections: 1,
    filter: { name: collectionName },
  });
  const stored: unknown = options.cursor?.firstBatch?.[0]?.options;
  const current = isRecord(stored) ? stored : {};
  const enforced =
    (current.validationLevel ?? "strict") === "strict" &&
    (current.validationAction ?? "error") === "error";
  if (enforced && dirtyEquivalent(current.validator ?? {}, validator)) {
    return;
  }

  if (!enforced) {
    log.warn(
      `ensureValidator(${collectionName}): validation was ${String(current.validationLevel ?? "strict")}/${String(current.validationAction ?? "error")}, restoring strict/error`,
    );
  }
  await db.command({
    collMod: collectionName,
    validator,
    validationLevel: "strict",
    validationAction: "error",
  });
}

function isNamespaceExists(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === NAMESPACE_EXISTS
  );
}
