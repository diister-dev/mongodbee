/**
 * Read preference rules shared by every collection kind.
 *
 * - A transaction always runs on the primary: the driver rejects any read
 *   inside a transaction whose read preference is not `primary`, so the
 *   per-call options must never carry one there.
 * - Internal metadata reads (migration history, multi-collection registry,
 *   index and validator sync) always read the primary: they precede a write
 *   and must not see a lagging secondary, whatever the client default is.
 * - Everything else follows the collection, then the client, default.
 *
 * @module
 */

import * as m from "mongodb";
import type { ClientSession, Db } from "./mongodb.ts";

/** The primary read preference. */
export const PRIMARY: m.ReadPreference = m.ReadPreference.primary;

/** Per-call read options accepted by the methods that take no driver options. */
export type ReadOptions = {
  /**
   * Overrides the collection's read preference for this call, e.g.
   * `"primary"` to read your own writes on a collection reading secondaries.
   * Ignored inside a transaction, which always reads the primary.
   */
  readPreference?: m.ReadPreferenceLike;
};

/**
 * The driver options of a read: the ambient session merged with the caller's
 * options. Inside a transaction, the read preference and concerns are dropped
 * so the transaction's own (primary) ones apply.
 */
export function readOpts<T extends object>(
  session: ClientSession | undefined,
  options?: T,
): T & { session: ClientSession | undefined } {
  if (!options)
    return { session } as T & { session: ClientSession | undefined };
  if (!session?.inTransaction()) return { session, ...options };
  const {
    readPreference: _readPreference,
    readConcern: _readConcern,
    writeConcern: _writeConcern,
    ...rest
  } = options as T & {
    readPreference?: unknown;
    readConcern?: unknown;
    writeConcern?: unknown;
  };
  return { session, ...rest } as T & { session: ClientSession | undefined };
}

/** A handle on `name` that always reads the primary. */
export function primaryCollection<T extends m.Document = m.Document>(
  db: Db | m.Db,
  name: string,
): m.Collection<T> {
  return db.collection<T>(name, { readPreference: PRIMARY });
}
