/**
 * Moving every document of a type from one scoped multi-collection to another,
 * on MongoDB, without losing a write made meanwhile.
 *
 * Each document is moved on its own: copied into the target only where the
 * target holds nothing under its `_id` (or still holds the copy this run made),
 * then deleted from the source only if the source still holds exactly what was
 * copied. A document written in the source between the two stays there and is
 * moved again by the next pass; a document written in the target after its
 * copy is never overwritten. The source is read in `_id` order with a cursor,
 * pass after pass, until no document of the type is left in it.
 *
 * @module
 */

import { isDeepStrictEqual } from "node:util";
import type * as m from "mongodb";

type Doc = m.Document;

/** How many passes over the source a move makes before it gives up. */
export const MOVE_MAX_PASSES = 10;

/** What a move needs of the applier around it. */
export interface MoveTypeIo {
  readonly source: m.Collection<Doc>;
  readonly target: m.Collection<Doc>;
  readonly batchSize: number;
  /** Called with the number of documents moved by each batch. */
  readonly progress: (moved: number) => void;
}

/** A document both collections hold, each with its own content. */
export class MoveConflictError extends Error {
  readonly from: string;
  readonly into: string;
  readonly documentType: string;
  readonly ids: readonly unknown[];

  constructor(
    from: string,
    into: string,
    documentType: string,
    ids: readonly unknown[],
  ) {
    super(
      `Cannot move ${from}.${documentType} into ${into}: ${ids.length} document(s) differ between the two collections (${ids
        .slice(0, 5)
        .map(String)
        .join(
          ", ",
        )}${ids.length > 5 ? ", …" : ""}). Neither copy was overwritten; keep the right one, delete the other, and run the migration again.`,
    );
    this.name = "MoveConflictError";
    this.from = from;
    this.into = into;
    this.documentType = documentType;
    this.ids = ids;
  }
}

const isDuplicateKey = (error: unknown): boolean =>
  (error as { code?: number } | null)?.code === 11000;

// `_id`s as MongoDB tells them apart: the string "x" and an ObjectId are two ids.
export const idKey = (id: unknown): string =>
  typeof id === "string" ? `s:${id}` : `o:${JSON.stringify(id)}`;

const sameContent = (stored: Doc) => ({
  _id: stored._id,
  $expr: { $eq: ["$$ROOT", { $literal: stored }] },
});

/**
 * Moves every document of `documentType` from `io.source` to `io.target`, and
 * resolves once none is left in the source. Throws {@link MoveConflictError}
 * when a document differs between the two collections and this run did not
 * write the target's copy, and an error when a document of another type holds
 * the same `_id` in the target: the documents before it are moved by then.
 */
export async function moveDocumentsOfType(
  io: MoveTypeIo,
  names: { from: string; into: string; documentType: string },
): Promise<void> {
  const { source, target, batchSize } = io;
  const { documentType } = names;
  // Copies this run wrote whose source it has not deleted yet, by `_id`: the
  // only copies it may replace or withdraw. Bounded by the documents a writer
  // touched during the move, not by the size of the type.
  const pending = new Map<string, Doc>();
  const conflicts: unknown[] = [];

  // One document: "moved", "left" for the next pass, or recorded as a conflict.
  const moveOne = async (doc: Doc): Promise<"moved" | "left"> => {
    const key = idKey(doc._id);
    const held = await target.findOne({ _id: doc._id });
    if (held === null) {
      // Inserted as read, its fields in the source's order, so the copy compares
      // equal to it in MongoDB (`$$ROOT`). A document of this type inserted
      // meanwhile under this `_id` leaves the decision to the next pass; any
      // other duplicate key (another type, or another `_id` holding a unique
      // field) stops the move.
      try {
        await target.insertOne(doc);
      } catch (error) {
        if (!isDuplicateKey(error)) throw error;
        const raced = await target.findOne({ _id: doc._id });
        if (raced === null || raced._type !== documentType) throw error;
        return "left";
      }
    } else if (held._type !== documentType) {
      throw new Error(
        `Cannot move ${names.from}.${documentType} into ${names.into}: _id ${String(doc._id)} is held there by a document of type ${String(held._type)}`,
      );
    } else if (!isDeepStrictEqual(held, doc)) {
      const ours = pending.get(key);
      if (ours === undefined || !isDeepStrictEqual(held, ours)) {
        conflicts.push(doc._id);
        return "left";
      }
      const replaced = await target.replaceOne(sameContent(held), doc);
      if (replaced.matchedCount === 0) return "left";
    }
    pending.set(key, doc);
    const removed = await source.deleteOne(sameContent(doc));
    if (removed.deletedCount === 0) return "left";
    pending.delete(key);
    return "moved";
  };

  // A copy whose source was not deleted goes, unless the target changed it
  // since: the source holds a newer version, or a writer deleted it there. Run
  // before every exit, so each document ends in one collection only and a
  // later run finds no copy it cannot tell from a conflict.
  const withdrawPending = async (): Promise<void> => {
    for (const ours of pending.values()) {
      const held = await target.findOne({ _id: ours._id });
      if (held !== null && isDeepStrictEqual(held, ours)) {
        await target.deleteOne(sameContent(held));
      }
    }
    pending.clear();
  };

  const fail = async (error: unknown): Promise<never> => {
    await withdrawPending();
    throw error;
  };

  for (let pass = 1; ; pass += 1) {
    let lastId: unknown;
    while (true) {
      const filter: Doc =
        lastId === undefined
          ? { _type: documentType }
          : { _type: documentType, _id: { $gt: lastId } };
      const docs = await source
        .find(filter)
        .sort({ _id: 1 })
        .limit(batchSize)
        .toArray();
      if (docs.length === 0) break;
      // Settled first, so no write of the batch is still running when the move
      // reports its failure.
      const outcomes = await Promise.allSettled(docs.map(moveOne));
      const failed = outcomes.find((outcome) => outcome.status === "rejected");
      if (failed !== undefined) await fail(failed.reason);
      io.progress(
        outcomes.filter(
          (outcome) =>
            outcome.status === "fulfilled" && outcome.value === "moved",
        ).length,
      );
      lastId = docs[docs.length - 1]._id;
    }
    if (conflicts.length > 0) {
      await fail(
        new MoveConflictError(names.from, names.into, documentType, conflicts),
      );
    }
    if (
      (await source.countDocuments({ _type: documentType }, { limit: 1 })) === 0
    )
      break;
    if (pass === MOVE_MAX_PASSES) {
      await fail(
        new Error(
          `Cannot move ${names.from}.${documentType} into ${names.into}: its documents kept changing over ${MOVE_MAX_PASSES} passes. Nothing was lost; stop the writers of the type and run the migration again.`,
        ),
      );
    }
  }

  await withdrawPending();
}
