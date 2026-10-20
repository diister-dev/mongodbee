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
  // What this run wrote in the target, by `_id`: the only copies it may replace.
  const copied = new Map<string, Doc>();
  const deleted = new Set<string>();
  const conflicts: unknown[] = [];

  // One document: "moved", "left" for the next pass, or recorded as a conflict.
  const moveOne = async (doc: Doc): Promise<"moved" | "left"> => {
    const held = await target.findOne({ _id: doc._id });
    if (held === null) {
      // Inserted as read, its fields in the source's order, so the copy compares
      // equal to it in MongoDB (`$$ROOT`). A document inserted meanwhile under this
      // `_id` makes the insert fail: of another type, the move stops there;
      // of this type, the next pass decides.
      try {
        await target.insertOne(doc);
      } catch (error) {
        if (!isDuplicateKey(error)) throw error;
        const raced = await target.findOne({ _id: doc._id });
        if (raced !== null && raced._type !== documentType) throw error;
        return "left";
      }
    } else if (held._type !== documentType) {
      throw new Error(
        `Cannot move ${names.from}.${documentType} into ${names.into}: _id ${String(doc._id)} is held there by a document of type ${String(held._type)}`,
      );
    } else if (!isDeepStrictEqual(held, doc)) {
      const ours = copied.get(idKey(doc._id));
      if (ours === undefined || !isDeepStrictEqual(held, ours)) {
        conflicts.push(doc._id);
        return "left";
      }
      const replaced = await target.replaceOne(sameContent(held), doc);
      if (replaced.matchedCount === 0) return "left";
    }
    copied.set(idKey(doc._id), doc);
    const removed = await source.deleteOne(sameContent(doc));
    if (removed.deletedCount === 0) return "left";
    deleted.add(idKey(doc._id));
    return "moved";
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
      const outcomes = await Promise.all(docs.map(moveOne));
      io.progress(outcomes.filter((outcome) => outcome === "moved").length);
      lastId = docs[docs.length - 1]._id;
    }
    if (conflicts.length > 0) {
      throw new MoveConflictError(
        names.from,
        names.into,
        documentType,
        conflicts,
      );
    }
    if (
      (await source.countDocuments({ _type: documentType }, { limit: 1 })) === 0
    )
      break;
    if (pass === MOVE_MAX_PASSES) {
      throw new Error(
        `Cannot move ${names.from}.${documentType} into ${names.into}: its documents kept changing over ${MOVE_MAX_PASSES} passes. Nothing was lost; stop the writers of the type and run the migration again.`,
      );
    }
  }

  // A document deleted from the source by a writer after this run copied it
  // must not come back to life in the target: its copy goes, unless the
  // target changed it since.
  for (const [key, doc] of copied) {
    if (deleted.has(key)) continue;
    if ((await source.countDocuments({ _id: doc._id }, { limit: 1 })) > 0) {
      continue;
    }
    const held = await target.findOne({ _id: doc._id });
    if (held !== null && isDeepStrictEqual(held, doc)) {
      await target.deleteOne(sameContent(held));
    }
  }
}
