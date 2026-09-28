import { type Collection, type Filter, ObjectId } from "mongodb";
import type { Db } from "./mongodb.ts";
import { isRecord } from "./utils/guards.ts";

export type DocumentId = string | ObjectId;

export function isDocumentId(value: unknown): value is DocumentId {
  return typeof value === "string" || value instanceof ObjectId;
}

export interface StoredDocument {
  _id: DocumentId;
  [field: string]: unknown;
}

export function isStoredDocument(value: unknown): value is StoredDocument {
  return isRecord(value) && isDocumentId(value._id);
}

export function toStoredDocument(value: unknown): StoredDocument {
  if (!isStoredDocument(value)) {
    throw new TypeError(
      "a document written by mongodbee must be an object with a string or ObjectId _id",
    );
  }
  return value;
}

export function isStoredFilter(
  value: unknown,
): value is Filter<StoredDocument> {
  return isRecord(value);
}

export function toStoredFilter(value: unknown): Filter<StoredDocument> {
  if (value === undefined) return {};
  if (!isStoredFilter(value)) {
    throw new TypeError("a filter must be a plain object");
  }
  return value;
}

export function storedCollection(
  db: Db,
  name: string,
): Collection<StoredDocument> {
  return db.collection<StoredDocument>(name);
}
