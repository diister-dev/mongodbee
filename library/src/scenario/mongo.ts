import type { Db } from "../mongodb.ts";
import { isMetadataDocument } from "./state.ts";
import { sanitizeForMongoDB } from "../sanitizer.ts";
import { createMongodbApplier } from "../migration/appliers/mongodb.ts";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type MigrationDefinition,
  type SchemasDefinition,
} from "../migration/types.ts";
import { discoverMultiCollectionInstances } from "../migration/multicollection-registry.ts";

export type InsertedIds = Map<string, unknown[]>;

export interface WriteStateOptions {
  readonly batchSize?: number;
  readonly inserted?: InsertedIds;
}

const DUPLICATE_KEY = 11000;
const DUPLICATE_INDEX_PATTERN = /index: (\S+) dup key/;

interface BulkWriteFailure {
  readonly index?: number;
  readonly code?: number;
  readonly errmsg?: string;
}

function bulkWriteFailures(error: unknown): BulkWriteFailure[] | undefined {
  const raw = (error as { writeErrors?: BulkWriteFailure | BulkWriteFailure[] })
    .writeErrors;
  if (raw === undefined) return undefined;
  return Array.isArray(raw) ? raw : [raw];
}

function describeBulkWriteFailure(
  collection: string,
  attempted: number,
  failures: readonly BulkWriteFailure[],
): string {
  const duplicates = failures.filter((f) => f.code === DUPLICATE_KEY);
  const others = failures.filter((f) => f.code !== DUPLICATE_KEY);
  const parts = [`${failures.length} of ${attempted} document(s) rejected`];
  if (duplicates.length > 0) {
    const index = DUPLICATE_INDEX_PATTERN.exec(duplicates[0].errmsg ?? "")?.[1];
    parts.push(
      `${duplicates.length} duplicate key${index ? ` on index ${index}` : ""}`,
    );
  }
  if (others.length > 0) {
    parts.push(`${others.length} other (code ${others[0].code})`);
  }
  return `Writing "${collection}" failed: ${parts.join(", ")}`;
}

async function insertAll(
  db: Db,
  collection: string,
  docs: readonly Record<string, unknown>[],
  batchSize: number,
  inserted?: InsertedIds,
): Promise<number> {
  if (docs.length === 0) return 0;
  const log = (ids: unknown[]) => {
    if (inserted)
      inserted.set(collection, [...(inserted.get(collection) ?? []), ...ids]);
  };
  const target = db.collection(collection);
  for (let i = 0; i < docs.length; i += batchSize) {
    const batch = docs.slice(i, i + batchSize);
    try {
      await target.insertMany(
        batch.map((d) => sanitizeForMongoDB(d)),
        { ordered: false, bypassDocumentValidation: true },
      );
      log(batch.map((d) => d._id));
    } catch (error) {
      const failures = bulkWriteFailures(error);
      if (failures === undefined) throw error;
      const rejected = new Set(failures.map((f) => f.index));
      log(batch.filter((_, index) => !rejected.has(index)).map((d) => d._id));
      throw new Error(
        describeBulkWriteFailure(collection, batch.length, failures),
        { cause: error },
      );
    }
  }
  return docs.length;
}

type WriteBucket = Exclude<keyof DatabaseState, "multiModels">;
const WRITE_BUCKETS: readonly WriteBucket[] = [
  "collections",
  "multiCollections",
  "scopedMultiCollections",
];

export async function writeStateToDatabase(
  db: Db,
  state: DatabaseState,
  options: WriteStateOptions = {},
): Promise<Record<string, number>> {
  const batchSize = options.batchSize ?? 500;
  const written: Record<string, number> = {};
  for (const bucket of WRITE_BUCKETS) {
    for (const [name, { content }] of Object.entries(state[bucket])) {
      written[name] = await insertAll(
        db,
        name,
        content,
        batchSize,
        options.inserted,
      );
    }
  }
  for (const [name, { content }] of Object.entries(state.multiModels)) {
    written[name] = await insertAll(
      db,
      name,
      content,
      batchSize,
      options.inserted,
    );
  }
  return written;
}

export interface PopulateDatabaseOptions extends WriteStateOptions {
  readonly migration: MigrationDefinition;
}

async function listCollectionNames(db: Db): Promise<Set<string>> {
  const infos = await db.listCollections({}, { nameOnly: true }).toArray();
  return new Set(infos.map((info) => info.name));
}

export async function populateDatabase(
  db: Db,
  state: DatabaseState,
  options: PopulateDatabaseOptions,
): Promise<Record<string, number>> {
  const batchSize = options.batchSize ?? 500;
  const { schemas } = options.migration;
  const existing = await listCollectionNames(db);
  const created: string[] = [];
  const inserted: InsertedIds = new Map();
  const create = async (name: string) => {
    if (existing.has(name) || created.includes(name)) return;
    await db.createCollection(name);
    created.push(name);
  };
  try {
    for (const bucket of WRITE_BUCKETS) {
      for (const name of Object.keys(schemas[bucket] ?? {})) await create(name);
      for (const name of Object.keys(state[bucket])) await create(name);
    }
    const remaining = createEmptyDatabaseState();
    Object.assign(remaining, {
      collections: state.collections,
      multiCollections: state.multiCollections,
      scopedMultiCollections: state.scopedMultiCollections,
    });
    for (const [name, instance] of Object.entries(state.multiModels)) {
      await create(name);
      await insertAll(
        db,
        name,
        instance.content.filter(isMetadataDocument),
        batchSize,
        inserted,
      );
      remaining.multiModels[name] = {
        ...instance,
        content: instance.content.filter((doc) => !isMetadataDocument(doc)),
      };
    }
    await createMongodbApplier(db, options.migration, {
      currentMigrationId: "",
    }).applyMigration([], "up");
    return await writeStateToDatabase(db, remaining, { batchSize, inserted });
  } catch (error) {
    for (const [name, ids] of inserted) {
      if (created.includes(name)) continue;
      await db
        .collection(name)
        .deleteMany({ _id: { $in: ids } } as never)
        .catch(() => false);
    }
    for (const name of created) {
      await db
        .collection(name)
        .drop()
        .catch(() => false);
    }
    throw error;
  }
}

export interface ReadStateOptions {
  readonly scope?: string;
}

export async function readStateFromDatabase(
  db: Db,
  schemas: SchemasDefinition,
  options: ReadStateOptions = {},
): Promise<DatabaseState> {
  const state = createEmptyDatabaseState();
  const read = async (name: string, filter: Record<string, unknown> = {}) =>
    (await db
      .collection(name)
      .find(filter)
      .sort({ _id: 1 })
      .toArray()) as Record<string, unknown>[];

  for (const name of Object.keys(schemas.collections ?? {})) {
    state.collections[name] = { content: await read(name) };
  }
  for (const name of Object.keys(schemas.multiCollections ?? {})) {
    state.multiCollections[name] = { content: await read(name) };
  }
  for (const name of Object.keys(schemas.scopedMultiCollections ?? {})) {
    state.scopedMultiCollections[name] = {
      content: await read(name, options.scope ? { _scope: options.scope } : {}),
    };
  }
  for (const model of Object.keys(schemas.multiModels ?? {})) {
    for (const name of await discoverMultiCollectionInstances(db, model)) {
      if (options.scope && name !== options.scope) continue;
      state.multiModels[name] = {
        modelType: model,
        content: await read(name),
      };
    }
  }
  return state;
}

export async function countCollections(db: Db): Promise<number> {
  const existing = await listCollectionNames(db);
  return [...existing].filter((name) => !name.startsWith("system.")).length;
}
