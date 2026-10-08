import type { Db } from "../mongodb.ts";
import { isMetadataDocument } from "./state.ts";
import { COMPUTED_ROOT } from "../computed-guard.ts";
import { sanitizeForMongoDB } from "../sanitizer.ts";
import { createMongodbApplier } from "../migration/appliers/mongodb.ts";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type MigrationDefinition,
  type SchemasDefinition,
} from "../migration/types.ts";
import {
  createMultiCollectionInfo,
  discoverMultiCollectionInstances,
  MULTI_COLLECTION_INFO_TYPE,
} from "../migration/multicollection-registry.ts";

export interface WriteStateOptions {
  readonly batchSize?: number;
}

const DUPLICATE_KEY = 11000;
const DUPLICATE_INDEX_PATTERN = /index: (\S+) dup key/;

interface BulkWriteFailure {
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

function sanitizeKeepingComputed(
  doc: Record<string, unknown>,
): Record<string, unknown> {
  const { [COMPUTED_ROOT]: computed, ...rest } = doc;
  const clean = sanitizeForMongoDB(rest);
  return computed === undefined
    ? clean
    : { ...clean, [COMPUTED_ROOT]: computed };
}

async function insertAll(
  db: Db,
  collection: string,
  docs: readonly Record<string, unknown>[],
  batchSize: number,
): Promise<number> {
  if (docs.length === 0) return 0;
  const target = db.collection(collection);
  for (let i = 0; i < docs.length; i += batchSize) {
    const batch = docs.slice(i, i + batchSize);
    try {
      await target.insertMany(batch.map(sanitizeKeepingComputed), {
        ordered: false,
        bypassDocumentValidation: true,
      });
    } catch (error) {
      const failures = bulkWriteFailures(error);
      if (failures === undefined) throw error;
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
      written[name] = await insertAll(db, name, content, batchSize);
    }
  }
  for (const [name, { content }] of Object.entries(state.multiModels)) {
    written[name] = await insertAll(db, name, content, batchSize);
  }
  return written;
}

export interface PopulateDatabaseOptions extends WriteStateOptions {
  readonly migration: MigrationDefinition;
  /**
   * Migration recorded as the creation point of a multi-model instance that
   * carries no `_information` marker in the state (default: `migration`).
   */
  readonly instanceOrigin?: string;
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
  const managed = new Set([
    ...WRITE_BUCKETS.flatMap((bucket) => [
      ...Object.keys(schemas[bucket] ?? {}),
      ...Object.keys(state[bucket]),
    ]),
    ...Object.keys(state.multiModels),
  ]);
  const existing = await listCollectionNames(db);
  const clashing = [...managed].filter((name) => existing.has(name));
  const occupied: string[] = [];
  for (const name of clashing) {
    if ((await db.collection(name).countDocuments({}, { limit: 1 })) > 0) {
      occupied.push(name);
    }
  }
  if (occupied.length > 0) {
    throw new Error(
      `The target already holds documents in the collection(s) ${occupied.join(", ")} that the schemas manage; extract and seed never write into them, a failed write could not restore their validators and indexes`,
    );
  }
  for (const name of clashing) await db.collection(name).drop();
  const created: string[] = [];
  const create = async (name: string) => {
    if (created.includes(name)) return;
    await db.createCollection(name);
    created.push(name);
  };
  try {
    for (const name of managed) await create(name);
    const remaining = createEmptyDatabaseState();
    Object.assign(remaining, {
      collections: state.collections,
      multiCollections: state.multiCollections,
      scopedMultiCollections: state.scopedMultiCollections,
    });
    for (const [name, instance] of Object.entries(state.multiModels)) {
      const metadata = instance.content.filter(isMetadataDocument);
      if (metadata.some((doc) => doc._type === MULTI_COLLECTION_INFO_TYPE)) {
        await insertAll(db, name, metadata, batchSize);
      } else {
        await createMultiCollectionInfo(
          db,
          name,
          instance.modelType,
          options.instanceOrigin ?? options.migration.id,
        );
      }
      remaining.multiModels[name] = {
        ...instance,
        content: instance.content.filter((doc) => !isMetadataDocument(doc)),
      };
    }
    await createMongodbApplier(db, options.migration, {
      currentMigrationId: "",
    }).applyMigration([], "up");
    return await writeStateToDatabase(db, remaining, { batchSize });
  } catch (error) {
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
