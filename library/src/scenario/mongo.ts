import type { Db } from "../mongodb.ts";
import { sanitizeForMongoDB } from "../sanitizer.ts";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type SchemasDefinition,
} from "../migration/types.ts";

export interface WriteStateOptions {
  readonly batchSize?: number;
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
    await target.insertMany(
      docs.slice(i, i + batchSize).map((d) => sanitizeForMongoDB(d)),
      { ordered: false },
    );
  }
  return docs.length;
}

export async function writeStateToDatabase(
  db: Db,
  state: DatabaseState,
  options: WriteStateOptions = {},
): Promise<Record<string, number>> {
  const batchSize = options.batchSize ?? 500;
  const written: Record<string, number> = {};
  for (const [name, { content }] of Object.entries(state.collections)) {
    written[name] = await insertAll(db, name, content, batchSize);
  }
  for (const [name, { content }] of Object.entries(state.multiCollections)) {
    written[name] = await insertAll(db, name, content, batchSize);
  }
  for (
    const [name, { content }] of Object.entries(state.scopedMultiCollections)
  ) {
    written[name] = await insertAll(db, name, content, batchSize);
  }
  for (const [name, { content }] of Object.entries(state.multiModels)) {
    written[name] = await insertAll(db, name, content, batchSize);
  }
  return written;
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
    await db.collection(name).find(filter).toArray() as Record<
      string,
      unknown
    >[];

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
  const models = Object.keys(schemas.multiModels ?? {});
  if (models.length > 0) {
    const existing = await db.listCollections({}, { nameOnly: true }).toArray();
    for (const info of existing) {
      const model = models.find((m) => info.name.startsWith(`${m}:`));
      if (!model) continue;
      if (options.scope && info.name !== options.scope) continue;
      state.multiModels[info.name] = {
        modelType: model,
        content: await read(info.name),
      };
    }
  }
  return state;
}

export async function countDocuments(db: Db): Promise<number> {
  const existing = await db.listCollections({}, { nameOnly: true }).toArray();
  let total = 0;
  for (const info of existing) {
    if (info.name.startsWith("system.")) continue;
    total += await db.collection(info.name).estimatedDocumentCount();
  }
  return total;
}
