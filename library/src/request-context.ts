import { BSON } from "mongodb";
import type * as m from "mongodb";
import { contextVariable } from "./context-variable.ts";

export interface RequestContextOptions {
  memoizeReads?: boolean;
}

export interface RequestReadStats {
  loaded: number;
  reused: number;
  invalidations: number;
}

interface RequestState {
  reads: Map<string, Promise<Uint8Array>> | undefined;
  stats: RequestReadStats;
}

interface ReadTarget {
  readonly dbName: string;
  readonly collectionName: string;
}

const READ_OPERATIONS: ReadonlySet<string> = new Set([
  "getById",
  "findOne",
  "find",
  "findProject",
  "findOneAny",
  "findAny",
  "paginate",
  "countDocuments",
  "estimatedDocumentCount",
  "distinct",
  "aggregate",
  "listScopes",
  "scopeExists",
  "scopeStats",
]);

const DRIVER_WRITE_COMMANDS: ReadonlySet<string> = new Set([
  "insert",
  "update",
  "delete",
  "findAndModify",
  "bulkWrite",
  "commitTransaction",
]);

const requestState = contextVariable<RequestState>("mongodbee.request");

export function invalidateReadsOnDriverWrites(
  client: m.MongoClient,
): () => void {
  if (client.options.monitorCommands !== true) {
    throw new Error(
      "invalidateReadsOnDriverWrites needs a client created with monitorCommands: true",
    );
  }
  const onCommand = (event: { commandName: string }) => {
    if (DRIVER_WRITE_COMMANDS.has(event.commandName)) invalidateReads();
  };
  client.on("commandStarted", onCommand);
  client.on("commandSucceeded", onCommand);
  client.on("commandFailed", onCommand);
  return () => {
    client.off("commandStarted", onCommand);
    client.off("commandSucceeded", onCommand);
    client.off("commandFailed", onCommand);
  };
}

export function withRequestContext<T>(
  fn: () => T,
  options: RequestContextOptions = {},
): T {
  return requestState.run(
    {
      reads: options.memoizeReads === true ? new Map() : undefined,
      stats: { loaded: 0, reused: 0, invalidations: 0 },
    },
    fn,
  );
}

export function requestReadStats(): RequestReadStats | undefined {
  const stats = requestState.get()?.stats;
  return stats ? { ...stats } : undefined;
}

export function invalidateReads(): void {
  const state = requestState.get();
  if (!state?.reads || state.reads.size === 0) return;
  state.reads.clear();
  state.stats.invalidations++;
}

export function isReadOperation(operationName: string): boolean {
  return READ_OPERATIONS.has(operationName);
}

function readKey(
  target: ReadTarget,
  operation: string,
  args: readonly unknown[],
): string | undefined {
  try {
    return `${target.dbName}.${target.collectionName}\u0000${operation}\u0000${BSON.EJSON.stringify(
      args,
      { relaxed: false },
    )}`;
  } catch {
    return undefined;
  }
}

function copyOf<T>(bytes: Uint8Array): T {
  return BSON.deserialize(bytes).value as T;
}

export function readThrough<T>(
  target: ReadTarget,
  operation: string,
  args: readonly unknown[],
  session: unknown,
  load: () => Promise<T>,
): Promise<T> {
  const state = requestState.get();
  const reads = state?.reads;
  if (!state || !reads || session !== undefined) return load();
  const key = readKey(target, operation, args);
  if (key === undefined) return load();
  let entry = reads.get(key);
  if (entry) {
    state.stats.reused++;
  } else {
    state.stats.loaded++;
    const loading = load().then((value) => BSON.serialize({ value }));
    entry = loading;
    reads.set(key, loading);
    loading.catch(() => {
      if (reads.get(key) === loading) reads.delete(key);
    });
  }
  return entry.then((bytes) => copyOf<T>(bytes));
}

export function findOneThrough<TDoc extends m.Document>(
  collection: m.Collection<TDoc>,
  query: m.Filter<TDoc>,
  options: object | undefined,
  driverOptions: m.FindOptions & { session?: m.ClientSession },
): Promise<m.WithId<TDoc> | null> {
  return readThrough(
    collection,
    "findOne",
    [query, options],
    driverOptions.session,
    () => collection.findOne(query, driverOptions),
  );
}

export function findThrough<TDoc extends m.Document>(
  collection: m.Collection<TDoc>,
  query: m.Filter<TDoc>,
  options: object | undefined,
  driverOptions: m.FindOptions & { session?: m.ClientSession },
): Promise<m.WithId<TDoc>[]> {
  return readThrough(
    collection,
    "find",
    [query, options],
    driverOptions.session,
    () => collection.find(query, driverOptions).toArray(),
  );
}

function writesThroughPipeline(pipeline: readonly m.Document[]): boolean {
  return pipeline.some((stage) => "$out" in stage || "$merge" in stage);
}

export async function aggregateThrough<
  TDoc extends m.Document,
  R extends m.Document,
>(
  collection: m.Collection<TDoc>,
  pipeline: m.Document[],
  options: object | undefined,
  driverOptions: m.AggregateOptions & { session?: m.ClientSession },
): Promise<R[]> {
  const load = () => collection.aggregate<R>(pipeline, driverOptions).toArray();
  if (!writesThroughPipeline(pipeline)) {
    return await readThrough(
      collection,
      "aggregate",
      [pipeline, options],
      driverOptions.session,
      load,
    );
  }
  invalidateReads();
  try {
    return await load();
  } finally {
    invalidateReads();
  }
}
