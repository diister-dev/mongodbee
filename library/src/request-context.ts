import { BSON } from "mongodb";
import type * as m from "mongodb";
import { contextVariable } from "./context-variable.ts";
import { currentReadPreference } from "./read-preference.ts";
import type { Db } from "./mongodb.ts";

export type DatabaseSource = Db | (() => Db | undefined);

export interface RequestContextOptions {
  memoizeReads?: boolean;
  database?: DatabaseSource;
}

export interface RequestScope {
  readonly database: DatabaseSource | undefined;
  readonly parent: RequestScope | undefined;
}

export interface RequestReadStats {
  loaded: number;
  reused: number;
  invalidations: number;
}

const MEMOIZED_ROWS_LIMIT = 100;

interface RequestState extends RequestScope {
  reads: Map<string, Promise<Uint8Array | undefined>> | undefined;
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

const mongodbeeWrite = contextVariable<true>("mongodbee.write");

const driverWriteListeners = new Set<() => void>();

export function onUntrackedWrite(listener: () => void): void {
  driverWriteListeners.add(listener);
}

export function asMongodbeeWrite<T>(fn: () => T): T {
  return mongodbeeWrite.run(true, fn);
}

export function currentRequestScope(): RequestScope | undefined {
  return requestState.get();
}

export function invalidateReadsOnDriverWrites(
  client: m.MongoClient,
): () => void {
  if (client.options.monitorCommands !== true) {
    throw new Error(
      "invalidateReadsOnDriverWrites needs a client created with monitorCommands: true",
    );
  }
  const onCommand = (event: { commandName: string }) => {
    if (!DRIVER_WRITE_COMMANDS.has(event.commandName)) return;
    invalidateReads();
    if (!mongodbeeWrite.get()) notifyUntrackedWrite();
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
      database: options.database,
      parent: requestState.get(),
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
      [currentReadPreference()?.toJSON() ?? null, ...args],
      { relaxed: false },
    )}`;
  } catch {
    return undefined;
  }
}

function copyOf<T>(bytes: Uint8Array): T {
  return BSON.deserialize(bytes).value as T;
}

function memoizable(value: unknown): boolean {
  return !Array.isArray(value) || value.length <= MEMOIZED_ROWS_LIMIT;
}

export async function readThrough<T>(
  target: ReadTarget,
  operation: string,
  args: readonly unknown[],
  session: unknown,
  load: () => Promise<T>,
): Promise<T> {
  const state = requestState.get();
  const reads = state?.reads;
  if (!state || !reads || session !== undefined) return await load();
  const key = readKey(target, operation, args);
  if (key === undefined) return await load();
  const shared = reads.get(key);
  if (shared) {
    const bytes = await shared;
    if (bytes !== undefined) {
      state.stats.reused++;
      return copyOf<T>(bytes);
    }
    state.stats.loaded++;
    return await load();
  }
  state.stats.loaded++;
  const loading = load();
  const entry = loading.then((value) =>
    memoizable(value) ? BSON.serialize({ value }) : undefined,
  );
  reads.set(key, entry);
  const forget = () => {
    if (reads.get(key) === entry) reads.delete(key);
  };
  entry.then((bytes) => {
    if (bytes === undefined) forget();
  }, forget);
  return await loading;
}

export interface RecordedRead {
  readonly dbName: string;
  readonly collectionName: string;
  readonly projected: boolean;
  readonly primary: boolean;
  readonly inTransaction: boolean;
  readonly documents: readonly m.Document[];
}

const readRecorder = contextVariable<(read: RecordedRead) => void>(
  "mongodbee.readRecorder",
);

export function recordingReads<T>(
  recorder: (read: RecordedRead) => void,
  fn: () => T,
): T {
  return readRecorder.run(recorder, fn);
}

interface RecordedSource extends ReadTarget {
  readonly dbName: string;
  readonly readPreference?: m.ReadPreference;
}

function recorded<T>(
  collection: RecordedSource,
  driverOptions: m.FindOptions & { session?: m.ClientSession },
  load: () => Promise<T>,
): () => Promise<T> {
  const recorder = readRecorder.get();
  if (!recorder) return load;
  return async () => {
    const value = await load();
    const preference =
      driverOptions.readPreference ??
      currentReadPreference() ??
      collection.readPreference;
    recorder({
      dbName: collection.dbName,
      collectionName: collection.collectionName,
      projected: driverOptions.projection !== undefined,
      primary:
        preference === undefined ||
        (typeof preference === "string" ? preference : preference.mode) ===
          "primary",
      inTransaction: driverOptions.session?.inTransaction() === true,
      documents: Array.isArray(value)
        ? value
        : value === null || value === undefined
          ? []
          : [value as m.Document],
    });
    return value;
  };
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
    recorded(collection, driverOptions, () =>
      collection.findOne(query, driverOptions),
    ),
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
    recorded(collection, driverOptions, () =>
      collection.find(query, driverOptions).toArray(),
    ),
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
  notifyUntrackedWrite();
  try {
    return await load();
  } finally {
    invalidateReads();
    notifyUntrackedWrite();
  }
}

export function notifyUntrackedWrite(): void {
  for (const listener of driverWriteListeners) listener();
}
