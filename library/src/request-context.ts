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
  reads: Map<string, Promise<RawRead | undefined>> | undefined;
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

function keyPart(value: unknown): string {
  switch (typeof value) {
    case "undefined":
      return "n";
    case "string":
      return JSON.stringify(value);
    case "number":
      return `d${Object.is(value, -0) ? "-0" : String(value)}`;
    case "boolean":
      return value ? "T" : "F";
    case "bigint":
      return `i${value}`;
    case "object":
      break;
    default:
      return "n";
  }
  if (value === null) return "n";
  if (Array.isArray(value)) {
    let out = "[";
    for (let index = 0; index < value.length; index++) {
      if (index > 0) out += ",";
      out += keyPart(value[index]);
    }
    return `${out}]`;
  }
  if (value instanceof Date) return `t${value.getTime()}`;
  if ("_bsontype" in value) {
    return value._bsontype === "ObjectId"
      ? `o${(value as BSON.ObjectId).toHexString()}`
      : `x${BSON.EJSON.stringify(value as BSON.Document, { relaxed: false })}`;
  }
  if (value instanceof RegExp) return `r${String(value)}`;
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    return `x${BSON.EJSON.stringify(value as BSON.Document, { relaxed: false })}`;
  }
  let out = "{";
  let first = true;
  for (const name of Object.keys(value)) {
    const entry = (value as Record<string, unknown>)[name];
    if (typeof entry === "function" || typeof entry === "symbol") continue;
    if (!first) out += ",";
    first = false;
    out += `${JSON.stringify(name)}:${keyPart(entry)}`;
  }
  return `${out}}`;
}

function readKey(
  target: ReadTarget,
  operation: string,
  args: readonly unknown[],
): string | undefined {
  try {
    return `${target.dbName}.${target.collectionName}\u0000${operation}\u0000${keyPart(
      [currentReadPreference()?.toJSON() ?? null, ...args],
    )}`;
  } catch {
    return undefined;
  }
}

type RawRead = Uint8Array | Uint8Array[] | null;

type BsonSource = { readonly bsonOptions: m.BSONSerializeOptions };

interface MemoizedRead<T> {
  readonly load: () => Promise<T>;
  readonly loadRaw: () => Promise<RawRead>;
  readonly decode: (raw: RawRead) => T;
  readonly loaded?: (value: T) => void;
}

function memoizable(raw: RawRead): boolean {
  return !Array.isArray(raw) || raw.length <= MEMOIZED_ROWS_LIMIT;
}

async function readThrough<T>(
  target: ReadTarget,
  operation: string,
  args: readonly unknown[],
  session: unknown,
  read: MemoizedRead<T>,
): Promise<T> {
  const state = requestState.get();
  const reads = state?.reads;
  if (!state || !reads || session !== undefined) return await read.load();
  const key = readKey(target, operation, args);
  if (key === undefined) return await read.load();
  const shared = reads.get(key);
  if (shared) {
    const raw = await shared;
    if (raw !== undefined) {
      state.stats.reused++;
      return read.decode(raw);
    }
    state.stats.loaded++;
    return await read.load();
  }
  state.stats.loaded++;
  const loading = read.loadRaw();
  const entry = loading.then((raw) => (memoizable(raw) ? raw : undefined));
  reads.set(key, entry);
  const forget = () => {
    if (reads.get(key) === entry) reads.delete(key);
  };
  entry.then((raw) => {
    if (raw === undefined) forget();
  }, forget);
  const value = read.decode(await loading);
  read.loaded?.(value);
  return value;
}

function decodeOptions(
  collection: BsonSource,
  options: m.BSONSerializeOptions,
): BSON.DeserializeOptions {
  const parent = collection.bsonOptions;
  return {
    useBigInt64: options.useBigInt64 ?? parent.useBigInt64,
    promoteLongs: options.promoteLongs ?? parent.promoteLongs,
    promoteValues: options.promoteValues ?? parent.promoteValues,
    promoteBuffers: options.promoteBuffers ?? parent.promoteBuffers,
    bsonRegExp: options.bsonRegExp ?? parent.bsonRegExp,
    fieldsAsRaw: options.fieldsAsRaw ?? parent.fieldsAsRaw,
    validation: {
      utf8:
        (options.enableUtf8Validation ?? parent.enableUtf8Validation) !== false,
    },
  };
}

function documentRead<T>(
  collection: BsonSource,
  driverOptions: m.BSONSerializeOptions,
  load: () => Promise<T>,
  loadRaw: () => Promise<RawRead>,
  loaded?: (value: T) => void,
): MemoizedRead<T> {
  let options: BSON.DeserializeOptions | undefined;
  const decodeOne = (bytes: Uint8Array) =>
    BSON.deserialize(
      bytes,
      (options ??= decodeOptions(collection, driverOptions)),
    );
  return {
    load,
    loadRaw,
    decode: (raw) =>
      (raw === null
        ? null
        : Array.isArray(raw)
          ? raw.map(decodeOne)
          : decodeOne(raw)) as T,
    loaded,
  };
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

function recorderOf(
  collection: RecordedSource,
  driverOptions: m.FindOptions & { session?: m.ClientSession },
): ((value: unknown) => void) | undefined {
  const recorder = readRecorder.get();
  if (!recorder) return undefined;
  return (value) => {
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
  };
}

function recorded<T>(
  load: () => Promise<T>,
  record: ((value: unknown) => void) | undefined,
): () => Promise<T> {
  if (!record) return load;
  return async () => {
    const value = await load();
    record(value);
    return value;
  };
}

export function findOneThrough<TDoc extends m.Document>(
  collection: m.Collection<TDoc>,
  query: m.Filter<TDoc>,
  options: object | undefined,
  driverOptions: m.FindOptions & { session?: m.ClientSession },
): Promise<m.WithId<TDoc> | null> {
  const record = recorderOf(collection, driverOptions);
  const load = recorded(() => collection.findOne(query, driverOptions), record);
  if (driverOptions.raw === true) return load();
  return readThrough(
    collection,
    "findOne",
    [query, options],
    driverOptions.session,
    documentRead(
      collection,
      driverOptions,
      load,
      () =>
        collection.findOne(query, {
          ...driverOptions,
          raw: true,
        }) as Promise<RawRead>,
      record,
    ),
  );
}

export function findThrough<TDoc extends m.Document>(
  collection: m.Collection<TDoc>,
  query: m.Filter<TDoc>,
  options: object | undefined,
  driverOptions: m.FindOptions & { session?: m.ClientSession },
): Promise<m.WithId<TDoc>[]> {
  const record = recorderOf(collection, driverOptions);
  const load = recorded(
    () => collection.find(query, driverOptions).toArray(),
    record,
  );
  if (driverOptions.raw === true) return load();
  return readThrough(
    collection,
    "find",
    [query, options],
    driverOptions.session,
    documentRead(
      collection,
      driverOptions,
      load,
      () =>
        collection
          .find(query, { ...driverOptions, raw: true })
          .toArray() as unknown as Promise<RawRead>,
      record,
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
    if (driverOptions.raw === true) return await load();
    return await readThrough(
      collection,
      "aggregate",
      [pipeline, options],
      driverOptions.session,
      documentRead(
        collection,
        driverOptions,
        load,
        () =>
          collection
            .aggregate(pipeline, { ...driverOptions, raw: true })
            .toArray() as unknown as Promise<RawRead>,
      ),
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
