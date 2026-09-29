import type { ClientSession, Document } from "mongodb";
import type { Db, MongoClient } from "./mongodb.ts";
import {
  currentRequestScope,
  type DatabaseSource,
  type RecordedRead,
  recordingReads,
} from "./request-context.ts";
import { getSessionContext } from "./session.ts";
import {
  type ComputedLocation,
  type ComputedTopology,
  leadingIndexes,
} from "./computed-topology.ts";
import {
  type DeepReadonly,
  type Plain,
  type ReaderArgument,
  ReaderQuery,
  type ReaderQueryDescriptor,
  type ScopeArgs,
} from "./reader-query.ts";
import {
  type EntryFootprint,
  forgetEntry,
  linkEntries,
  type ReaderCache,
  type ReaderEntry,
  requestReaderCache,
} from "./reader-cache.ts";
import {
  compositeIsLoading,
  currentComposite,
  recordDependency,
  runInComposite,
} from "./reader-frame.ts";
import {
  ReaderArgumentError,
  ReaderDatabaseError,
  ReaderDefinitionError,
  ReaderNotRegisteredError,
} from "./reader-errors.ts";
import { freezeCopy, freezeOwned } from "./reader-freeze.ts";
import { cacheKey, encodeKey } from "./reader-key.ts";
import {
  loadRows,
  matchesWhere,
  parseScope,
  selectedRow,
  shaped,
  valueAt,
} from "./reader-load.ts";
import { ClientRegistry, isDb } from "./client-registry.ts";
import {
  createReaderTracer,
  type ReaderSpan,
  type ReaderTracer,
  TELEMETRY_ATTRIBUTES,
  type TelemetryOptions,
} from "./telemetry.ts";

export {
  type DeepReadonly,
  type FrozenDate,
  type Plain,
  type ReaderArgument,
  ReaderQuery,
  type ReaderQueryDescriptor,
} from "./reader-query.ts";
export { ReaderDirectReadError } from "./reader-frame.ts";
export {
  ReaderArgumentError,
  ReaderDatabaseError,
  ReaderDefinitionError,
  ReaderNotRegisteredError,
} from "./reader-errors.ts";
export {
  invalidateAllReaders,
  type ReaderStats,
  requestReaderStats,
} from "./reader-cache.ts";

interface ReaderBase {
  readonly readerName: string;
}

export type QueryReader<A extends readonly unknown[], V, S, K> = ((
  ...args: A
) => Promise<V>) &
  ReaderBase & {
    readonly query: ReaderQueryDescriptor;
    readonly many: [K] extends [never]
      ? never
      : (
          ...args: [...ScopeArgs<S>, keys: readonly K[]]
        ) => Promise<ReadonlyMap<K, V>>;
    readonly primeFrom: [K] extends [never]
      ? null extends V
        ? <R>(read: () => Promise<R>) => Promise<R>
        : never
      : never;
  };

export type CompositeReader<A extends readonly unknown[], V> = ((
  ...args: A
) => Promise<V>) &
  ReaderBase;

export type AnyReader =
  | QueryReader<never, unknown, unknown, unknown>
  | CompositeReader<never, unknown>;

export interface ReaderLimits {
  readonly entriesPerRequest?: number;
  readonly rowsPerEntry?: number;
}

export const DEFAULT_READER_LIMITS: Required<ReaderLimits> = Object.freeze({
  entriesPerRequest: 500,
  rowsPerEntry: 200,
});

export interface ReaderRegistrationOptions {
  readonly topology: ComputedTopology;
  readonly readers: readonly AnyReader[];
  readonly limits?: ReaderLimits;
  readonly database?: () => Db | undefined;
  readonly telemetry?: TelemetryOptions;
}

interface Placement {
  readonly location: ComputedLocation;
  readonly paths: readonly string[];
}

interface Registration {
  readonly client: MongoClient;
  readonly db: Db | undefined;
  readonly limits: Required<ReaderLimits>;
  readonly placements: ReadonlyMap<ReaderQueryDescriptor, Placement>;
  readonly database: (() => Db | undefined) | undefined;
  readonly tracer: ReaderTracer | null;
}

const registrations = new ClientRegistry<Registration>();
const registered = new Set<Registration>();

let nextReaderIdentity = 0;

function isQueryReader(
  value: AnyReader,
): value is QueryReader<never, unknown, unknown, unknown> {
  return "query" in value;
}

function place(
  topology: ComputedTopology,
  reader: string,
  query: ReaderQueryDescriptor,
): Placement & { readonly input: unknown } {
  let placed: ReturnType<ComputedTopology["place"]>;
  try {
    placed = topology.place(query.source.type);
  } catch (error) {
    throw new ReaderDefinitionError(
      `reader "${reader}": ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  const scopedLocation = placed.location.kind === "scoped";
  if (scopedLocation && !query.scope) {
    throw new ReaderDefinitionError(
      `reader "${reader}": "${query.source.type}" lives in the scoped collection "${placed.location.collection}"; declare its source with from(scoped(Model, scopeSchema), "${query.source.type}") so the reader takes the scope as its first argument`,
    );
  }
  if (!scopedLocation && query.scope) {
    throw new ReaderDefinitionError(
      `reader "${reader}": "${query.source.type}" lives in "${placed.location.collection}", which has no scope; declare its source without scoped()`,
    );
  }
  return {
    location: placed.location,
    input: placed.input,
    paths: Object.freeze([
      "_id",
      "_type",
      "_scope",
      ...(query.by === undefined ? [] : [query.by]),
      ...Object.keys(query.where),
      ...query.select,
    ]),
  };
}

function assertIndexed(
  reader: string,
  query: ReaderQueryDescriptor,
  input: unknown,
): void {
  const by = query.by;
  if (by === undefined || by === "_id") return;
  if (leadingIndexes(input).some((index) => index.path === by)) return;
  throw new ReaderDefinitionError(
    `reader "${reader}" reads "${query.source.type}" by "${by}", and no declared index leads with "${by}"; declare withIndex() on it or a composite index leading with it, or every call scans the ${query.scope ? "scope" : "collection"}`,
  );
}

function removeRegistered(
  client: MongoClient,
  matches: (registration: Registration) => boolean,
): void {
  for (const registration of registered) {
    if (registration.client === client && matches(registration))
      registered.delete(registration);
  }
}

export function registerReaders(
  target: Db | MongoClient,
  options: ReaderRegistrationOptions,
): void {
  const names = new Set<string>();
  const placements = new Map<ReaderQueryDescriptor, Placement>();
  for (const declared of options.readers) {
    if (names.has(declared.readerName)) {
      throw new ReaderDefinitionError(
        `two readers are named "${declared.readerName}"; a reader name identifies it in metrics and logs, so it must be unique`,
      );
    }
    names.add(declared.readerName);
    if (!isQueryReader(declared)) continue;
    const placed = place(options.topology, declared.readerName, declared.query);
    assertIndexed(declared.readerName, declared.query, placed.input);
    placements.set(declared.query, {
      location: placed.location,
      paths: placed.paths,
    });
  }
  const db = isDb(target) ? target : undefined;
  const client = db ? db.client : (target as MongoClient);
  removeRegistered(client, (registration) =>
    db ? registration.db?.databaseName === db.databaseName : !registration.db,
  );
  const registration: Registration = {
    client,
    db,
    limits: { ...DEFAULT_READER_LIMITS, ...options.limits },
    placements,
    database: options.database,
    tracer: createReaderTracer(options.telemetry),
  };
  registrations.set(target, registration);
  registered.add(registration);
}

export function unregisterReaders(target: Db | MongoClient): void {
  if (isDb(target)) {
    registrations.delete(target);
    removeRegistered(
      target.client,
      (registration) => registration.db?.databaseName === target.databaseName,
    );
    return;
  }
  registrations.deleteClient(target);
  removeRegistered(target, () => true);
}

function ambientDatabase(): Db | undefined {
  const scope = currentRequestScope();
  for (let current = scope; current; current = current.parent) {
    if (current.database !== undefined) return fromSource(current.database);
  }
  const resolvers = [
    ...new Set(
      [...registered].flatMap((registration) =>
        registration.database ? [registration.database] : [],
      ),
    ),
  ];
  if (resolvers.length === 1) return resolvers[0]!();
  if (resolvers.length > 1 || scope) return undefined;
  const databases = [...registered].flatMap((registration) =>
    registration.db ? [registration.db] : [],
  );
  return databases.length === 1 ? databases[0] : undefined;
}

function fromSource(source: DatabaseSource): Db | undefined {
  return typeof source === "function" ? source() : source;
}

function resolveDatabase(reader: string): Db {
  const database = ambientDatabase();
  if (database) return database;
  throw new ReaderDatabaseError(
    `reader "${reader}" has no database: open withRequestContext(fn, { database }) or give registerReaders(client, { database }) a resolver`,
  );
}

interface CallContext {
  readonly db: Db;
  readonly limits: Required<ReaderLimits>;
  readonly session: ClientSession | undefined;
  readonly cache: ReaderCache | undefined;
  readonly tracer: ReaderTracer | null;
}

function callContext(
  db: Db,
  registration: Registration | undefined,
): CallContext {
  const session = getSessionContext(db.client).getSession();
  const inTransaction = session?.inTransaction() === true;
  return {
    db,
    limits: registration?.limits ?? DEFAULT_READER_LIMITS,
    session: inTransaction ? session : undefined,
    cache: inTransaction ? undefined : requestReaderCache(),
    tracer: registration?.tracer ?? null,
  };
}

const A = TELEMETRY_ATTRIBUTES;

function observed<T>(
  context: CallContext | undefined,
  reader: string,
  kind: "query" | "composite",
  collection: string | undefined,
  run: (span: ReaderSpan | undefined) => Promise<T>,
): Promise<T> {
  const tracer = context?.tracer;
  if (!context || !tracer) return run(undefined);
  return tracer(
    reader,
    {
      [A.READER_KIND]: kind,
      [A.READER_LEVEL]: context.cache ? "request" : "none",
      [A.DB_NAMESPACE]: context.db.databaseName,
      [A.COLLECTION_NAME]: collection,
    },
    run,
  );
}

function sizeOf(value: unknown): number {
  return Array.isArray(value) ? value.length : 1;
}

function startLoad<T>(load: () => Promise<T>): Promise<T> {
  try {
    return Promise.resolve(load());
  } catch (error) {
    return Promise.reject(error);
  }
}

interface NewEntry {
  readonly entry: ReaderEntry;
  readonly settle: (value: Promise<unknown>) => void;
}

function newEntry(
  cache: ReaderCache,
  db: Db,
  key: string,
  footprint: EntryFootprint | undefined,
): NewEntry {
  const { promise, resolve } = Promise.withResolvers<unknown>();
  return {
    entry: {
      key,
      owner: cache,
      database: db.databaseName,
      footprint,
      dependents: new Set(),
      dependencies: new Set(),
      promise,
      stale: false,
    },
    settle: resolve,
  };
}

interface CachedCall {
  readonly context: CallContext;
  readonly cache: ReaderCache;
  readonly key: string;
  readonly footprint: EntryFootprint | undefined;
  readonly freeze: (value: unknown) => unknown;
  readonly span?: ReaderSpan;
  readonly load: (entry: ReaderEntry | undefined) => Promise<unknown>;
}

function cached(call: CachedCall): Promise<unknown> {
  const { context, cache, key, footprint, freeze, span, load } = call;
  const existing = cache.entries.get(key);
  if (existing) {
    cache.stats.hits++;
    span?.setAttributes({ [A.READER_OUTCOME]: "hit" });
    recordDependency(existing);
    return existing.promise;
  }
  if (cache.entries.size >= context.limits.entriesPerRequest) {
    cache.stats.bypasses++;
    span?.setAttributes({ [A.READER_OUTCOME]: "bypass" });
    recordDependency(undefined);
    return startLoad(() => load(undefined)).then(freeze);
  }
  cache.stats.loads++;
  span?.setAttributes({ [A.READER_OUTCOME]: "load" });
  const { entry, settle } = newEntry(cache, context.db, key, footprint);
  cache.entries.set(key, entry);
  recordDependency(entry);
  settle(
    startLoad(() => load(entry)).then(
      (value) => {
        const frozen = freeze(value);
        if (entry.stale) {
          cache.stats.discarded++;
          span?.setAttributes({ [A.READER_DISCARDED]: true });
        } else if (sizeOf(value) > context.limits.rowsPerEntry) {
          cache.stats.bypasses++;
          span?.setAttributes({ [A.READER_OUTCOME]: "bypass" });
          forgetEntry(entry);
        }
        return frozen;
      },
      (error: unknown) => {
        forgetEntry(entry);
        throw error;
      },
    ),
  );
  return entry.promise;
}

function assertName(name: string): void {
  if (!/^[a-z0-9][a-z0-9.-]*$/.test(name)) {
    throw new ReaderDefinitionError(
      `reader name "${name}" must be lowercase letters, digits, dots and dashes`,
    );
  }
}

function queryReader(
  name: string,
  query: ReaderQueryDescriptor,
): QueryReader<never, unknown, unknown, unknown> {
  const identity = nextReaderIdentity++;

  const locate = (db: Db): { context: CallContext; placement: Placement } => {
    const registration = registrations.get(db);
    const placement = registration?.placements.get(query);
    if (!registration || !placement) {
      throw new ReaderNotRegisteredError(
        `reader "${name}" is not listed in registerReaders(db, { readers }) for database "${db.databaseName}"; the registration is where its placement and index are checked`,
      );
    }
    return { context: callContext(db, registration), placement };
  };

  const footprintOf = (
    placement: Placement,
    scope: unknown,
  ): EntryFootprint => ({
    collection: placement.location.collection,
    type:
      placement.location.kind === "collection"
        ? undefined
        : placement.location.type,
    scope: typeof scope === "string" ? scope : undefined,
    paths: placement.paths,
  });

  const splitArgs = (args: readonly unknown[]) =>
    query.scope
      ? { scope: parseScope(name, query, args[0]), rest: args.slice(1) }
      : { scope: undefined, rest: [...args] };

  const keyFor = (db: Db, scope: unknown, keys: readonly unknown[]) =>
    cacheKey(db, identity, name, [...(query.scope ? [scope] : []), ...keys]);

  const call = async (...args: unknown[]): Promise<unknown> => {
    const db = resolveDatabase(name);
    const { context, placement } = locate(db);
    const { scope, rest } = splitArgs(args);
    const keys = query.by === undefined ? undefined : [rest[0]];
    const load = async () =>
      shaped(
        query,
        (
          await loadRows(
            db,
            context.session,
            query,
            placement.location,
            scope,
            keys,
          )
        ).map((document) => selectedRow(query, document)),
      );
    return await observed(
      context,
      name,
      "query",
      placement.location.collection,
      async (span) => {
        const cache = context.cache;
        if (!cache) {
          span?.setAttributes({ [A.READER_OUTCOME]: "load" });
          recordDependency(undefined);
          return freezeOwned(await load());
        }
        return await cached({
          context,
          cache,
          key: keyFor(db, scope, keys ?? []),
          footprint: footprintOf(placement, scope),
          freeze: freezeOwned,
          span,
          load,
        });
      },
    );
  };

  const many = async (
    ...args: unknown[]
  ): Promise<ReadonlyMap<unknown, unknown>> => {
    const by = query.by;
    if (by === undefined) {
      throw new ReaderDefinitionError(
        `reader "${name}" has no by(), so it has no keys to read many of`,
      );
    }
    const db = resolveDatabase(name);
    const { context, placement } = locate(db);
    const { scope, rest } = splitArgs(args);
    const requested = rest[0];
    if (!Array.isArray(requested)) {
      throw new ReaderArgumentError(
        `reader "${name}".many() takes an array of keys`,
      );
    }
    const keys = [
      ...new Map(
        requested.map((key) => [encodeKey(key, name), key] as const),
      ).entries(),
    ];
    const loadGrouped = async (
      wanted: readonly unknown[],
    ): Promise<Map<string, Document[]>> => {
      const grouped = new Map<string, Document[]>();
      if (wanted.length === 0) return grouped;
      const documents = await loadRows(
        db,
        context.session,
        query,
        placement.location,
        scope,
        wanted,
      );
      const accepted = new Set(wanted.map((key) => encodeKey(key, name)));
      for (const document of documents) {
        const raw = valueAt(document, by);
        const values: readonly unknown[] = Array.isArray(raw) ? raw : [raw];
        const row = selectedRow(query, document);
        for (const value of values) {
          const text = encodeKey(value, name);
          if (!accepted.has(text)) continue;
          grouped.set(text, [...(grouped.get(text) ?? []), row]);
        }
      }
      return grouped;
    };
    return await observed(
      context,
      name,
      "query",
      placement.location.collection,
      async (span) => {
        span?.setAttributes({ [A.READER_KEYS]: keys.length });
        const cache = context.cache;
        const result = new Map<unknown, unknown>();
        const missing = cache
          ? keys.filter(
              ([, key]) => !cache.entries.has(keyFor(db, scope, [key])),
            )
          : keys;
        if (
          !cache ||
          cache.entries.size + missing.length > context.limits.entriesPerRequest
        ) {
          if (cache) cache.stats.bypasses++;
          span?.setAttributes({
            [A.READER_OUTCOME]: cache ? "bypass" : "load",
          });
          recordDependency(undefined);
          const grouped = await loadGrouped(keys.map(([, key]) => key));
          for (const [text, key] of keys)
            result.set(
              key,
              freezeOwned(shaped(query, grouped.get(text) ?? [])),
            );
          return Object.freeze(result);
        }
        span?.setAttributes({
          [A.READER_OUTCOME]: missing.length === 0 ? "hit" : "load",
        });
        let shared: Promise<Map<string, Document[]>> | undefined;
        const grouped = () => {
          shared ??= loadGrouped(missing.map(([, key]) => key));
          return shared;
        };
        const values = await Promise.all(
          keys.map(([text, key]) =>
            cached({
              context,
              cache,
              key: keyFor(db, scope, [key]),
              footprint: footprintOf(placement, scope),
              freeze: freezeOwned,
              load: async () =>
                shaped(query, (await grouped()).get(text) ?? []),
            }),
          ),
        );
        for (const [index, [, key]] of keys.entries())
          result.set(key, values[index]);
        return Object.freeze(result);
      },
    );
  };

  const primeFrom = async <R>(read: () => Promise<R>): Promise<R> => {
    if (!query.one || query.by !== undefined) {
      throw new ReaderDefinitionError(
        `reader "${name}" is not a singleton: primeFrom primes readers declared with .one() and no by(), whose value is the one document of its scope`,
      );
    }
    const db = resolveDatabase(name);
    const { context, placement } = locate(db);
    const cache = context.cache;
    if (!cache) return await read();
    const writesBefore = cache.writes;
    const reads: RecordedRead[] = [];
    const result = await recordingReads(
      (recorded) => reads.push(recorded),
      read,
    );
    if (cache.writes !== writesBefore) return result;
    const location = placement.location;
    for (const recorded of reads) {
      if (
        recorded.dbName !== db.databaseName ||
        recorded.collectionName !== location.collection ||
        recorded.projected ||
        !recorded.primary ||
        recorded.inTransaction
      )
        continue;
      for (const document of recorded.documents) {
        if (location.kind !== "collection" && document._type !== location.type)
          continue;
        if (!matchesWhere(query, document)) continue;
        const scope: unknown = query.scope ? document._scope : undefined;
        if (query.scope && typeof scope !== "string") continue;
        const key = keyFor(db, scope, []);
        if (
          cache.entries.has(key) ||
          cache.entries.size >= context.limits.entriesPerRequest
        )
          continue;
        const { entry, settle } = newEntry(
          cache,
          db,
          key,
          footprintOf(placement, scope),
        );
        settle(Promise.resolve(freezeCopy(selectedRow(query, document))));
        cache.entries.set(key, entry);
        cache.stats.primed++;
      }
    }
    return result;
  };

  return Object.assign(call, {
    readerName: name,
    query,
    many,
    primeFrom,
  }) as unknown as QueryReader<never, unknown, unknown, unknown>;
}

function compositeReader(
  name: string,
  load: (...args: never[]) => Promise<unknown>,
): CompositeReader<never, unknown> {
  const identity = nextReaderIdentity++;
  const call = async (...args: unknown[]): Promise<unknown> => {
    const callKey = `${identity}\u0000${encodeKey(args, name)}`;
    if (compositeIsLoading(callKey)) {
      throw new ReaderDefinitionError(
        `reader "${name}" calls itself with the same arguments while loading`,
      );
    }
    const db = ambientDatabase();
    const context = db ? callContext(db, registrations.get(db)) : undefined;
    const run = (entry: ReaderEntry | undefined): Promise<unknown> => {
      let cacheable = true;
      const frame = {
        reader: name,
        call: callKey,
        parent: currentComposite(),
        dependOn: (dependency: ReaderEntry | undefined) => {
          if (!dependency || (entry && !linkEntries(dependency, entry)))
            cacheable = false;
        },
      };
      return startLoad(() =>
        runInComposite(frame, () =>
          (load as (...values: unknown[]) => Promise<unknown>)(...args),
        ),
      ).then((value) => {
        if (entry && !cacheable) forgetEntry(entry);
        return value;
      });
    };
    return await observed(
      context,
      name,
      "composite",
      undefined,
      async (span) => {
        const cache = context?.cache;
        if (!context || !cache) {
          span?.setAttributes({ [A.READER_OUTCOME]: "load" });
          recordDependency(undefined);
          return freezeCopy(await run(undefined));
        }
        return await cached({
          context,
          cache,
          key: cacheKey(context.db, identity, name, args),
          footprint: undefined,
          freeze: freezeCopy,
          span,
          load: run,
        });
      },
    );
  };
  return Object.assign(call, {
    readerName: name,
  }) as unknown as CompositeReader<never, unknown>;
}

export function reader<A extends readonly unknown[], V, S, K>(
  name: string,
  query: ReaderQuery<A, V, S, K>,
): QueryReader<A, V, S, K>;
export function reader<A extends readonly ReaderArgument[], V>(
  name: string,
  load: (...args: A) => Promise<V & Plain<V>>,
): CompositeReader<A, DeepReadonly<V>>;
export function reader(
  name: string,
  definition:
    | ReaderQuery<readonly unknown[], unknown, unknown, unknown>
    | ((...args: never[]) => Promise<unknown>),
): AnyReader {
  assertName(name);
  if (definition instanceof ReaderQuery) {
    return queryReader(name, definition.descriptor);
  }
  if (typeof definition !== "function") {
    throw new ReaderDefinitionError(
      `reader "${name}" takes a query built with from(...).select() or an async function over other readers`,
    );
  }
  return compositeReader(name, definition);
}
