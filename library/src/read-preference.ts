/**
 * Read preference rules shared by every collection kind.
 *
 * - A transaction always runs on the primary: the driver rejects any read
 *   inside a transaction whose read preference is not `primary`, so the
 *   per-call options must never carry one there.
 * - Internal metadata reads (migration history, multi-collection registry,
 *   index and validator sync) always read the primary: they precede a write
 *   and must not see a lagging secondary, whatever the client default is.
 * - Everything else follows the collection, then the client, default.
 *
 * @module
 */

import * as m from "mongodb";
import type { ClientSession, Db } from "./mongodb.ts";
import { contextVariable } from "./context-variable.ts";
import { isRecord } from "./utils/guards.ts";

/** The primary read preference. */
export const PRIMARY: m.ReadPreference = m.ReadPreference.primary;

/**
 * A read preference as mongodbee accepts it: a mode name, a driver
 * `ReadPreference`, or the plain `{ mode, maxStalenessSeconds, tags }` object
 * the driver's own typings reject but its runtime understands.
 */
export type ReadPreferenceInput =
  | m.ReadPreferenceLike
  | {
      readonly mode: m.ReadPreferenceMode;
      readonly maxStalenessSeconds?: number;
      readonly tags?: m.TagSet[];
    };

/** The driver `ReadPreference` for any accepted form. */
export function toReadPreference(input: ReadPreferenceInput): m.ReadPreference {
  if (input instanceof m.ReadPreference) return input;
  if (typeof input === "string") return new m.ReadPreference(input);
  return new m.ReadPreference(input.mode, input.tags, {
    maxStalenessSeconds: input.maxStalenessSeconds,
  });
}

function driverReadOptions<
  T extends { readonly readPreference?: ReadPreferenceInput },
>(
  options: T,
): Omit<T, "readPreference"> & { readPreference?: m.ReadPreference } {
  const { readPreference, ...rest } = options;
  return readPreference === undefined
    ? rest
    : { ...rest, readPreference: toReadPreference(readPreference) };
}

const ambientReadPreference = contextVariable<m.ReadPreference>(
  "mongodbee.readPreference",
);

export function withReadPreference<T>(
  preference: ReadPreferenceInput,
  fn: () => T,
): T {
  return ambientReadPreference.run(toReadPreference(preference), fn);
}

export function currentReadPreference(): m.ReadPreference | undefined {
  return ambientReadPreference.get();
}

const READ_OPTIONS_POSITION: Readonly<Record<string, number>> = {
  find: 1,
  findOne: 1,
  aggregate: 1,
  countDocuments: 1,
  distinct: 2,
  estimatedDocumentCount: 0,
};

function inTransaction(options: Record<string, unknown>): boolean {
  const session = options.session;
  return session instanceof m.ClientSession && session.inTransaction();
}

function withAmbientPreference(
  args: readonly unknown[],
  position: number,
): unknown[] {
  const preference = ambientReadPreference.get();
  if (preference === undefined) return [...args];
  const given = args[position];
  const options = isRecord(given) ? given : {};
  if (options.readPreference !== undefined || inTransaction(options)) {
    return [...args];
  }
  const next = Array.from(
    { length: Math.max(args.length, position + 1) },
    (_, index) => args[index],
  );
  next[position] = { ...options, readPreference: preference };
  return next;
}

function ambientReads<T extends m.Document>(
  target: m.Collection<T>,
): m.Collection<T> {
  return new Proxy(target, {
    get(object, property, receiver) {
      const value = Reflect.get(object, property, receiver);
      if (typeof property !== "string" || typeof value !== "function") {
        return value;
      }
      const position = READ_OPTIONS_POSITION[property];
      if (position === undefined) return value;
      return (...args: unknown[]) =>
        value.apply(object, withAmbientPreference(args, position));
    },
  });
}

export function readingCollection<
  T extends m.Document = m.Document,
  O extends {
    readonly readPreference?: ReadPreferenceInput;
  } = DriverCollectionOptions,
>(db: Db | m.Db, name: string, options?: O): m.Collection<T> {
  return ambientReads(
    db.collection<T>(name, options ? driverReadOptions(options) : {}),
  );
}

/** Driver options `O` whose `readPreference` accepts every mongodbee form. */
export type WithReadPreferenceInput<O> = Omit<O, "readPreference"> & {
  readPreference?: ReadPreferenceInput;
};

/** Collection options of the driver, with a mongodbee read preference. */
export type DriverCollectionOptions = Omit<
  m.CollectionOptions,
  "readPreference"
> & {
  readPreference?: ReadPreferenceInput;
};

/** Per-call read options accepted by the methods that take no driver options. */
export type ReadOptions = {
  /**
   * Overrides the collection's read preference for this call, e.g.
   * `"primary"` to read your own writes on a collection reading secondaries.
   * Ignored inside a transaction, which always reads the primary.
   */
  readPreference?: ReadPreferenceInput;
};

/**
 * The driver options of a read: the ambient session merged with the caller's
 * options. Inside a transaction, the read preference and concerns are dropped
 * so the transaction's own (primary) ones apply.
 */
export type ResolvedReadOptions<T> = Partial<
  Omit<T, "readPreference" | "readConcern" | "writeConcern">
> & {
  readPreference?: m.ReadPreference;
  readConcern?: T[keyof T & "readConcern"];
  writeConcern?: T[keyof T & "writeConcern"];
  session: ClientSession | undefined;
};

export type SessionOnly = { session: ClientSession | undefined };

export function readOpts<
  T extends {
    readonly readPreference?: ReadPreferenceInput;
    readonly readConcern?: unknown;
    readonly writeConcern?: unknown;
  },
>(
  session: ClientSession | undefined,
  options?: T,
): ResolvedReadOptions<T> | SessionOnly {
  if (options === undefined) return { session };
  const { readPreference, readConcern, writeConcern, ...rest } = options;
  if (session?.inTransaction()) return { ...rest, session };
  return {
    ...rest,
    session,
    ...(readPreference !== undefined && {
      readPreference: toReadPreference(readPreference),
    }),
    ...(readConcern !== undefined && { readConcern }),
    ...(writeConcern !== undefined && { writeConcern }),
  };
}

/** A handle on `name` that always reads the primary. */
export function primaryCollection<T extends m.Document = m.Document>(
  db: Db | m.Db,
  name: string,
): m.Collection<T> {
  return db.collection<T>(name, { readPreference: PRIMARY });
}
