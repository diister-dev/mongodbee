import {
  type AnyBulkWriteOperation,
  type ClientSession,
  type Collection,
  type Document,
  type Filter,
  ObjectId,
} from "mongodb";
import type { Db, MongoClient } from "./mongodb.ts";
import { COMPUTED_ROOT } from "./computed.ts";
import {
  type ComputedField,
  type ComputedTopology,
  locationFilter,
} from "./computed-topology.ts";
import { type ComputedSubject, recomputeSubjects } from "./computed-apply.ts";
import { markFar, markWhole } from "./computed-marks.ts";
import { checkTransactionEnabled, getSessionContext } from "./session.ts";
import { type RetryOptions, retryOnWriteConflict } from "./utils/retry.ts";

export const DEFAULT_INLINE_RECOMPUTE_LIMIT = 1000;

export type ComputedRetryOptions = Pick<
  RetryOptions,
  "maxRetries" | "initialDelay" | "maxDelay" | "jitter"
>;

export const DEFAULT_COMPUTED_RETRY: Readonly<ComputedRetryOptions> =
  Object.freeze({
    maxRetries: 60,
    initialDelay: 2,
    maxDelay: 50,
    jitter: "full",
  });

export interface ComputedRegistrationOptions {
  readonly inlineLimit?: number;
  readonly standaloneMode?: "refuse" | "best-effort";
  readonly retry?: ComputedRetryOptions;
}

interface Registration {
  readonly topology: ComputedTopology;
  readonly inlineLimit: number;
  readonly standaloneMode: "refuse" | "best-effort";
  readonly retry: ComputedRetryOptions;
}

export class ComputedRequiresTransactionError extends Error {
  override readonly name = "ComputedRequiresTransactionError";
}

export class ComputedUnsupportedWriteError extends Error {
  override readonly name = "ComputedUnsupportedWriteError";
}

export class ComputedNotRegisteredError extends Error {
  override readonly name = "ComputedNotRegisteredError";
}

const registrations = new WeakMap<MongoClient, Map<string, Registration>>();

const EVERY_DATABASE = "*";

function isDb(target: Db | MongoClient): target is Db {
  return "databaseName" in target && "client" in target;
}

function registrationKey(target: Db | MongoClient): {
  client: MongoClient;
  name: string;
} {
  return isDb(target)
    ? { client: target.client, name: target.databaseName }
    : { client: target, name: EVERY_DATABASE };
}

export function registerComputed(
  target: Db | MongoClient,
  topology: ComputedTopology,
  options: ComputedRegistrationOptions = {},
): void {
  const { client, name } = registrationKey(target);
  const byName = registrations.get(client) ?? new Map<string, Registration>();
  byName.set(name, {
    topology,
    inlineLimit: options.inlineLimit ?? DEFAULT_INLINE_RECOMPUTE_LIMIT,
    standaloneMode: options.standaloneMode ?? "refuse",
    retry: options.retry ?? DEFAULT_COMPUTED_RETRY,
  });
  registrations.set(client, byName);
}

export function unregisterComputed(target: Db | MongoClient): void {
  const { client, name } = registrationKey(target);
  registrations.get(client)?.delete(name);
}

export function computedRegistration(db: Db): Registration | undefined {
  const byName = registrations.get(db.client);
  return byName?.get(db.databaseName) ?? byName?.get(EVERY_DATABASE);
}

interface Plan {
  readonly registration: Registration;
  readonly near: readonly ComputedField[];
  readonly subjects: readonly ComputedField[];
  readonly far: readonly ComputedField[];
}

function planFor(
  db: Db,
  collectionName: string,
  declaresComputed: boolean,
): Plan | undefined {
  const registration = computedRegistration(db);
  if (!registration) {
    if (declaresComputed) {
      throw new ComputedNotRegisteredError(
        `"${collectionName}" holds types with computed fields but no computed topology is registered for database "${db.databaseName}"; call registerComputed(db, computedTopology(schemas)) at boot`,
      );
    }
    return undefined;
  }
  const near = registration.topology.fields.filter(
    (field) => field.source.collection === collectionName,
  );
  const subjects = registration.topology.fields.filter(
    (field) => field.at.collection === collectionName,
  );
  const far = registration.topology.fields.filter(
    (field) => field.far?.collection === collectionName,
  );
  if (near.length === 0 && subjects.length === 0 && far.length === 0)
    return undefined;
  return { registration, near, subjects, far };
}

type UpdateShape = Document | Document[];

function touchedPaths(
  update: UpdateShape | undefined,
): readonly string[] | "all" {
  if (update === undefined || Array.isArray(update)) return "all";
  const keys = Object.keys(update);
  if (!keys.some((key) => key.startsWith("$"))) return "all";
  const paths: string[] = [];
  for (const [operator, value] of Object.entries(update)) {
    if (value === null || typeof value !== "object") continue;
    for (const [path, target] of Object.entries(
      value as Record<string, unknown>,
    )) {
      paths.push(path.replace(/\.\$(\[[^\]]*\])?/g, ""));
      if (operator === "$rename" && typeof target === "string")
        paths.push(target);
    }
  }
  return paths;
}

function relevantPaths(field: ComputedField): readonly string[] {
  const { descriptor } = field;
  const aggregate = descriptor.aggregate;
  return [
    "_type",
    "_scope",
    descriptor.by,
    ...Object.keys(descriptor.where),
    ...(descriptor.through
      ? [descriptor.through.via]
      : aggregate.kind === "collect"
        ? [aggregate.path]
        : []),
  ];
}

function overlaps(a: string, b: string): boolean {
  return a === b || a.startsWith(`${b}.`) || b.startsWith(`${a}.`);
}

function farRelevantPaths(field: ComputedField): readonly string[] {
  const { through, aggregate } = field.descriptor;
  return [
    "_type",
    "_scope",
    "_id",
    ...Object.keys(through?.where ?? {}),
    ...(aggregate.kind === "collect" ? [aggregate.path] : []),
  ];
}

function fieldsTouchedBy(
  fields: readonly ComputedField[],
  update: UpdateShape | undefined,
  paths: (field: ComputedField) => readonly string[],
): readonly ComputedField[] {
  const touched = touchedPaths(update);
  if (touched === "all") return fields;
  return fields.filter((field) =>
    paths(field).some((path) =>
      touched.some((candidate) => overlaps(candidate, path)),
    ),
  );
}

function nearFieldsTouchedBy(
  plan: Plan,
  update: UpdateShape | undefined,
): readonly ComputedField[] {
  return fieldsTouchedBy(plan.near, update, relevantPaths);
}

function farFieldsTouchedBy(
  plan: Plan,
  update: UpdateShape | undefined,
): readonly ComputedField[] {
  return fieldsTouchedBy(plan.far, update, farRelevantPaths);
}

function valueAt(document: Document, path: string): unknown {
  let current: unknown = document;
  for (const segment of path.split(".")) {
    if (
      current === null ||
      typeof current !== "object" ||
      Array.isArray(current)
    )
      return undefined;
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}

function projectionFor(fields: readonly ComputedField[]): Document {
  const paths = new Set([
    "_id",
    "_type",
    "_scope",
    ...fields.map((field) => field.descriptor.by),
  ]);
  const kept = [...paths].filter(
    (path, _, all) =>
      !all.some((other) => other !== path && path.startsWith(`${other}.`)),
  );
  return Object.fromEntries(kept.map((path) => [path, 1]));
}

function isOfType(
  document: Document,
  location: ComputedField["source"],
): boolean {
  return location.kind === "collection" || document._type === location.type;
}

class Affected {
  readonly #bySubjectField = new Map<ComputedField, Map<string, unknown>>();
  readonly #whole = new Map<ComputedField, Set<string | undefined>>();

  whole(fields: readonly ComputedField[], scope: string | undefined): void {
    for (const field of fields) {
      const scopes = this.#whole.get(field) ?? new Set<string | undefined>();
      scopes.add(scope);
      this.#whole.set(field, scopes);
    }
  }

  wholeEntries(): IterableIterator<[ComputedField, Set<string | undefined>]> {
    return this.#whole.entries();
  }

  readonly #far = new Map<
    ComputedField,
    Map<string, { id: unknown; scope: string | undefined }>
  >();

  fromFar(
    fields: readonly ComputedField[],
    documents: readonly Document[],
  ): void {
    for (const document of documents) {
      for (const field of fields) {
        if (!field.far || !isOfType(document, field.far)) continue;
        const ids =
          this.#far.get(field) ??
          new Map<string, { id: unknown; scope: string | undefined }>();
        const scope =
          field.farScoped && typeof document._scope === "string"
            ? document._scope
            : undefined;
        ids.set(`${scope ?? "*"}|${String(document._id)}`, {
          id: document._id,
          scope,
        });
        this.#far.set(field, ids);
      }
    }
  }

  farEntries(): IterableIterator<
    [ComputedField, Map<string, { id: unknown; scope: string | undefined }>]
  > {
    return this.#far.entries();
  }

  add(field: ComputedField, id: unknown): void {
    const ids = this.#bySubjectField.get(field) ?? new Map<string, unknown>();
    ids.set(String(id), id);
    this.#bySubjectField.set(field, ids);
  }

  fromSource(
    fields: readonly ComputedField[],
    documents: readonly Document[],
  ): void {
    for (const document of documents) {
      for (const field of fields) {
        if (!isOfType(document, field.source)) continue;
        const by = valueAt(document, field.descriptor.by);
        for (const id of by === undefined || by === null
          ? []
          : Array.isArray(by)
            ? by
            : [by])
          this.add(field, id);
      }
    }
  }

  fromSubjects(
    fields: readonly ComputedField[],
    documents: readonly Document[],
  ): void {
    for (const document of documents) {
      for (const field of fields) {
        if (isOfType(document, field.at)) this.add(field, document._id);
      }
    }
  }

  entries(): IterableIterator<[ComputedField, Map<string, unknown>]> {
    return this.#bySubjectField.entries();
  }
}

function idCandidates(ids: Iterable<unknown>): unknown[] {
  const candidates: unknown[] = [];
  for (const id of ids) {
    candidates.push(id);
    if (typeof id === "string" && /^[0-9a-f]{24}$/i.test(id))
      candidates.push(new ObjectId(id));
  }
  return candidates;
}

async function recomputeAffected(
  db: Db,
  affected: Affected,
  session: ClientSession | undefined,
  limit: number,
): Promise<void> {
  const marked = new Set<ComputedField>();
  for (const [field, scopes] of affected.wholeEntries()) {
    marked.add(field);
    for (const scope of scopes.has(undefined) ? [undefined] : scopes) {
      await markWhole(
        db,
        field,
        scope,
        "a write targeted more documents than the inline limit",
        session,
      );
    }
  }
  for (const [field, targets] of affected.farEntries()) {
    if (marked.has(field)) continue;
    if (targets.size > limit) {
      marked.add(field);
      await markWhole(
        db,
        field,
        undefined,
        "a write changed more far documents than the inline limit",
        session,
      );
      continue;
    }
    for (const { id, scope } of targets.values()) {
      await markFar(
        db,
        field,
        id,
        scope,
        "a far document of a through field changed",
        session,
      );
    }
  }
  const byLocation = new Map<
    string,
    { fields: ComputedField[]; ids: Map<string, unknown> }
  >();
  for (const [field, ids] of affected.entries()) {
    if (marked.has(field)) continue;
    if (ids.size > limit) {
      await markWhole(
        db,
        field,
        undefined,
        "a write affected more subjects than the inline limit",
        session,
      );
      continue;
    }
    const key = `${field.at.collection}|${field.subject}`;
    const entry = byLocation.get(key) ?? {
      fields: [],
      ids: new Map<string, unknown>(),
    };
    entry.fields.push(field);
    for (const [key, id] of ids) entry.ids.set(key, id);
    byLocation.set(key, entry);
  }
  for (const { fields, ids } of byLocation.values()) {
    const at = fields[0]!.at;
    const subjects = (await db
      .collection(at.collection)
      .find(
        {
          ...locationFilter(at),
          _id: { $in: idCandidates(ids.values()) },
        } as Filter<Document>,
        {
          session,
          projection: { _id: 1, _scope: 1, [COMPUTED_ROOT]: 1 },
        },
      )
      .toArray()) as ComputedSubject[];
    for (const field of fields) {
      const concerned = subjects.filter((subject) =>
        ids.has(String(subject._id)),
      );
      await recomputeSubjects(db, [field], concerned, session);
    }
  }
}

const WRITE_METHODS = new Set([
  "insertOne",
  "insertMany",
  "updateOne",
  "updateMany",
  "replaceOne",
  "deleteOne",
  "deleteMany",
  "findOneAndUpdate",
  "findOneAndReplace",
  "findOneAndDelete",
  "bulkWrite",
]);

const OPTIONS_INDEX: Record<string, number> = {
  insertOne: 1,
  insertMany: 1,
  updateOne: 2,
  updateMany: 2,
  replaceOne: 2,
  deleteOne: 1,
  deleteMany: 1,
  findOneAndUpdate: 2,
  findOneAndReplace: 2,
  findOneAndDelete: 1,
  bulkWrite: 1,
};

interface WriteContext {
  readonly db: Db;
  readonly target: Collection<Document>;
  readonly name: string;
  readonly plan: Plan;
  readonly session: ClientSession | undefined;
}

function scopeOf(filter: Filter<Document>): string | undefined {
  const scope = (filter as { _scope?: unknown })._scope;
  return typeof scope === "string" ? scope : undefined;
}

async function readTargets(
  context: WriteContext,
  filter: Filter<Document>,
  fields: readonly ComputedField[],
  affected: Affected,
): Promise<Document[]> {
  const limit = context.plan.registration.inlineLimit;
  const found = await context.target
    .find(filter, {
      session: context.session,
      projection: projectionFor([...fields, ...context.plan.near]),
      limit: limit + 1,
    })
    .toArray();
  if (found.length > limit) {
    affected.whole(fields, scopeOf(filter));
    return [];
  }
  return found;
}

async function readByIds(
  context: WriteContext,
  ids: readonly unknown[],
): Promise<Document[]> {
  if (ids.length === 0) return [];
  return await context.target
    .find({ _id: { $in: [...ids] } } as Filter<Document>, {
      session: context.session,
      projection: projectionFor(context.plan.near),
    })
    .toArray();
}

interface FilterOperation {
  readonly filter: Filter<Document>;
  readonly update?: UpdateShape;
  readonly replaces: boolean;
}

function bulkShape(operations: readonly AnyBulkWriteOperation<Document>[]): {
  inserted: Document[];
  filtered: FilterOperation[];
} {
  const inserted: Document[] = [];
  const filtered: FilterOperation[] = [];
  for (const operation of operations) {
    if ("insertOne" in operation) inserted.push(operation.insertOne.document);
    else if ("updateOne" in operation)
      filtered.push({
        filter: operation.updateOne.filter,
        update: operation.updateOne.update as UpdateShape,
        replaces: false,
      });
    else if ("updateMany" in operation)
      filtered.push({
        filter: operation.updateMany.filter,
        update: operation.updateMany.update as UpdateShape,
        replaces: false,
      });
    else if ("replaceOne" in operation)
      filtered.push({ filter: operation.replaceOne.filter, replaces: true });
    else if ("deleteOne" in operation)
      filtered.push({ filter: operation.deleteOne.filter, replaces: false });
    else if ("deleteMany" in operation)
      filtered.push({ filter: operation.deleteMany.filter, replaces: false });
  }
  return { inserted, filtered };
}

async function maintainedCall(
  context: WriteContext,
  method: string,
  args: unknown[],
  original: (...args: unknown[]) => Promise<unknown>,
): Promise<unknown> {
  const { plan } = context;
  const affected = new Affected();
  const callArgs = [...args];
  const optionsIndex = OPTIONS_INDEX[method]!;
  callArgs[optionsIndex] = {
    ...(args[optionsIndex] as Document | undefined),
    session: context.session,
  };

  switch (method) {
    case "insertOne":
    case "insertMany": {
      const result = await original(...callArgs);
      const documents =
        method === "insertOne"
          ? [args[0] as Document]
          : (args[0] as Document[]);
      affected.fromSource(plan.near, documents);
      affected.fromSubjects(plan.subjects, documents);
      affected.fromFar(plan.far, documents);
      await recomputeAffected(
        context.db,
        affected,
        context.session,
        plan.registration.inlineLimit,
      );
      return result;
    }
    case "updateOne":
    case "updateMany":
    case "replaceOne":
    case "findOneAndUpdate":
    case "findOneAndReplace": {
      const replaces =
        method === "replaceOne" || method === "findOneAndReplace";
      const fields = replaces
        ? plan.near
        : nearFieldsTouchedBy(plan, args[1] as UpdateShape);
      const farFields = replaces
        ? plan.far
        : farFieldsTouchedBy(plan, args[1] as UpdateShape);
      const filter = args[0] as Filter<Document>;
      const upsert =
        (args[optionsIndex] as { upsert?: boolean } | undefined)?.upsert ===
        true;
      if (fields.length === 0 && farFields.length === 0 && !replaces && !upsert)
        return await original(...args);
      const before = await readTargets(
        context,
        filter,
        [...fields, ...farFields],
        affected,
      );
      let upsertedId: unknown;
      let result: unknown;
      if (method.startsWith("findOneAnd")) {
        const wantsMetadata =
          (
            args[optionsIndex] as
              | { includeResultMetadata?: boolean }
              | undefined
          )?.includeResultMetadata === true;
        callArgs[optionsIndex] = {
          ...(callArgs[optionsIndex] as Document),
          includeResultMetadata: true,
        };
        const raw = (await original(...callArgs)) as {
          value: Document | null;
          lastErrorObject?: { upserted?: unknown };
        };
        upsertedId = raw.lastErrorObject?.upserted;
        if (
          raw.value?._id !== undefined &&
          !before.some(
            (document) => String(document._id) === String(raw.value!._id),
          )
        )
          upsertedId ??= raw.value._id;
        result = wantsMetadata ? raw : raw.value;
      } else {
        result = await original(...callArgs);
        upsertedId =
          (result as { upsertedId?: unknown }).upsertedId ?? undefined;
      }
      const after = await readByIds(context, [
        ...before.map((document) => document._id),
        ...(upsertedId === undefined || upsertedId === null
          ? []
          : [upsertedId]),
      ]);
      affected.fromSource(fields, before);
      affected.fromSource(fields, after);
      const created = after.filter(
        (document) =>
          upsertedId !== undefined &&
          String(document._id) === String(upsertedId),
      );
      affected.fromSubjects(plan.subjects, replaces ? after : created);
      affected.fromFar(farFields, after);
      if (upsertedId !== undefined && upsertedId !== null) {
        affected.fromSource(plan.near, created);
        affected.fromFar(plan.far, created);
      }
      await recomputeAffected(
        context.db,
        affected,
        context.session,
        plan.registration.inlineLimit,
      );
      return result;
    }
    case "deleteOne":
    case "deleteMany":
    case "findOneAndDelete": {
      const before = await readTargets(
        context,
        args[0] as Filter<Document>,
        [...plan.near, ...plan.far],
        affected,
      );
      const result = await original(...callArgs);
      const remaining = new Set(
        (
          await readByIds(
            context,
            before.map((document) => document._id),
          )
        ).map((document) => String(document._id)),
      );
      const deleted = before.filter(
        (document) => !remaining.has(String(document._id)),
      );
      affected.fromSource(plan.near, deleted);
      affected.fromFar(plan.far, deleted);
      await recomputeAffected(
        context.db,
        affected,
        context.session,
        plan.registration.inlineLimit,
      );
      return result;
    }
    case "bulkWrite": {
      const { inserted, filtered } = bulkShape(
        args[0] as AnyBulkWriteOperation<Document>[],
      );
      const before: Document[] = [];
      for (const operation of filtered) {
        const whole = operation.replaces || operation.update === undefined;
        const fields = whole
          ? plan.near
          : nearFieldsTouchedBy(plan, operation.update);
        const farFields = whole
          ? plan.far
          : farFieldsTouchedBy(plan, operation.update);
        before.push(
          ...(await readTargets(
            context,
            operation.filter,
            [...fields, ...farFields],
            affected,
          )),
        );
      }
      const result = (await original(...callArgs)) as {
        upsertedIds?: Record<number, unknown>;
      };
      const upserted = Object.values(result.upsertedIds ?? {});
      const after = await readByIds(context, [
        ...before.map((document) => document._id),
        ...upserted,
      ]);
      affected.fromSource(plan.near, before);
      affected.fromSource(plan.near, after);
      affected.fromSource(plan.near, inserted);
      affected.fromSubjects(plan.subjects, inserted);
      affected.fromFar(plan.far, [...before, ...after, ...inserted]);
      const replacedOrUpserted = new Set([
        ...upserted.map(String),
        ...(filtered.some((operation) => operation.replaces)
          ? after.map((document) => String(document._id))
          : []),
      ]);
      affected.fromSubjects(
        plan.subjects,
        after.filter((document) =>
          replacedOrUpserted.has(String(document._id)),
        ),
      );
      await recomputeAffected(
        context.db,
        affected,
        context.session,
        plan.registration.inlineLimit,
      );
      return result;
    }
  }
  return await original(...args);
}

export function maintainedCollection<T extends Document>(
  db: Db,
  target: Collection<T>,
  collectionName: string,
  declaresComputed: boolean,
): Collection<T> {
  return new Proxy(target, {
    get(object, property, receiver) {
      const value = Reflect.get(object, property, receiver);
      if (typeof property !== "string" || typeof value !== "function")
        return value;
      if (
        property === "initializeOrderedBulkOp" ||
        property === "initializeUnorderedBulkOp"
      ) {
        return (...args: unknown[]) => {
          if (planFor(db, collectionName, declaresComputed)) {
            throw new ComputedUnsupportedWriteError(
              `${property} cannot maintain computed fields on "${collectionName}"; use bulkWrite`,
            );
          }
          return value.apply(object, args);
        };
      }
      if (property === "drop") {
        return async (...args: unknown[]) => {
          const plan = planFor(db, collectionName, declaresComputed);
          for (const field of [...(plan?.near ?? []), ...(plan?.far ?? [])]) {
            await markWhole(
              db,
              field,
              undefined,
              `source collection "${collectionName}" was dropped`,
            );
          }
          return await value.apply(object, args);
        };
      }
      if (!WRITE_METHODS.has(property)) return value;
      return async (...args: unknown[]) => {
        const plan = planFor(db, collectionName, declaresComputed);
        const original = (...callArgs: unknown[]) =>
          value.apply(object, callArgs) as Promise<unknown>;
        if (!plan) return await original(...args);
        const sessionContext = getSessionContext(db.client);
        const given =
          (
            args[OPTIONS_INDEX[property]!] as
              | { session?: ClientSession }
              | undefined
          )?.session ?? sessionContext.getSession();
        const run = (session: ClientSession | undefined) =>
          maintainedCall(
            {
              db,
              target: object as unknown as Collection<Document>,
              name: collectionName,
              plan,
              session,
            },
            property,
            args,
            original,
          );
        if (given?.inTransaction()) return await run(given);
        const transactions = await checkTransactionEnabled(db.client, db);
        if (!transactions || given) {
          if (plan.registration.standaloneMode !== "best-effort") {
            throw new ComputedRequiresTransactionError(
              `a write on "${collectionName}" feeds computed fields and needs a transaction; this deployment has none (declare standaloneMode: "best-effort" for tests and development only)`,
            );
          }
          return await run(given);
        }
        return await retryOnWriteConflict(
          () => sessionContext.withSession((session) => run(session)),
          plan.registration.retry,
        );
      };
    },
  });
}
