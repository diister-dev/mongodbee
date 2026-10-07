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
  farLookupIndexed,
  locationFilter,
} from "./computed-topology.ts";
import {
  type FarTarget,
  farLinksOf,
  recomputeSubjects,
  subjectsReachingFar,
  toSubjects,
} from "./computed-apply.ts";
import {
  type DocumentId,
  isDocumentId,
  type StoredDocument,
  storedCollection,
} from "./stored-document.ts";
import {
  farFenceId,
  fenceFarLinks,
  markFar,
  markWhole,
} from "./computed-marks.ts";
import { checkTransactionEnabled, getSessionContext } from "./session.ts";
import {
  isRetryableTransactionFailure,
  type RetryOptions,
  retryOnWriteConflict,
} from "./utils/retry.ts";
import { primaryCollection } from "./read-preference.ts";
import { isRecord } from "./utils/guards.ts";
import {
  invalidateReadersAtCommit,
  invalidateReadersFor,
  type WriteFootprint,
} from "./reader-cache.ts";
import { asMongodbeeWrite } from "./request-context.ts";
import { ClientRegistry } from "./client-registry.ts";

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

const registrations = new ClientRegistry<Registration>();

export function registerComputed(
  target: Db | MongoClient,
  topology: ComputedTopology,
  options: ComputedRegistrationOptions = {},
): void {
  registrations.set(target, {
    topology,
    inlineLimit: options.inlineLimit ?? DEFAULT_INLINE_RECOMPUTE_LIMIT,
    standaloneMode: options.standaloneMode ?? "refuse",
    retry: options.retry ?? DEFAULT_COMPUTED_RETRY,
  });
}

export function unregisterComputed(target: Db | MongoClient): void {
  registrations.delete(target);
}

export function computedRegistration(db: Db): Registration | undefined {
  return registrations.get(db);
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
    if (!isRecord(value)) continue;
    for (const [path, target] of Object.entries(value)) {
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
    if (!isRecord(current)) return undefined;
    current = current[segment];
  }
  return current;
}

function projectionFor(fields: readonly ComputedField[]): Document {
  const paths = new Set([
    "_id",
    "_type",
    "_scope",
    ...fields.flatMap(({ descriptor }) => [
      descriptor.by,
      ...(descriptor.through
        ? [descriptor.through.via, ...Object.keys(descriptor.where)]
        : []),
    ]),
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
  readonly #bySubjectField = new Map<ComputedField, Map<string, DocumentId>>();
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

  readonly #far = new Map<ComputedField, Map<string, FarTarget>>();

  fromFar(
    fields: readonly ComputedField[],
    documents: readonly Document[],
  ): void {
    for (const document of documents) {
      for (const field of fields) {
        if (!field.far || !isOfType(document, field.far)) continue;
        const id: unknown = document._id;
        if (!isDocumentId(id)) {
          throw new TypeError(
            `a far document of a computed field must have a string or ObjectId _id, got ${typeof id}`,
          );
        }
        const ids = this.#far.get(field) ?? new Map<string, FarTarget>();
        const scope =
          field.farScoped && typeof document._scope === "string"
            ? document._scope
            : undefined;
        ids.set(`${scope ?? "*"}|${String(id)}`, { id, scope });
        this.#far.set(field, ids);
      }
    }
  }

  farEntries(): IterableIterator<[ComputedField, Map<string, FarTarget>]> {
    return this.#far.entries();
  }

  add(field: ComputedField, id: unknown): void {
    if (!isDocumentId(id)) return;
    const ids =
      this.#bySubjectField.get(field) ?? new Map<string, DocumentId>();
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
        const values: readonly unknown[] = Array.isArray(by) ? by : [by];
        for (const id of values) this.add(field, id);
      }
    }
  }

  readonly #created = new Map<ComputedField, Set<string>>();

  fromSubjects(
    fields: readonly ComputedField[],
    documents: readonly Document[],
  ): void {
    for (const document of documents) {
      for (const field of fields) {
        if (!isOfType(document, field.at)) continue;
        this.add(field, document._id);
        if (!field.descriptor.through) continue;
        const created = this.#created.get(field) ?? new Set<string>();
        created.add(String(document._id));
        this.#created.set(field, created);
      }
    }
  }

  created(field: ComputedField, subject: string): boolean {
    return this.#created.get(field)?.has(subject) === true;
  }

  readonly #links = new Set<string>();

  linksFrom(
    fields: readonly ComputedField[],
    before: readonly Document[],
    after: readonly Document[],
  ): void {
    const previous = new Map(
      before.map((document) => [String(document._id), document]),
    );
    for (const field of fields) {
      if (!field.descriptor.through) continue;
      for (const document of after) {
        const had = farLinksOf(field, previous.get(String(document._id)));
        for (const [key, far] of farLinksOf(field, document)) {
          if (!had.has(key)) this.#links.add(farFenceId(field, far));
        }
      }
    }
  }

  linkFences(): ReadonlySet<string> {
    return this.#links;
  }

  entries(): IterableIterator<[ComputedField, Map<string, DocumentId>]> {
    return this.#bySubjectField.entries();
  }
}

function idCandidates(ids: Iterable<DocumentId>): DocumentId[] {
  const candidates: DocumentId[] = [];
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
  registration: Registration,
): Promise<void> {
  const limit = registration.inlineLimit;
  const fenced = new Set<string>(affected.linkFences());
  for (const [field, targets] of affected.farEntries()) {
    for (const target of targets.values())
      fenced.add(farFenceId(field, target));
  }
  await fenceFarLinks(db, fenced, session);
  const late = new Set<string>();
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
    const indexed = farLookupIndexed(registration.topology, field);
    const subjects = indexed
      ? await subjectsReachingFar(
          db,
          field,
          [...targets.values()],
          session,
          limit,
        )
      : undefined;
    if (subjects === undefined) {
      for (const { id, scope } of targets.values()) {
        await markFar(
          db,
          field,
          id,
          scope,
          indexed
            ? "a far document of a through field changed and reaches more subjects than the inline limit"
            : "a far document of a through field changed and no index leads the near read with its link",
          session,
        );
      }
      continue;
    }
    for (const id of subjects.values()) affected.add(field, id);
  }
  const byLocation = new Map<
    string,
    { fields: ComputedField[]; ids: Map<string, DocumentId> }
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
      ids: new Map<string, DocumentId>(),
    };
    entry.fields.push(field);
    for (const [key, id] of ids) entry.ids.set(key, id);
    byLocation.set(key, entry);
  }
  for (const { fields, ids } of byLocation.values()) {
    const at = fields[0]!.at;
    const filter: Filter<StoredDocument> = {
      ...locationFilter(at),
      _id: { $in: idCandidates(ids.values()) },
    };
    const subjects = toSubjects(
      await storedCollection(db, at.collection)
        .find(filter, {
          session,
          projection: { _id: 1, _scope: 1, [COMPUTED_ROOT]: 1 },
        })
        .toArray(),
    );
    for (const field of fields) {
      const concerned = subjects.filter((subject) =>
        ids.has(String(subject._id)),
      );
      await recomputeSubjects(
        db,
        [field],
        concerned,
        session,
        (reachedField, subject, far) => {
          const fence = farFenceId(reachedField, far);
          if (affected.created(reachedField, subject) && !fenced.has(fence))
            late.add(fence);
        },
      );
    }
  }
  await fenceFarLinks(db, late, session);
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

function documentIds(ids: readonly unknown[]): DocumentId[] {
  return ids.map((id) => {
    if (!isDocumentId(id)) {
      throw new TypeError(
        `a document feeding a computed field must have a string or ObjectId _id, got ${typeof id}`,
      );
    }
    return id;
  });
}

interface WriteContext {
  readonly db: Db;
  readonly name: string;
  readonly plan: Plan;
  readonly session: ClientSession | undefined;
}

function scopeOf(filter: Filter<Document>): string | undefined {
  const scope: unknown = filter._scope;
  return typeof scope === "string" ? scope : undefined;
}

async function readTargets(
  context: WriteContext,
  filter: Filter<Document>,
  fields: readonly ComputedField[],
  affected: Affected,
): Promise<Document[]> {
  const limit = context.plan.registration.inlineLimit;
  const found = await primaryCollection(context.db, context.name)
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
  const filter: Filter<StoredDocument> = { _id: { $in: documentIds(ids) } };
  return await primaryCollection<StoredDocument>(context.db, context.name)
    .find(filter, {
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
      affected.linksFrom(plan.near, [], documents);
      affected.fromSubjects(plan.subjects, documents);
      affected.fromFar(plan.far, documents);
      await recomputeAffected(
        context.db,
        affected,
        context.session,
        plan.registration,
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
      affected.linksFrom(fields, before, after);
      const created = after.filter(
        (document) =>
          upsertedId !== undefined &&
          String(document._id) === String(upsertedId),
      );
      affected.fromSubjects(plan.subjects, replaces ? after : created);
      affected.fromFar(farFields, after);
      if (upsertedId !== undefined && upsertedId !== null) {
        affected.fromSource(plan.near, created);
        affected.linksFrom(plan.near, [], created);
        affected.fromFar(plan.far, created);
      }
      await recomputeAffected(
        context.db,
        affected,
        context.session,
        plan.registration,
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
        plan.registration,
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
      affected.linksFrom(plan.near, before, after);
      affected.linksFrom(plan.near, [], inserted);
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
        plan.registration,
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
          return withReaderInvalidationOnExecute(value.apply(object, args), [
            wholeCollection(db, collectionName),
          ]);
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
          return await invalidatingReaders(
            [wholeCollection(db, collectionName)],
            () => value.apply(object, args) as Promise<unknown>,
          );
        };
      }
      if (!WRITE_METHODS.has(property)) return value;
      return async (...args: unknown[]) => {
        const plan = planFor(db, collectionName, declaresComputed);
        return await invalidatingReaders(
          readerFootprints(db, collectionName, property, args, plan),
          () =>
            maintainedWrite(
              db,
              collectionName,
              plan,
              property,
              args,
              (...callArgs) =>
                value.apply(object, callArgs) as Promise<unknown>,
            ),
        );
      };
    },
  });
}

async function maintainedWrite(
  db: Db,
  collectionName: string,
  plan: Plan | undefined,
  property: string,
  args: unknown[],
  original: (...callArgs: unknown[]) => Promise<unknown>,
): Promise<unknown> {
  if (!plan) return await original(...args);
  const sessionContext = getSessionContext(db.client);
  const given =
    (args[OPTIONS_INDEX[property]!] as { session?: ClientSession } | undefined)
      ?.session ?? sessionContext.getSession();
  const run = (session: ClientSession | undefined) =>
    maintainedCall(
      { db, name: collectionName, plan, session },
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
    {
      ...plan.registration.retry,
      shouldRetry: isRetryableTransactionFailure,
    },
  );
}

function wholeCollection(db: Db, collection: string): WriteFootprint {
  return {
    database: db.databaseName,
    collection,
    types: "*",
    scopes: "*",
    touched: "all",
  };
}

function constrainedTo(
  filter: unknown,
  field: "_type" | "_scope",
): readonly string[] | "*" {
  if (!isRecord(filter)) return "*";
  const value = filter[field];
  if (typeof value === "string") return [value];
  if (isRecord(value)) {
    if (typeof value.$eq === "string") return [value.$eq];
    if (
      Array.isArray(value.$in) &&
      value.$in.every((item) => typeof item === "string")
    )
      return value.$in as string[];
  }
  if (Array.isArray(filter.$and)) {
    for (const part of filter.$and) {
      const found = constrainedTo(part, field);
      if (found !== "*") return found;
    }
  }
  return "*";
}

function footprint(
  db: Db,
  collection: string,
  target: unknown,
  touched: readonly string[] | "all",
): WriteFootprint {
  return {
    database: db.databaseName,
    collection,
    types: constrainedTo(target, "_type"),
    scopes: constrainedTo(target, "_scope"),
    touched,
  };
}

function updateFootprint(
  db: Db,
  collection: string,
  filter: unknown,
  update: unknown,
  options: unknown,
): WriteFootprint {
  const upsert = isRecord(options) && options.upsert === true;
  const shape = Array.isArray(update) || isRecord(update) ? update : undefined;
  return footprint(
    db,
    collection,
    filter,
    upsert ? "all" : touchedPaths(shape as UpdateShape | undefined),
  );
}

function readerFootprints(
  db: Db,
  collection: string,
  method: string,
  args: readonly unknown[],
  plan: Plan | undefined,
): WriteFootprint[] {
  const written: WriteFootprint[] = [];
  switch (method) {
    case "insertOne":
      written.push(footprint(db, collection, args[0], "all"));
      break;
    case "insertMany":
      for (const document of Array.isArray(args[0]) ? args[0] : [])
        written.push(footprint(db, collection, document, "all"));
      break;
    case "updateOne":
    case "updateMany":
    case "findOneAndUpdate":
      written.push(updateFootprint(db, collection, args[0], args[1], args[2]));
      break;
    case "bulkWrite":
      for (const operation of Array.isArray(args[0]) ? args[0] : []) {
        if (!isRecord(operation)) continue;
        const [kind, body] = Object.entries(operation)[0] ?? [];
        if (!isRecord(body)) continue;
        if (kind === "insertOne")
          written.push(footprint(db, collection, body.document, "all"));
        else if (kind === "updateOne" || kind === "updateMany")
          written.push(
            updateFootprint(db, collection, body.filter, body.update, body),
          );
        else written.push(footprint(db, collection, body.filter, "all"));
      }
      break;
    default:
      written.push(footprint(db, collection, args[0], "all"));
  }
  for (const field of [...(plan?.near ?? []), ...(plan?.far ?? [])]) {
    written.push({
      database: db.databaseName,
      collection: field.at.collection,
      types: field.at.kind === "collection" ? "*" : [field.at.type],
      scopes: "*",
      touched: [COMPUTED_ROOT],
    });
  }
  return written;
}

async function invalidatingReaders<T>(
  footprints: readonly WriteFootprint[],
  write: () => Promise<T>,
): Promise<T> {
  invalidateReadersFor(footprints);
  try {
    return await asMongodbeeWrite(write);
  } finally {
    invalidateReadersFor(footprints);
    invalidateReadersAtCommit(footprints);
  }
}

function withReaderInvalidationOnExecute<T>(
  operation: T,
  footprints: readonly WriteFootprint[],
): T {
  if (!isBulkOperation(operation)) return operation;
  const execute = operation.execute.bind(operation);
  operation.execute = (...args: unknown[]) =>
    invalidatingReaders(footprints, () => execute(...args));
  return operation;
}

function isBulkOperation(
  value: unknown,
): value is { execute: (...args: unknown[]) => Promise<unknown> } {
  return isRecord(value) && typeof value.execute === "function";
}
