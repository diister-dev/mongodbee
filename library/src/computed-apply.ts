import type {
  AnyBulkWriteOperation,
  Document,
  Filter,
  UpdateFilter,
} from "mongodb";
import type { ClientSession, Db } from "./mongodb.ts";
import {
  type DocumentId,
  isDocumentId,
  type StoredDocument,
  storedCollection,
} from "./stored-document.ts";
import {
  COMPUTED_REVISION,
  COMPUTED_ROOT,
  type ComputedWhere,
} from "./computed.ts";
import {
  type ComputedField,
  type ComputedTopology,
  locationFilter,
} from "./computed-topology.ts";
import { getSessionContext } from "./session.ts";
import { retryOnWriteConflict } from "./utils/retry.ts";
import { isRecord } from "./utils/guards.ts";

export interface ComputedSubject {
  readonly _id: DocumentId;
  readonly _scope?: string;
  readonly _computed?: Readonly<Record<string, unknown>>;
}

export function toSubjects(documents: readonly Document[]): ComputedSubject[] {
  return documents.map((document) => {
    const id: unknown = document._id;
    if (!isDocumentId(id)) {
      throw new TypeError(
        `a computed subject must have a string or ObjectId _id, got ${typeof id}`,
      );
    }
    return {
      _id: id,
      ...(typeof document._scope === "string" && { _scope: document._scope }),
      ...(isRecord(document[COMPUTED_ROOT]) && {
        _computed: document[COMPUTED_ROOT],
      }),
    };
  });
}

export class ComputedEntriesExceededError extends Error {
  override readonly name = "ComputedEntriesExceededError";
  readonly field: string;
  readonly subject: string;
  readonly entries: number;
  readonly maxEntries: number;

  constructor(
    field: ComputedField,
    subject: string,
    entries: number,
    maxEntries: number,
  ) {
    super(
      `computed field "${field.subject}.${field.name}" of ${subject} would hold ${entries} entries, above its maxEntries of ${maxEntries}`,
    );
    this.field = `${field.subject}.${field.name}`;
    this.subject = subject;
    this.entries = entries;
    this.maxEntries = maxEntries;
  }
}

export interface ComputedDrift {
  readonly subject: string;
  readonly field: string;
  readonly id: DocumentId;
  readonly stored: unknown;
  readonly truth: unknown;
  readonly missing: boolean;
}

export function whereFilter(where: ComputedWhere): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(where).map(([path, value]) => [
      path,
      Array.isArray(value) ? { $in: [...value] } : value,
    ]),
  );
}

function valueAt(document: Document, path: string): unknown {
  let current: unknown = document;
  for (const segment of path.split(".")) {
    if (!isRecord(current)) return undefined;
    current = current[segment];
  }
  return current;
}

function asList(value: unknown): unknown[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function keyOf(value: unknown): string {
  return typeof value === "object" && value !== null
    ? `o:${JSON.stringify(value)}`
    : `${typeof value}:${String(value)}`;
}

function projectionOf(paths: readonly string[]): Record<string, 1> {
  const kept = [...new Set(["_id", ...paths])].filter(
    (path, _, all) =>
      !all.some((other) => other !== path && path.startsWith(`${other}.`)),
  );
  return Object.fromEntries(kept.map((path) => [path, 1 as const]));
}

function groupByScope(
  field: ComputedField,
  subjects: readonly ComputedSubject[],
): Map<string | undefined, ComputedSubject[]> {
  const groups = new Map<string | undefined, ComputedSubject[]>();
  for (const subject of subjects) {
    const scope = field.scoped || field.farScoped ? subject._scope : undefined;
    groups.set(scope, [...(groups.get(scope) ?? []), subject]);
  }
  return groups;
}

export async function computeTruth(
  db: Db,
  field: ComputedField,
  subjects: readonly ComputedSubject[],
  session?: ClientSession,
): Promise<Map<string, unknown>> {
  const { descriptor } = field;
  const aggregate = descriptor.aggregate;
  const contributions = new Map<string, Document[]>(
    subjects.map((subject) => [String(subject._id), []]),
  );

  for (const [scope, group] of groupByScope(field, subjects)) {
    const ids = group.map((subject) => subject._id);
    const valuePath = descriptor.through
      ? descriptor.through.via
      : aggregate.kind === "collect"
        ? aggregate.path
        : undefined;
    const near = await storedCollection(db, field.source.collection)
      .find(
        {
          ...locationFilter(field.source),
          ...whereFilter(descriptor.where),
          [descriptor.by]: {
            $in: [
              ...ids,
              ...ids.filter((id) => typeof id !== "string").map(String),
            ],
          },
          ...(field.scoped && { _scope: scope }),
        },
        {
          session,
          projection: projectionOf([
            descriptor.by,
            ...(valuePath ? [valuePath] : []),
          ]),
          sort: { _id: 1 },
        },
      )
      .toArray();

    let farById: Map<string, Document> | undefined;
    if (descriptor.through && field.far) {
      const vias = [
        ...new Map(
          near
            .flatMap((document) =>
              asList(valueAt(document, descriptor.through!.via)),
            )
            .filter(isDocumentId)
            .map((via) => [keyOf(via), via]),
        ).values(),
      ];
      const farDocuments =
        vias.length === 0
          ? []
          : await storedCollection(db, field.far.collection)
              .find(
                {
                  ...locationFilter(field.far),
                  ...whereFilter(descriptor.through.where),
                  _id: { $in: vias },
                  ...(field.farScoped && { _scope: scope }),
                },
                {
                  session,
                  projection: projectionOf(
                    aggregate.kind === "collect" ? [aggregate.path] : [],
                  ),
                },
              )
              .toArray();
      farById = new Map(
        farDocuments.map((document) => [keyOf(document._id), document]),
      );
    }

    const wanted = new Set(ids.map(String));
    for (const document of near) {
      const owners = new Set(
        asList(valueAt(document, descriptor.by))
          .map(String)
          .filter((id) => wanted.has(id)),
      );
      const reached = farById
        ? asList(valueAt(document, descriptor.through!.via))
            .map((via) => farById!.get(keyOf(via)))
            .filter((far): far is Document => far !== undefined)
        : [document];
      for (const owner of owners) contributions.get(owner)!.push(...reached);
    }
  }

  const truth = new Map<string, unknown>();
  for (const [subject, documents] of contributions) {
    if (aggregate.kind === "count") {
      truth.set(subject, documents.length);
      continue;
    }
    const values = documents
      .map((document) => valueAt(document, aggregate.path))
      .filter((value) => value !== undefined && value !== null);
    const collected = aggregate.distinct
      ? [...new Map(values.map((value) => [keyOf(value), value])).values()]
      : values;
    if (
      aggregate.maxEntries !== undefined &&
      collected.length > aggregate.maxEntries
    ) {
      throw new ComputedEntriesExceededError(
        field,
        subject,
        collected.length,
        aggregate.maxEntries,
      );
    }
    truth.set(subject, collected);
  }
  return truth;
}

function sameValue(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

interface RevisedSubject {
  _id: DocumentId;
  _type?: string;
  _scope?: string;
  _computed?: { _rev?: number };
}

type SubjectUpdate = {
  readonly collection: string;
  readonly filter: Filter<RevisedSubject>;
  readonly set: Record<string, unknown>;
};

export async function recomputeSubjects(
  db: Db,
  fields: readonly ComputedField[],
  subjects: readonly ComputedSubject[],
  session?: ClientSession,
): Promise<number> {
  if (subjects.length === 0) return 0;
  const revise = session?.inTransaction() === true;
  const updates = new Map<string, SubjectUpdate>();
  let written = 0;
  for (const field of fields) {
    const truth = await computeTruth(db, field, subjects, session);
    for (const subject of subjects) {
      const key = `${field.at.collection}|${String(subject._id)}`;
      const update = updates.get(key) ?? {
        collection: field.at.collection,
        filter: { _id: subject._id, ...locationFilter(field.at) },
        set: {},
      };
      updates.set(key, update);
      const value = truth.get(String(subject._id));
      const unchanged =
        subject._computed !== undefined &&
        field.name in subject._computed &&
        sameValue(subject._computed[field.name], value);
      if (unchanged) continue;
      update.set[`${COMPUTED_ROOT}.${field.name}`] = value;
      written++;
    }
  }
  const byCollection = new Map<
    string,
    AnyBulkWriteOperation<RevisedSubject>[]
  >();
  for (const { collection, filter, set } of updates.values()) {
    const changed = Object.keys(set).length > 0;
    if (!changed && !revise) continue;
    const update: UpdateFilter<RevisedSubject> = {};
    if (changed) update.$set = set;
    if (revise) update.$inc = { [`${COMPUTED_ROOT}.${COMPUTED_REVISION}`]: 1 };
    const operations = byCollection.get(collection) ?? [];
    operations.push({ updateOne: { filter, update } });
    byCollection.set(collection, operations);
  }
  for (const [collection, operations] of byCollection) {
    await db
      .collection<RevisedSubject>(collection)
      .bulkWrite(operations, { session, ordered: true });
  }
  return written;
}

function inBatchTransaction<T>(
  db: Db,
  work: (session?: ClientSession) => Promise<T>,
): Promise<T> {
  const { withSession } = getSessionContext(db.client);
  return retryOnWriteConflict(() => withSession(work), { maxRetries: 8 });
}

function subjectFields(
  topology: ComputedTopology,
  subject: string,
  names?: readonly string[],
): readonly ComputedField[] {
  const fields = names
    ? names.map((name) => topology.field(subject, name))
    : topology.fieldsOf(subject);
  if (fields.length === 0)
    throw new Error(`"${subject}" has no computed field`);
  return fields;
}

async function readSubjects(
  db: Db,
  field: ComputedField,
  options: { scope?: string; after?: DocumentId; limit: number },
  session?: ClientSession,
): Promise<ComputedSubject[]> {
  const filter: Filter<StoredDocument> = {
    ...locationFilter(field.at),
    ...(options.scope !== undefined && { _scope: options.scope }),
    ...(options.after !== undefined && { _id: { $gt: options.after } }),
  };
  return toSubjects(
    await storedCollection(db, field.at.collection)
      .find(filter, {
        session,
        projection: { _id: 1, _scope: 1, [COMPUTED_ROOT]: 1 },
        sort: { _id: 1 },
        limit: options.limit,
      })
      .toArray(),
  );
}

export interface ApplyComputedOptions {
  readonly subject: string;
  readonly fields?: readonly string[];
  readonly scope?: string;
  readonly batchSize?: number;
}

export interface ApplyComputedResult {
  readonly subjects: number;
  readonly written: number;
  readonly batches: number;
}

export async function applyComputed(
  db: Db,
  topology: ComputedTopology,
  options: ApplyComputedOptions,
): Promise<ApplyComputedResult> {
  const fields = subjectFields(topology, options.subject, options.fields);
  const batchSize = options.batchSize ?? 100;
  let after: DocumentId | undefined;
  let subjects = 0;
  let written = 0;
  let batches = 0;
  for (;;) {
    const batch = await inBatchTransaction(db, async (session) => {
      const read = await readSubjects(
        db,
        fields[0]!,
        { scope: options.scope, after, limit: batchSize },
        session,
      );
      const count = await recomputeSubjects(db, fields, read, session);
      return { read, count };
    });
    if (batch.read.length === 0) break;
    batches++;
    subjects += batch.read.length;
    written += batch.count;
    after = batch.read[batch.read.length - 1]!._id;
    if (batch.read.length < batchSize) break;
  }
  return { subjects, written, batches };
}

export interface CheckComputedOptions {
  readonly subject?: string;
  readonly fields?: readonly string[];
  readonly scope?: string;
  readonly batchSize?: number;
  readonly limit?: number;
}

export interface CheckComputedResult {
  readonly checked: number;
  readonly drifts: readonly ComputedDrift[];
  readonly complete: boolean;
}

export async function checkComputed(
  db: Db,
  topology: ComputedTopology,
  options: CheckComputedOptions = {},
): Promise<CheckComputedResult> {
  const subjectTypes = options.subject
    ? [options.subject]
    : [...new Set(topology.fields.map((field) => field.subject))];
  const batchSize = options.batchSize ?? 100;
  const limit = options.limit ?? Number.POSITIVE_INFINITY;
  const drifts: ComputedDrift[] = [];
  let checked = 0;
  for (const subjectType of subjectTypes) {
    const fields = subjectFields(topology, subjectType, options.fields);
    let after: DocumentId | undefined;
    for (;;) {
      if (checked >= limit) return { checked, drifts, complete: false };
      const size = Math.min(batchSize, limit - checked);
      const found = await inBatchTransaction(db, async (session) => {
        const read = await readSubjects(
          db,
          fields[0]!,
          { scope: options.scope, after, limit: size },
          session,
        );
        const batchDrifts: ComputedDrift[] = [];
        for (const field of fields) {
          const truth = await computeTruth(db, field, read, session);
          for (const subject of read) {
            const missing =
              !subject._computed || !(field.name in subject._computed);
            const stored = subject._computed?.[field.name];
            const expected = truth.get(String(subject._id));
            if (missing || !sameValue(stored, expected)) {
              batchDrifts.push({
                subject: subjectType,
                field: field.name,
                id: subject._id,
                stored,
                truth: expected,
                missing,
              });
            }
          }
        }
        return { read, batchDrifts };
      });
      drifts.push(...found.batchDrifts);
      checked += found.read.length;
      if (found.read.length < size) break;
      after = found.read[found.read.length - 1]!._id;
    }
  }
  return { checked, drifts, complete: true };
}

export async function repairComputed(
  db: Db,
  topology: ComputedTopology,
  drifts: readonly ComputedDrift[],
): Promise<number> {
  let written = 0;
  const bySubject = new Map<string, ComputedDrift[]>();
  for (const drift of drifts)
    bySubject.set(drift.subject, [
      ...(bySubject.get(drift.subject) ?? []),
      drift,
    ]);
  for (const [subjectType, subjectDrifts] of bySubject) {
    const fields = subjectFields(topology, subjectType, [
      ...new Set(subjectDrifts.map((drift) => drift.field)),
    ]);
    const ids = [
      ...new Map(
        subjectDrifts.map((drift) => [keyOf(drift.id), drift.id]),
      ).values(),
    ];
    written += await inBatchTransaction(db, async (session) => {
      const read = toSubjects(
        await storedCollection(db, fields[0]!.at.collection)
          .find(
            {
              ...locationFilter(fields[0]!.at),
              _id: { $in: ids },
            },
            {
              session,
              projection: { _id: 1, _scope: 1, [COMPUTED_ROOT]: 1 },
            },
          )
          .toArray(),
      );
      return await recomputeSubjects(db, fields, read, session);
    });
  }
  return written;
}
