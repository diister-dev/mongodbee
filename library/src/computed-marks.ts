import type { ClientSession, Document, Filter } from "mongodb";
import type { Db } from "./mongodb.ts";
import { COMPUTED_ROOT } from "./computed.ts";
import {
  type ComputedField,
  type ComputedTopology,
  locationFilter,
} from "./computed-topology.ts";
import {
  applyComputed,
  type ComputedSubject,
  recomputeSubjects,
  whereFilter,
} from "./computed-apply.ts";
import { getSessionContext } from "./session.ts";
import { retryOnWriteConflict } from "./utils/retry.ts";

export const COMPUTED_PENDING_COLLECTION = "__dbee_computed_pending__";

export type ComputedMarkKind = "whole" | "subject" | "far";

export interface ComputedMark {
  readonly _id: string;
  readonly field: string;
  readonly kind: ComputedMarkKind;
  readonly scope: string | null;
  readonly subject?: unknown;
  readonly far?: unknown;
  readonly reason: string;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly generation: number;
  readonly claimedUntil?: Date;
}

export function computedFieldKey(
  field: Pick<ComputedField, "subject" | "name">,
): string {
  return `${field.subject}.${field.name}`;
}

function pending(db: Db) {
  return db.collection<ComputedMark>(COMPUTED_PENDING_COLLECTION);
}

async function upsertMark(
  db: Db,
  identity: Pick<ComputedMark, "_id" | "field" | "kind" | "scope"> & {
    subject?: unknown;
    far?: unknown;
  },
  reason: string,
  session: ClientSession | undefined,
): Promise<void> {
  const now = new Date();
  await pending(db).updateOne(
    { _id: identity._id },
    {
      $setOnInsert: {
        field: identity.field,
        kind: identity.kind,
        scope: identity.scope,
        ...(identity.subject !== undefined && { subject: identity.subject }),
        ...(identity.far !== undefined && { far: identity.far }),
        createdAt: now,
      },
      $set: { reason, updatedAt: now },
      $inc: { generation: 1 },
    },
    { upsert: true, session },
  );
}

export async function markWhole(
  db: Db,
  field: ComputedField,
  scope: string | undefined,
  reason: string,
  session?: ClientSession,
): Promise<void> {
  const key = computedFieldKey(field);
  const bounded = field.scoped && scope !== undefined ? scope : null;
  await upsertMark(
    db,
    {
      _id: `${key}|whole|${bounded ?? "*"}`,
      field: key,
      kind: "whole",
      scope: bounded,
    },
    reason,
    session,
  );
}

export async function markSubjects(
  db: Db,
  field: ComputedField,
  subjects: Iterable<unknown>,
  reason: string,
  session?: ClientSession,
): Promise<void> {
  const key = computedFieldKey(field);
  for (const subject of subjects) {
    await upsertMark(
      db,
      {
        _id: `${key}|subject|${String(subject)}`,
        field: key,
        kind: "subject",
        scope: null,
        subject,
      },
      reason,
      session,
    );
  }
}

export async function markFar(
  db: Db,
  field: ComputedField,
  far: unknown,
  scope: string | undefined,
  reason: string,
  session?: ClientSession,
): Promise<void> {
  const key = computedFieldKey(field);
  const bounded = scope ?? null;
  await upsertMark(
    db,
    {
      _id: `${key}|far|${bounded ?? "*"}|${String(far)}`,
      field: key,
      kind: "far",
      scope: bounded,
      far,
    },
    reason,
    session,
  );
}

export interface DrainComputedOptions {
  readonly topology: ComputedTopology;
  readonly limit?: number;
  readonly batchSize?: number;
  readonly leaseMs?: number;
  readonly afterRecompute?: (mark: ComputedMark) => Promise<void>;
}

export interface DrainComputedResult {
  readonly drained: number;
  readonly requeued: number;
  readonly remaining: number;
  readonly oldestAgeMs: number | null;
}

async function claim(db: Db, leaseMs: number): Promise<ComputedMark | null> {
  const now = new Date();
  return await pending(db).findOneAndUpdate(
    {
      $or: [
        { claimedUntil: { $exists: false } },
        { claimedUntil: { $lt: now } },
      ],
    } as Filter<ComputedMark>,
    { $set: { claimedUntil: new Date(now.getTime() + leaseMs) } },
    { sort: { createdAt: 1, _id: 1 }, returnDocument: "after" },
  );
}

async function release(db: Db, mark: ComputedMark): Promise<void> {
  await pending(db).updateOne(
    { _id: mark._id },
    { $unset: { claimedUntil: "" } },
  );
}

function fieldOf(
  topology: ComputedTopology,
  key: string,
): ComputedField | undefined {
  return topology.fields.find((field) => computedFieldKey(field) === key);
}

async function drainSubject(
  db: Db,
  field: ComputedField,
  mark: ComputedMark,
  afterRecompute?: (mark: ComputedMark) => Promise<void>,
): Promise<boolean> {
  const { withSession } = getSessionContext(db.client);
  return await retryOnWriteConflict(
    () =>
      withSession(async (session) => {
        const subjects = (await db
          .collection(field.at.collection)
          .find(
            {
              ...locationFilter(field.at),
              _id: mark.subject,
            } as Filter<Document>,
            {
              session,
              projection: { _id: 1, _scope: 1, [COMPUTED_ROOT]: 1 },
            },
          )
          .toArray()) as ComputedSubject[];
        await recomputeSubjects(db, [field], subjects, session);
        await afterRecompute?.(mark);
        const removed = await pending(db).deleteOne(
          { _id: mark._id, generation: mark.generation },
          { session },
        );
        return removed.deletedCount === 1;
      }),
    { maxRetries: 8 },
  );
}

async function drainFar(
  db: Db,
  field: ComputedField,
  mark: ComputedMark,
  batchSize: number,
): Promise<void> {
  const { descriptor } = field;
  if (!descriptor.through) return;
  const near = await db
    .collection(field.source.collection)
    .find(
      {
        ...locationFilter(field.source),
        ...whereFilter(descriptor.where),
        [descriptor.through.via]: mark.far,
        ...(field.scoped && mark.scope !== null && { _scope: mark.scope }),
      } as Filter<Document>,
      { projection: { [descriptor.by]: 1 } },
    )
    .toArray();
  const subjects = new Map<string, unknown>();
  for (const document of near) {
    const by = document[descriptor.by];
    for (const id of by === undefined || by === null
      ? []
      : Array.isArray(by)
        ? by
        : [by])
      subjects.set(String(id), id);
  }
  const ids = [...subjects.values()];
  const { withSession } = getSessionContext(db.client);
  for (let start = 0; start < ids.length; start += batchSize) {
    const chunk = ids.slice(start, start + batchSize);
    await retryOnWriteConflict(
      () =>
        withSession(async (session) => {
          const read = (await db
            .collection(field.at.collection)
            .find(
              {
                ...locationFilter(field.at),
                _id: { $in: chunk },
              } as Filter<Document>,
              {
                session,
                projection: { _id: 1, _scope: 1, [COMPUTED_ROOT]: 1 },
              },
            )
            .toArray()) as ComputedSubject[];
          await recomputeSubjects(db, [field], read, session);
        }),
      { maxRetries: 8 },
    );
  }
}

export async function drainComputedPending(
  db: Db,
  options: DrainComputedOptions,
): Promise<DrainComputedResult> {
  const limit = options.limit ?? 100;
  const leaseMs = options.leaseMs ?? 60_000;
  let drained = 0;
  let requeued = 0;
  for (let processed = 0; processed < limit; processed++) {
    const mark = await claim(db, leaseMs);
    if (!mark) break;
    const field = fieldOf(options.topology, mark.field);
    if (!field) {
      await pending(db).deleteOne({
        _id: mark._id,
        generation: mark.generation,
      });
      drained++;
      continue;
    }
    let done: boolean;
    try {
      if (mark.kind === "subject") {
        done = await drainSubject(db, field, mark, options.afterRecompute);
      } else if (mark.kind === "far") {
        await drainFar(db, field, mark, options.batchSize ?? 100);
        await options.afterRecompute?.(mark);
        done =
          (
            await pending(db).deleteOne({
              _id: mark._id,
              generation: mark.generation,
            })
          ).deletedCount === 1;
      } else {
        await applyComputed(db, options.topology, {
          subject: field.subject,
          fields: [field.name],
          scope: mark.scope ?? undefined,
          batchSize: options.batchSize,
        });
        await options.afterRecompute?.(mark);
        done =
          (
            await pending(db).deleteOne({
              _id: mark._id,
              generation: mark.generation,
            })
          ).deletedCount === 1;
      }
    } catch (error) {
      await release(db, mark);
      throw error;
    }
    if (done) {
      drained++;
    } else {
      requeued++;
      await release(db, mark);
    }
  }
  const summary = await pendingComputed(db);
  return {
    drained,
    requeued,
    remaining: summary.count,
    oldestAgeMs: summary.oldestAgeMs,
  };
}

export interface PendingComputedSummary {
  readonly count: number;
  readonly oldestAgeMs: number | null;
  readonly byField: Readonly<Record<string, number>>;
}

export async function pendingComputed(db: Db): Promise<PendingComputedSummary> {
  const groups = await pending(db)
    .aggregate<{ _id: string; count: number; oldest: Date }>([
      {
        $group: {
          _id: "$field",
          count: { $sum: 1 },
          oldest: { $min: "$createdAt" },
        },
      },
    ])
    .toArray();
  const oldest = groups.reduce<Date | null>(
    (min, group) => (min === null || group.oldest < min ? group.oldest : min),
    null,
  );
  return {
    count: groups.reduce((sum, group) => sum + group.count, 0),
    oldestAgeMs: oldest === null ? null : Date.now() - oldest.getTime(),
    byField: Object.fromEntries(
      groups.map((group) => [group._id, group.count]),
    ),
  };
}
