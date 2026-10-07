import { ObjectId } from "mongodb";
import { errorWithSafeMessage } from "./telemetry.ts";

export interface InsertedBatch {
  readonly ids: readonly unknown[];
  readonly stored: readonly Record<string, unknown>[];
}

export interface ReturningPlan<T> {
  readonly label: string;
  readonly computed: boolean;
  readonly decode: (document: unknown) => T;
  readonly readBack: (
    ids: readonly unknown[],
  ) => Promise<readonly Record<string, unknown>[]>;
}

function idKey(id: unknown): string {
  return id instanceof ObjectId ? id.toHexString() : String(id);
}

/**
 * The documents an insert wrote, decoded the way a read decodes them. A type
 * without computed fields returns what it stored, with no read; a type with
 * computed fields is read back, since the write interceptor sets those values.
 */
export async function returnInserted<T>(
  plan: ReturningPlan<T>,
  inserted: InsertedBatch,
): Promise<T[]> {
  if (!plan.computed) return inserted.stored.map(plan.decode);
  const byId = new Map(
    (await plan.readBack(inserted.ids)).map((raw) => [idKey(raw._id), raw]),
  );
  return inserted.ids.map((id) => {
    const raw = byId.get(idKey(id));
    if (!raw) {
      throw errorWithSafeMessage(
        `${plan.label}: the inserted document ${idKey(id)} is missing`,
        `${plan.label}: an inserted document is missing`,
      );
    }
    return plan.decode(raw);
  });
}
