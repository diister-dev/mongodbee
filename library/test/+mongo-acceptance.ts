import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { toMongoValidator } from "../src/validator.ts";

const DOCUMENT_VALIDATION_FAILURE = 121;

export type FieldSchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

export type PipeAction = v.PipeAction<any, any, any>;

export const piped = v.pipe as unknown as (
  schema: FieldSchema,
  ...actions: PipeAction[]
) => FieldSchema;

export interface Verdict {
  probe: unknown;
  valibot: boolean;
  mongo: boolean;
}

export interface Acceptance {
  stricter: Verdict[];
  looser: Verdict[];
}

export async function compareAcceptance(
  db: Db,
  collectionName: string,
  field: FieldSchema,
  probes: readonly unknown[],
): Promise<Acceptance> {
  const schema = v.object({ x: field });
  await db.createCollection(collectionName, {
    validator: toMongoValidator(schema),
  });
  const raw = db.collection(collectionName);
  const verdicts: Verdict[] = [];
  for (const probe of probes) {
    const parsed = v.safeParse(schema, { x: probe });
    const stored = parsed.success ? parsed.output.x : probe;
    let mongo = true;
    try {
      await raw.insertOne({ x: stored });
    } catch (error) {
      if ((error as { code?: unknown }).code !== DOCUMENT_VALIDATION_FAILURE) {
        throw error;
      }
      mongo = false;
    }
    verdicts.push({ probe, valibot: parsed.success, mongo });
  }
  return {
    stricter: verdicts.filter((verdict) => verdict.valibot && !verdict.mongo),
    looser: verdicts.filter((verdict) => !verdict.valibot && verdict.mongo),
  };
}

export function permutations<T>(items: readonly T[]): T[][] {
  if (items.length <= 1) return [[...items]];
  return items.flatMap((item, index) =>
    permutations([...items.slice(0, index), ...items.slice(index + 1)]).map(
      (rest) => [item, ...rest],
    ),
  );
}
