import * as v from "../schema.ts";
import { dbId } from "../ids.ts";

export const PRIVACY_SYMBOL: unique symbol = Symbol("mongodbee.privacy");

export type PrivacyRole =
  | "direct"
  | "quasi"
  | "sensitive"
  | "contact"
  | "content"
  | "technical"
  | "derived"
  | "external";

export type PrivacyDirection = "extract" | "erase" | "export";

export type PrivacyTreatment =
  | "pseudonym"
  | "remap"
  | "fake"
  | "generalise"
  | "drop"
  | "keep"
  | "opaque"
  | "recompute"
  | "include"
  | "exclude";

export type PrivacyTreatments = Partial<
  Record<PrivacyDirection, PrivacyTreatment>
>;

export type PrivacyNormalize = "lowercase" | "trim";

export type PrivacyConsistency = "person" | "relationship" | "transaction";

export type PrivacyMetadata =
  | { readonly kind: "person"; readonly of: readonly string[] }
  | { readonly kind: "owner"; readonly of: readonly string[] }
  | {
    readonly kind: "field";
    readonly role: PrivacyRole;
    readonly space?: string;
    readonly treatment?: PrivacyTreatments;
    readonly consistent?: PrivacyConsistency;
    readonly relation?: "mention";
  }
  | {
    readonly kind: "mirror";
    readonly source: string;
    readonly normalize?: PrivacyNormalize;
  }
  | { readonly kind: "exempt"; readonly reason: string }
  | { readonly kind: "dynamic" };

type Schema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

type Tagged<T extends Schema> = v.SchemaWithPipe<
  readonly [
    T,
    v.MetadataAction<v.InferOutput<T>, {
      readonly [PRIVACY_SYMBOL]: PrivacyMetadata;
    }>,
  ]
>;

function tag<T extends Schema>(
  schema: T,
  metadata: PrivacyMetadata,
): Tagged<T> {
  return v.pipe(schema, v.metadata({ [PRIVACY_SYMBOL]: metadata }));
}

function list(
  value: string | readonly string[] | undefined,
): readonly string[] {
  if (value === undefined) return [];
  return typeof value === "string" ? [value] : [...value];
}

export interface PersonIdOptions {
  readonly of?: string | readonly string[];
}

export function personId(
  type: string,
  options: PersonIdOptions = {},
): Tagged<ReturnType<typeof dbId>> {
  return tag(dbId(type), { kind: "person", of: list(options.of) });
}

export interface PersonalFieldOptions {
  readonly role: PrivacyRole;
  readonly space?: string;
  readonly treatment?: PrivacyTreatments;
  readonly consistent?: PrivacyConsistency;
}

export interface PersonalOwnerOptions {
  readonly of: string | readonly string[];
}

export type PersonalOptions = PersonalFieldOptions | PersonalOwnerOptions;

export function personal<T extends Schema>(
  schema: T,
  options: PersonalOptions,
): Tagged<T> {
  if ("of" in options) {
    return tag(schema, { kind: "owner", of: list(options.of) });
  }
  const metadata: PrivacyMetadata = {
    kind: "field",
    role: options.role,
    ...(options.space !== undefined && { space: options.space }),
    ...(options.treatment !== undefined && { treatment: options.treatment }),
    ...(options.consistent !== undefined && { consistent: options.consistent }),
  };
  return tag(schema, metadata);
}

export function notPersonal<T extends Schema>(
  schema: T,
  reason: string,
): Tagged<T> {
  return tag(schema, { kind: "exempt", reason });
}

export function mention<T extends Schema>(schema: T): Tagged<T> {
  return tag(schema, { kind: "field", role: "technical", relation: "mention" });
}

export function dynamic<T extends Schema>(schema: T): Tagged<T> {
  return tag(schema, { kind: "dynamic" });
}

export interface MirrorOfOptions {
  readonly normalize?: PrivacyNormalize;
}

export function mirrorOf<T extends Schema>(
  schema: T,
  source: string,
  options: MirrorOfOptions = {},
): Tagged<T> {
  const metadata: PrivacyMetadata = {
    kind: "mirror",
    source,
    ...(options.normalize !== undefined && { normalize: options.normalize }),
  };
  return tag(schema, metadata);
}

export function collectActions(schema: unknown, depth = 0): unknown[] {
  if (depth > 12 || schema === null || typeof schema !== "object") return [];
  const s = schema as Record<string, unknown>;
  const out: unknown[] = [s];
  if (Array.isArray(s.pipe)) {
    for (const item of s.pipe) {
      if (item === schema) continue;
      out.push(...collectActions(item, depth + 1));
    }
  }
  if (s.wrapped !== undefined) {
    out.push(...collectActions(s.wrapped, depth + 1));
  }
  return out;
}

export function readPrivacyMetadata(schema: unknown): PrivacyMetadata[] {
  const found: PrivacyMetadata[] = [];
  for (const action of collectActions(schema)) {
    const a = action as Record<string, unknown>;
    if (a.type !== "metadata") continue;
    const meta = a.metadata as Record<PropertyKey, unknown> | undefined;
    const value = meta?.[PRIVACY_SYMBOL];
    if (value !== undefined) found.push(value as PrivacyMetadata);
  }
  return found;
}
