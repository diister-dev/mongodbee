import * as v from "../schema.ts";
import { createMockGenerator, SKIP } from "@diister/valibot-mock";
import { INDEX_SYMBOL } from "../indexes.ts";
import { extractIdPrefix } from "../migration/utils/seed-id.ts";
import type {
  SchemaContent,
  SchemasDefinition,
  TypeSource,
} from "../migration/types.ts";
import { fieldsOf as fieldsOfSource } from "../type-definition.ts";
import {
  defaultTreatments,
  type PrivacyPath,
  type PrivacyPlan,
  type PrivacyTarget,
} from "./plan.ts";
import {
  collectActions,
  type PrivacyConsistency,
  type PrivacyNormalize,
  type PrivacyRole,
  type PrivacyTreatments,
} from "./metadata.ts";
import {
  canonical,
  defaultTimeShiftMs,
  hmacSeed,
  isObjectId,
  looksLikeId,
  type PrivacySecret,
  remapId,
  remapObjectId,
} from "./pseudonym.ts";
import {
  DROP,
  walkDocument,
  type WalkKey,
  type WalkLeaf,
  type WalkNoteKind,
} from "./walk.ts";

export const SKIP_RECOMPUTE: unique symbol = Symbol(
  "mongodbee.privacy.skip-recompute",
);

export const SKIP_DYNAMIC: unique symbol = Symbol(
  "mongodbee.privacy.skip-dynamic",
);

export interface DynamicUnit {
  readonly target: string;
  readonly root: string;
  readonly unit: string;
  readonly key: string;
  readonly keys: readonly string[];
  readonly value: unknown;
  readonly doc: Record<string, unknown>;
  readonly scope: string;
}

export interface DynamicClassification {
  readonly role?: PrivacyRole;
  readonly treatment?: PrivacyTreatments;
  readonly consistent?: PrivacyConsistency;
  readonly space?: string;
  readonly schema?: unknown;
}

export type DynamicResolution = Record<string, DynamicClassification>;

export interface RecomputeContext {
  readonly target: string;
  readonly path: string;
  readonly key: string;
  readonly original: unknown;
  readonly doc: Record<string, unknown>;
}

export interface PrivacyTransformerOptions {
  readonly plan: PrivacyPlan;
  readonly schemas: SchemasDefinition;
  readonly secret: PrivacySecret;
  readonly consistency?: PrivacyConsistency;
  readonly timeShiftMs?: number;
  readonly recompute?: (context: RecomputeContext) => unknown;
  readonly resolveDynamic?: (
    unit: DynamicUnit,
  ) => DynamicResolution | typeof SKIP_DYNAMIC;
  readonly validate?: boolean;
}

export type TransformNoteKind =
  | WalkNoteKind
  | "dropped"
  | "opaque"
  | "generated_required"
  | "recompute_missing"
  | "unclassified"
  | "unresolved"
  | "input_invalid"
  | "invalid"
  | "collision"
  | "mismatch";

export interface TransformNote {
  readonly path: string;
  readonly kind: TransformNoteKind;
  readonly message?: string;
}

export interface TransformResult {
  readonly doc: Record<string, unknown>;
  readonly notes: readonly TransformNote[];
}

export interface TransformContext {
  readonly scope?: string;
}

export interface PrivacyTransformer {
  transform(
    targetKey: string,
    doc: Record<string, unknown>,
    context?: TransformContext,
  ): TransformResult;
}

const MAX_COLLISION_ATTEMPTS = 32;

const TEMPORAL_PATTERN =
  /^(\d{4}-\d{2}-\d{2})(?:T(\d{2}:\d{2})(?::(\d{2})(?:\.(\d+))?)?)?(Z|[+-]\d{2}:\d{2})?$/;

const KEY_VOCABULARY_TYPES: ReadonlySet<string> = new Set([
  "picklist",
  "literal",
  "enum",
]);

function mapTemporal(
  value: string,
  map: (wallClock: number) => number,
): string | undefined {
  const match = TEMPORAL_PATTERN.exec(value);
  if (!match) return undefined;
  const [, date, time, seconds, fraction, zone] = match;
  const wall = Date.parse(`${date}T${time ?? "00:00"}:${seconds ?? "00"}Z`);
  if (Number.isNaN(wall)) return undefined;
  const iso = new Date(map(wall)).toISOString();
  let out = iso.slice(0, 10);
  if (time !== undefined) {
    out += `T${iso.slice(11, 16)}`;
    if (seconds !== undefined) out += iso.slice(16, 19);
    if (fraction !== undefined) {
      out += `.${iso.slice(20, 23).padEnd(fraction.length, "0").slice(0, fraction.length)}`;
    }
  }
  return out + (zone ?? "");
}

const WRAPPER_TYPES: ReadonlySet<string> = new Set([
  "optional",
  "nullable",
  "nullish",
  "non_optional",
  "non_nullable",
  "non_nullish",
  "undefinedable",
  "exact_optional",
]);

function sourceOf(
  schemas: SchemasDefinition,
  target: PrivacyTarget,
): TypeSource | undefined {
  switch (target.bucket) {
    case "collections":
      return schemas.collections?.[target.collection];
    case "multiCollections":
      return schemas.multiCollections?.[target.collection]?.[target.type ?? ""];
    case "multiModels":
      return schemas.multiModels?.[target.collection]?.[target.type ?? ""];
    case "scopedMultiCollections":
      return schemas.scopedMultiCollections?.[target.collection]?.types[
        target.type ?? ""
      ];
  }
}

export function fieldsOf(
  schemas: SchemasDefinition,
  target: PrivacyTarget,
): SchemaContent | undefined {
  const source = sourceOf(schemas, target);
  return source === undefined ? undefined : fieldsOfSource(source);
}

function unwrapSchema(schema: unknown): Record<string, unknown> | undefined {
  let current = schema as Record<string, unknown> | undefined;
  while (current && WRAPPER_TYPES.has(current.type as string)) {
    current = current.wrapped as Record<string, unknown>;
  }
  return current;
}

function rawSchemaAtPath(fields: SchemaContent, path: string): unknown {
  const segments = path.split(".");
  let current: unknown = fields[segments[0]];
  for (const segment of segments.slice(1)) {
    const schema = unwrapSchema(current);
    if (!schema) return undefined;
    const type = schema.type as string;
    if (segment === "*") {
      current =
        type === "array"
          ? schema.item
          : type === "record"
            ? schema.value
            : undefined;
      continue;
    }
    if (type === "union" || type === "variant") {
      const options = schema.options as Record<string, unknown>[];
      const option = options.find((o) => {
        const inner = unwrapSchema(o);
        return (
          inner &&
          (inner.entries as Record<string, unknown> | undefined)?.[segment] !==
            undefined
        );
      });
      current = option
        ? (
            unwrapSchema(option)?.entries as Record<string, unknown> | undefined
          )?.[segment]
        : undefined;
      continue;
    }
    current = (schema.entries as Record<string, unknown> | undefined)?.[
      segment
    ];
  }
  return current;
}

export function schemaAtPath(fields: SchemaContent, path: string): unknown {
  return unwrapSchema(rawSchemaAtPath(fields, path));
}

function hasUniqueIndex(schema: unknown): boolean {
  return collectActions(schema).some((action) => {
    const metadata = (action as { metadata?: Record<PropertyKey, unknown> })
      .metadata;
    const index = metadata?.[INDEX_SYMBOL] as { unique?: boolean } | undefined;
    return index?.unique === true;
  });
}

type SeededGenerator = (seed: number) => unknown;

const generatorCache = new WeakMap<object, Map<string, SeededGenerator>>();

function seededGenerator(schema: unknown, key: string): SeededGenerator {
  const owner = schema as object;
  let byKey = generatorCache.get(owner);
  if (!byKey) {
    byKey = new Map();
    generatorCache.set(owner, byKey);
  }
  let cached = byKey.get(key);
  if (!cached) {
    let pendingSeed: number | undefined;
    // valibot-mock has no reseed API; its resolve hook is the only way to reach the Faker
    const generator = createMockGenerator(
      v.object({ [key]: schema as v.GenericSchema }),
      {
        resolve: ({ faker }) => {
          if (pendingSeed !== undefined) {
            faker.seed(pendingSeed);
            pendingSeed = undefined;
          }
          return SKIP;
        },
      },
    );
    cached = (seed) => {
      pendingSeed = seed;
      return (generator.generate() as Record<string, unknown>)[key];
    };
    byKey.set(key, cached);
  }
  return cached;
}

function issuePath(fields: SchemaContent, issue: v.BaseIssue<unknown>): string {
  const parts: string[] = [];
  for (const item of issue.path ?? []) {
    const parent =
      parts.length === 0
        ? undefined
        : unwrapSchema(rawSchemaAtPath(fields, parts.join(".")));
    const key = String(item.key);
    if (parts.length === 0) {
      parts.push(key);
    } else if (parent?.type === "array" || parent?.type === "tuple") {
      parts.push("*");
    } else if (parent !== undefined && (parent.entries as object | undefined)) {
      parts.push(Object.hasOwn(parent.entries as object, key) ? key : "*");
    } else {
      parts.push("*");
    }
  }
  return parts.join(".");
}

function issueMessage(issue: v.BaseIssue<unknown>): string {
  return `${issue.kind} ${issue.type} expected ${issue.expected ?? "-"}`;
}

function getAt(doc: Record<string, unknown>, keys: readonly string[]): unknown {
  let current: unknown = doc;
  for (const key of keys) {
    if (current === null || typeof current !== "object") return undefined;
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

function applyNormalize(
  value: unknown,
  normalize: PrivacyNormalize | undefined,
): unknown {
  if (typeof value !== "string" || normalize === undefined) return value;
  return normalize === "lowercase" ? value.toLowerCase() : value.trim();
}

export function createPrivacyTransformer(
  options: PrivacyTransformerOptions,
): PrivacyTransformer {
  const { plan, schemas, secret } = options;
  const consistency = options.consistency ?? "relationship";
  const strict =
    (plan as PrivacyPlan & { posture?: string }).posture === "strict";
  const shift = Math.round(
    options.timeShiftMs ?? (strict ? defaultTimeShiftMs(secret) : 0),
  );
  const validate = options.validate ?? true;

  interface MirrorSource {
    readonly cls: PrivacyPath;
    readonly schema: unknown;
    readonly path: string;
    readonly scoped: boolean;
    readonly unique: boolean;
  }

  const sources = new Map<string, MirrorSource | undefined>();

  const findSource = (mirror: string): MirrorSource | undefined => {
    if (sources.has(mirror)) return sources.get(mirror);
    const [space, ...rest] = mirror.split(".");
    const path = rest.join(".");
    const candidates = [...plan.targets.values()].filter(
      (t) => t.space === space,
    );
    candidates.sort((a, b) => Number(b.person) - Number(a.person));
    let found: MirrorSource | undefined;
    for (const target of candidates) {
      const cls = target.paths.find((p) => p.path === path);
      const fields = fieldsOf(schemas, target);
      if (cls && fields) {
        const raw = rawSchemaAtPath(fields, path);
        found = {
          cls,
          schema: unwrapSchema(raw),
          path,
          scoped: target.bucket === "scopedMultiCollections",
          unique: hasUniqueIndex(raw),
        };
        break;
      }
    }
    sources.set(mirror, found);
    return found;
  };

  const uniquePaths = new Map<string, boolean>();

  const isUnique = (
    targetKey: string,
    fields: SchemaContent,
    path: string,
  ): boolean => {
    const id = `${targetKey}|${path}`;
    let unique = uniquePaths.get(id);
    if (unique === undefined) {
      unique = hasUniqueIndex(rawSchemaAtPath(fields, path));
      uniquePaths.set(id, unique);
    }
    return unique;
  };

  const assigned = new Map<string, unknown>();
  const owners = new Map<string, Map<string, string>>();

  const foldFake = (value: unknown): string =>
    typeof value === "string"
      ? value.toLowerCase()
      : (JSON.stringify(value) ?? String(value));

  const pathIndexes = new Map<string, Map<string, PrivacyPath>>();

  const pathsOf = (target: PrivacyTarget): Map<string, PrivacyPath> => {
    let index = pathIndexes.get(target.key);
    if (!index) {
      index = new Map(target.paths.map((p) => [p.path, p]));
      pathIndexes.set(target.key, index);
    }
    return index;
  };

  const shiftDate = (value: unknown): unknown => {
    if (shift === 0) return value;
    if (value instanceof Date) return new Date(value.getTime() + shift);
    if (typeof value === "string") {
      return mapTemporal(value, (wall) => wall + shift) ?? value;
    }
    return value;
  };

  const monthStart = (ms: number): number => {
    const d = new Date(ms);
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1);
  };

  function transform(
    targetKey: string,
    doc: Record<string, unknown>,
    context: TransformContext = {},
  ): TransformResult {
    const target = plan.targets.get(targetKey);
    if (!target) throw new Error(`privacy: unknown target "${targetKey}"`);
    const fields = fieldsOf(schemas, target);
    if (!fields) {
      throw new Error(`privacy: no schema for target "${targetKey}"`);
    }

    const byPath = pathsOf(target);
    const notes: TransformNote[] = [];
    const scope =
      context.scope ?? (typeof doc._scope === "string" ? doc._scope : "");
    const docId = isObjectId(doc._id)
      ? doc._id.toHexString()
      : typeof doc._id === "string"
        ? doc._id
        : "";

    const scopePart = (
      cls: PrivacyPath,
      path: string,
      sourceScope: string = scope,
    ): string => {
      const policy = cls.consistent ?? consistency;
      if (policy === "person") return "";
      if (policy === "relationship") return sourceScope;
      return `${docId}|${path}`;
    };
    const spaceOf = (cls: PrivacyPath): string => cls.space ?? cls.role;
    const valueMessage = (
      cls: PrivacyPath,
      path: string,
      value: unknown,
      sourceScope?: string,
    ): string =>
      `value|${spaceOf(cls)}|${scopePart(cls, path, sourceScope)}|${canonical(value)}`;

    const generate = (
      schema: unknown,
      seed: number,
      path: string,
      key = path.split(".").pop() ?? path,
    ): unknown => {
      try {
        return seededGenerator(schema, key)(seed);
      } catch (error) {
        notes.push({
          path,
          kind: "dropped",
          message: error instanceof Error ? error.message : String(error),
        });
        return DROP;
      }
    };

    const produce = (
      ownerSpace: string,
      message: string,
      unique: boolean,
      schema: unknown,
      path: string,
      key?: string,
    ): unknown => {
      const known = assigned.get(message);
      if (known !== undefined) return known;
      if (!unique)
        return generate(schema, hmacSeed(secret, message), path, key);
      let taken = owners.get(ownerSpace);
      if (!taken) {
        taken = new Map();
        owners.set(ownerSpace, taken);
      }
      let produced: unknown = DROP;
      for (let attempt = 0; attempt < MAX_COLLISION_ATTEMPTS; attempt++) {
        const seed = hmacSeed(
          secret,
          attempt === 0 ? message : `${message}|retry|${attempt}`,
        );
        produced = generate(schema, seed, path, key);
        if (produced === DROP) return DROP;
        const holder = taken.get(foldFake(produced));
        if (holder === undefined || holder === message) {
          taken.set(foldFake(produced), message);
          assigned.set(message, produced);
          return produced;
        }
      }
      notes.push({ path, kind: "collision" });
      return produced;
    };

    const fakeMessage = (path: string): string =>
      `fake|${targetKey}|${docId}|${path}`;

    const fakeValue = (leaf: WalkLeaf, schema: unknown = leaf.schema) =>
      produce(
        `fake|${targetKey}|${leaf.path}`,
        fakeMessage(leaf.path),
        isUnique(targetKey, fields, leaf.path),
        schema,
        leaf.path,
      );

    const dropOrGenerate = (
      leaf: WalkLeaf,
      kind: "dropped" | "opaque" | "recompute_missing" | "mismatch",
    ): unknown => {
      notes.push({ path: leaf.path, kind });
      if (leaf.optional) return DROP;
      if (leaf.nullable) return null;
      notes.push({ path: leaf.path, kind: "generated_required" });
      return fakeValue(leaf);
    };

    const generalise = (leaf: WalkLeaf): unknown => {
      const { value } = leaf;
      if (value instanceof Date) {
        return new Date(monthStart(value.getTime() + shift));
      }
      if (typeof value === "string") {
        const collapsed = mapTemporal(value, (wall) =>
          monthStart(wall + shift),
        );
        if (collapsed !== undefined) return collapsed;
      }
      if (typeof value === "number") {
        if (value === 0) return 0;
        const magnitude = 10 ** Math.floor(Math.log10(Math.abs(value)));
        return Math.round(value / magnitude) * magnitude;
      }
      return dropOrGenerate(leaf, "dropped");
    };

    const dynamicRoots = target.paths
      .filter((p) => p.role === "dynamic")
      .map((p) => p.path);
    const resolutions = new Map<
      string,
      DynamicResolution | typeof SKIP_DYNAMIC
    >();

    const resolveOverride = (
      leaf: WalkLeaf,
    ): DynamicClassification | undefined => {
      const root = dynamicRoots.find((r) => leaf.path.startsWith(`${r}.`));
      if (root === undefined || !options.resolveDynamic) return undefined;
      const rootLength = root.split(".").length;
      const segments = leaf.path.split(".");
      const unitLength =
        segments[rootLength] === "*" ? rootLength + 1 : rootLength;
      const unitKeys = leaf.keys.slice(0, unitLength);
      const unitPath = unitKeys.join(".");
      let resolution = resolutions.get(unitPath);
      if (resolution === undefined) {
        resolution = options.resolveDynamic({
          target: targetKey,
          root,
          unit: segments.slice(0, unitLength).join("."),
          key: unitKeys[unitKeys.length - 1] ?? "",
          keys: unitKeys,
          value: getAt(doc, unitKeys),
          doc,
          scope,
        });
        resolutions.set(unitPath, resolution);
      }
      if (resolution === SKIP_DYNAMIC) return undefined;
      return resolution[segments.slice(unitLength).join(".")];
    };

    const handler = (leaf: WalkLeaf): unknown => {
      const found = byPath.get(leaf.path);
      if (!found) {
        notes.push({ path: leaf.path, kind: "unclassified" });
        return dropOrGenerate(leaf, "dropped");
      }
      const override = resolveOverride(leaf);
      const cls: PrivacyPath = override
        ? {
            ...found,
            tier: "declared",
            role: override.role ?? found.role,
            ...(override.consistent !== undefined && {
              consistent: override.consistent,
            }),
            ...(override.space !== undefined && { space: override.space }),
            treatment: {
              ...defaultTreatments(override.role ?? found.role),
              ...override.treatment,
            },
          }
        : found;
      const schema = override?.schema ?? leaf.schema;
      if (!override && cls.tier === "dynamic") {
        notes.push({ path: leaf.path, kind: "unresolved" });
        return dropOrGenerate(leaf, "dropped");
      }
      if (cls.mirrorOf !== undefined) {
        const source = findSource(cls.mirrorOf);
        if (!source || source.schema === undefined) {
          return dropOrGenerate(leaf, "dropped");
        }
        const generated = produce(
          spaceOf(source.cls),
          valueMessage(
            source.cls,
            source.path,
            leaf.value,
            source.scoped ? scope : "",
          ),
          source.unique,
          source.schema,
          leaf.path,
          source.path.split(".").pop(),
        );
        return generated === DROP
          ? DROP
          : applyNormalize(generated, cls.normalize);
      }
      switch (cls.treatment.extract) {
        case "keep":
        case "include": {
          const kept = shiftDate(leaf.value);
          return v.is(schema as v.GenericSchema, kept)
            ? kept
            : dropOrGenerate(leaf, "mismatch");
        }
        case "remap": {
          if (looksLikeId(leaf.value)) {
            return remapId(secret, leaf.value, shift);
          }
          if (isObjectId(leaf.value)) {
            return remapObjectId(secret, leaf.value, shift);
          }
          notes.push({ path: leaf.path, kind: "mismatch" });
          return produce(
            spaceOf(cls),
            valueMessage(cls, leaf.path, leaf.value),
            false,
            schema,
            leaf.path,
          );
        }
        case "pseudonym":
          return produce(
            spaceOf(cls),
            valueMessage(cls, leaf.path, leaf.value),
            !override && isUnique(targetKey, fields, leaf.path),
            schema,
            leaf.path,
          );
        case "fake":
          return fakeValue(leaf, schema);
        case "generalise":
          return generalise(leaf);
        case "recompute": {
          if (options.recompute) {
            const r = options.recompute({
              target: targetKey,
              path: leaf.path,
              key: leaf.key,
              original: leaf.value,
              doc,
            });
            if (r !== SKIP_RECOMPUTE) return r;
          }
          return dropOrGenerate(leaf, "recompute_missing");
        }
        case "opaque":
          return dropOrGenerate(leaf, "opaque");
        default:
          return dropOrGenerate(leaf, "dropped");
      }
    };

    const { _id, _scope, _type, ...rest } = doc;
    if (validate) {
      const parsed = v.safeParse(v.object(fields as v.ObjectEntries), doc);
      if (!parsed.success) {
        for (const issue of parsed.issues) {
          notes.push({
            path: issuePath(fields, issue),
            kind: "input_invalid",
            message: issueMessage(issue),
          });
        }
      }
    }
    const keyIsVocabulary = (schema: unknown): boolean =>
      collectActions(schema).some(
        (a) =>
          (a as { kind?: string }).kind === "schema" &&
          KEY_VOCABULARY_TYPES.has((a as { type: string }).type),
      );

    const mapKey = ({ path, key, schema }: WalkKey): string => {
      if (dynamicRoots.some((r) => path === r || path.startsWith(`${r}.`))) {
        return key;
      }
      if (schema !== undefined && keyIsVocabulary(schema)) return key;
      if (
        schema !== undefined &&
        extractIdPrefix(schema) !== "" &&
        looksLikeId(key)
      ) {
        return remapId(secret, key, shift);
      }
      const mapped = produce(
        `key|${targetKey}|${path}`,
        `key|${targetKey}|${path}|${key}`,
        true,
        schema ?? v.string(),
        path,
        "key",
      );
      return typeof mapped === "string"
        ? mapped
        : hmacSeed(secret, `key|${targetKey}|${path}|${key}`).toString(36);
    };

    const walked = walkDocument(fields, rest, handler, { mapKey });
    notes.push(...walked.notes);

    const out: Record<string, unknown> = { ...walked.doc };
    const identifier = (value: unknown, path: string): unknown => {
      if (typeof value === "string") return remapId(secret, value, shift);
      if (isObjectId(value)) return remapObjectId(secret, value, shift);
      if (
        value === undefined ||
        value === null ||
        typeof value === "number" ||
        typeof value === "bigint" ||
        typeof value === "boolean"
      ) {
        return value;
      }
      notes.push({ path, kind: "dropped" });
      return undefined;
    };
    const mappedId = identifier(_id, "_id");
    if (mappedId !== undefined) out._id = mappedId;
    const mappedScope = identifier(_scope, "_scope");
    if (mappedScope !== undefined) out._scope = mappedScope;
    if (_type !== undefined) out._type = _type;

    if (validate) {
      const parsed = v.safeParse(v.object(fields as v.ObjectEntries), out);
      if (!parsed.success) {
        for (const issue of parsed.issues) {
          notes.push({
            path: issuePath(fields, issue),
            kind: "invalid",
            message: issueMessage(issue),
          });
        }
      }
    }

    return { doc: out, notes };
  }

  return { transform };
}
