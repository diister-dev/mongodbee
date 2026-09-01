import * as v from "../schema.ts";
import { createMockGenerator } from "@diister/valibot-mock";
import type { SchemaContent, SchemasDefinition } from "../migration/types.ts";
import {
  defaultTreatments,
  type PrivacyPath,
  type PrivacyPlan,
  type PrivacyTarget,
} from "./plan.ts";
import type {
  PrivacyConsistency,
  PrivacyNormalize,
  PrivacyRole,
  PrivacyTreatments,
} from "./metadata.ts";
import {
  canonical,
  hmacSeed,
  looksLikeId,
  type PrivacySecret,
  remapId,
} from "./pseudonym.ts";
import {
  DROP,
  walkDocument,
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
  | "invalid";

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

const ISO_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

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

export function fieldsOf(
  schemas: SchemasDefinition,
  target: PrivacyTarget,
): SchemaContent | undefined {
  switch (target.bucket) {
    case "collections":
      return schemas.collections?.[target.collection];
    case "multiCollections":
      return schemas.multiCollections?.[target.collection]?.[target.type ?? ""];
    case "multiModels":
      return schemas.multiModels?.[target.collection]?.[target.type ?? ""];
    case "scopedMultiCollections":
      return schemas.scopedMultiCollections?.[target.collection]
        ?.types[target.type ?? ""];
  }
}

function unwrapSchema(schema: unknown): Record<string, unknown> | undefined {
  let current = schema as Record<string, unknown> | undefined;
  while (current && WRAPPER_TYPES.has(current.type as string)) {
    current = current.wrapped as Record<string, unknown>;
  }
  return current;
}

export function schemaAtPath(fields: SchemaContent, path: string): unknown {
  const segments = path.split(".");
  let current: unknown = fields[segments[0]];
  for (const segment of segments.slice(1)) {
    const schema = unwrapSchema(current);
    if (!schema) return undefined;
    const type = schema.type as string;
    if (segment === "*") {
      current = type === "array"
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
        return inner &&
          (inner.entries as Record<string, unknown> | undefined)?.[segment] !==
            undefined;
      });
      current = option
        ? (unwrapSchema(option)!.entries as Record<string, unknown>)[segment]
        : undefined;
      continue;
    }
    current = (schema.entries as Record<string, unknown> | undefined)
      ?.[segment];
  }
  return unwrapSchema(current);
}

function getAt(
  doc: Record<string, unknown>,
  keys: readonly string[],
): unknown {
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
  const shift = options.timeShiftMs ?? 0;
  const validate = options.validate ?? true;

  const findSource = (
    mirror: string,
  ): { cls: PrivacyPath; schema: unknown; path: string } | undefined => {
    const [space, ...rest] = mirror.split(".");
    const path = rest.join(".");
    const candidates = [...plan.targets.values()].filter((t) =>
      t.space === space
    );
    candidates.sort((a, b) => Number(b.person) - Number(a.person));
    for (const target of candidates) {
      const cls = target.paths.find((p) => p.path === path);
      const fields = fieldsOf(schemas, target);
      if (cls && fields) {
        return { cls, schema: schemaAtPath(fields, path), path };
      }
    }
    return undefined;
  };

  const shiftDate = (value: unknown): unknown => {
    if (shift === 0) return value;
    if (value instanceof Date) return new Date(value.getTime() + shift);
    if (typeof value === "string" && ISO_PATTERN.test(value)) {
      const t = Date.parse(value);
      if (!Number.isNaN(t)) return new Date(t + shift).toISOString();
    }
    return value;
  };

  const monthStart = (d: Date): Date =>
    new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1));

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

    const byPath = new Map(target.paths.map((p) => [p.path, p]));
    const notes: TransformNote[] = [];
    const scope = context.scope ??
      (typeof doc._scope === "string" ? doc._scope : "");
    const docId = typeof doc._id === "string" ? doc._id : "";

    const scopePart = (cls: PrivacyPath, path: string): string => {
      const policy = cls.consistent ?? consistency;
      if (policy === "person") return "";
      if (policy === "relationship") return scope;
      return `${docId}|${path}`;
    };
    const spaceOf = (cls: PrivacyPath): string => cls.space ?? cls.role;
    const seedFor = (cls: PrivacyPath, path: string, value: unknown): number =>
      hmacSeed(
        secret,
        `value|${spaceOf(cls)}|${scopePart(cls, path)}|${canonical(value)}`,
      );
    const fakeSeed = (path: string): number =>
      hmacSeed(secret, `fake|${targetKey}|${docId}|${path}`);

    const generate = (
      schema: unknown,
      seed: number,
      path: string,
      key = path.split(".").pop() ?? path,
    ): unknown => {
      try {
        const wrapped = v.object({ [key]: schema as v.GenericSchema });
        const produced = createMockGenerator(wrapped, { faker: { seed } })
          .generate() as Record<string, unknown>;
        return produced[key];
      } catch (error) {
        notes.push({
          path,
          kind: "dropped",
          message: error instanceof Error ? error.message : String(error),
        });
        return DROP;
      }
    };

    const dropOrGenerate = (
      leaf: WalkLeaf,
      kind: "dropped" | "opaque" | "recompute_missing",
    ): unknown => {
      notes.push({ path: leaf.path, kind });
      if (leaf.optional) return DROP;
      notes.push({ path: leaf.path, kind: "generated_required" });
      return generate(leaf.schema, fakeSeed(leaf.path), leaf.path);
    };

    const generalise = (leaf: WalkLeaf): unknown => {
      const shifted = shiftDate(leaf.value);
      if (shifted instanceof Date) return monthStart(shifted);
      if (typeof shifted === "string" && ISO_PATTERN.test(shifted)) {
        return monthStart(new Date(Date.parse(shifted))).toISOString();
      }
      if (typeof shifted === "number") {
        if (shifted === 0) return 0;
        const magnitude = 10 ** Math.floor(Math.log10(Math.abs(shifted)));
        return Math.round(shifted / magnitude) * magnitude;
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
      const unitLength = segments[rootLength] === "*"
        ? rootLength + 1
        : rootLength;
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
          ...(override.consistent !== undefined &&
            { consistent: override.consistent }),
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
        const generated = generate(
          source.schema,
          seedFor(source.cls, source.path, leaf.value),
          leaf.path,
          source.path.split(".").pop(),
        );
        return generated === DROP
          ? DROP
          : applyNormalize(generated, cls.normalize);
      }
      switch (cls.treatment.extract) {
        case "keep":
        case "include":
          return shiftDate(leaf.value);
        case "remap":
          return looksLikeId(leaf.value)
            ? remapId(secret, leaf.value, shift)
            : shiftDate(leaf.value);
        case "pseudonym":
          return generate(
            schema,
            seedFor(cls, leaf.path, leaf.value),
            leaf.path,
          );
        case "fake":
          return generate(schema, fakeSeed(leaf.path), leaf.path);
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
          const path = issue.path?.map((p) => String(p.key)).join(".") ?? "";
          notes.push({ path, kind: "input_invalid", message: issue.message });
        }
      }
    }
    const walked = walkDocument(fields, rest, handler);
    notes.push(...walked.notes);

    const out: Record<string, unknown> = { ...walked.doc };
    if (typeof _id === "string") out._id = remapId(secret, _id, shift);
    else if (_id !== undefined) out._id = _id;
    if (typeof _scope === "string") out._scope = remapId(secret, _scope, shift);
    else if (_scope !== undefined) out._scope = _scope;
    if (_type !== undefined) out._type = _type;

    if (validate) {
      const parsed = v.safeParse(v.object(fields as v.ObjectEntries), out);
      if (!parsed.success) {
        for (const issue of parsed.issues) {
          const path = issue.path?.map((p) => String(p.key)).join(".") ?? "";
          notes.push({ path, kind: "invalid", message: issue.message });
        }
      }
    }

    return { doc: out, notes };
  }

  return { transform };
}
