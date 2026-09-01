import type {
  DatabaseState,
  SchemaContent,
  SchemasDefinition,
} from "../migration/types.ts";
import { extractIdPrefix } from "../migration/utils/seed-id.ts";
import { INDEX_SYMBOL } from "../indexes.ts";
import { createSimpleVisitor, SchemaNavigator } from "../schema-navigator.ts";
import {
  collectActions,
  PRIVACY_SYMBOL,
  type PrivacyConsistency,
  type PrivacyDirection,
  type PrivacyMetadata,
  type PrivacyNormalize,
  type PrivacyRole,
  type PrivacyTreatment,
  type PrivacyTreatments,
  readPrivacyMetadata,
} from "./metadata.ts";

export type PrivacyTier =
  | "dynamic"
  | "certain"
  | "inferred"
  | "declared"
  | "unknown"
  | "none";

export type PrivacyPathRole = PrivacyRole | "reference" | "dynamic" | "none";

export type PrivacyRelation = "owner" | "delegation" | "mention" | "relation";

export interface PrivacyPath {
  readonly path: string;
  readonly tier: PrivacyTier;
  readonly role: PrivacyPathRole;
  readonly relation?: PrivacyRelation;
  readonly spaces: readonly string[];
  readonly space?: string;
  readonly consistent?: PrivacyConsistency;
  readonly mirrorOf?: string;
  readonly dynamicRoot?: string;
  readonly normalize?: PrivacyNormalize;
  readonly treatment: Readonly<Record<PrivacyDirection, PrivacyTreatment>>;
  readonly values?: readonly string[];
  readonly note?: string;
}

export type PrivacyOwnerKind =
  | "self"
  | "declared"
  | "inferred"
  | "exempt"
  | "none"
  | "ambiguous";

export interface PrivacyOwner {
  readonly kind: PrivacyOwnerKind;
  readonly spaces: readonly string[];
  readonly via: readonly string[];
  readonly chain: readonly string[];
  readonly reason?: string;
}

export interface PrivacyTarget {
  readonly key: string;
  readonly bucket: keyof DatabaseState;
  readonly collection: string;
  readonly type?: string;
  readonly space: string;
  readonly person: boolean;
  readonly delegatesTo: readonly string[];
  readonly owner: PrivacyOwner;
  readonly paths: readonly PrivacyPath[];
}

export type PrivacyFindingLevel = "error" | "warning" | "info";

export interface PrivacyFinding {
  readonly level: PrivacyFindingLevel;
  readonly target: string;
  readonly path?: string;
  readonly message: string;
}

export interface PrivacyPerson {
  readonly space: string;
  readonly target: string;
  readonly delegatesTo: readonly string[];
}

export interface PrivacyPlan {
  readonly persons: ReadonlyMap<string, PrivacyPerson>;
  readonly targets: ReadonlyMap<string, PrivacyTarget>;
  readonly findings: readonly PrivacyFinding[];
  readonly summary: Readonly<Record<PrivacyTier, number>>;
}

export interface PrivacyPlanOptions {
  readonly schemas: SchemasDefinition;
}

const BUCKET_ORDER: Record<keyof DatabaseState, number> = {
  collections: 0,
  multiCollections: 1,
  multiModels: 2,
  scopedMultiCollections: 3,
};

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

const CONTAINER_TYPES: ReadonlySet<string> = new Set([
  "object",
  "loose_object",
  "strict_object",
  "object_with_rest",
  "array",
  "tuple",
  "loose_tuple",
  "strict_tuple",
  "tuple_with_rest",
  "record",
  "map",
  "set",
  "union",
  "variant",
  "intersect",
]);

const TECHNICAL_TYPES: ReadonlySet<string> = new Set([
  "boolean",
  "number",
  "bigint",
  "date",
  "literal",
  "picklist",
  "enum",
  "null",
  "undefined",
  "void",
]);

const DATE_ACTIONS: ReadonlySet<string> = new Set([
  "iso_date",
  "iso_date_time",
  "iso_timestamp",
  "iso_time",
  "iso_time_second",
  "iso_week",
]);

const PERSONAL_ROLES: ReadonlySet<PrivacyPathRole> = new Set([
  "direct",
  "quasi",
  "sensitive",
  "contact",
  "content",
  "derived",
  "external",
]);

const DEFAULT_TREATMENTS: Record<
  PrivacyPathRole | "unknown",
  Record<PrivacyDirection, PrivacyTreatment>
> = {
  direct: { extract: "pseudonym", erase: "pseudonym", export: "include" },
  quasi: { extract: "generalise", erase: "generalise", export: "include" },
  sensitive: { extract: "drop", erase: "drop", export: "include" },
  contact: { extract: "keep", erase: "drop", export: "include" },
  content: { extract: "fake", erase: "drop", export: "include" },
  technical: { extract: "keep", erase: "keep", export: "exclude" },
  derived: { extract: "recompute", erase: "recompute", export: "exclude" },
  external: { extract: "drop", erase: "drop", export: "exclude" },
  reference: { extract: "remap", erase: "remap", export: "exclude" },
  dynamic: { extract: "keep", erase: "keep", export: "exclude" },
  none: { extract: "keep", erase: "keep", export: "exclude" },
  unknown: { extract: "drop", erase: "drop", export: "exclude" },
};

function targetKey(
  bucket: keyof DatabaseState,
  collection: string,
  type: string | undefined,
): string {
  return `${bucket}/${collection}/${type ?? ""}`;
}

function normalisePath(raw: readonly (string | number)[]): string | null {
  const parts: string[] = [];
  for (const element of raw) {
    const s = String(element);
    if (s === "$key") return null;
    if (s === "$[]" || s === "$value") {
      parts.push("*");
      continue;
    }
    if (s.startsWith("$")) continue;
    parts.push(s);
  }
  return parts.join(".");
}

interface Leaf {
  readonly path: string;
  readonly actions: unknown[];
}

interface Leaves {
  readonly leaves: Leaf[];
  readonly dynamicRoots: string[];
}

function collectLeaves(fields: SchemaContent): Leaves {
  const byPath = new Map<string, Leaf>();
  const dynamicRoots: string[] = [];
  const carried = new Map<string, unknown[]>();
  const navigator = new SchemaNavigator();

  const push = (path: string, actions: unknown[]) => {
    const inherited = carried.get(path) ?? [];
    carried.delete(path);
    const existing = byPath.get(path);
    if (existing) {
      existing.actions.push(...inherited, ...actions);
    } else {
      byPath.set(path, { path, actions: [...inherited, ...actions] });
    }
  };

  const visitor = createSimpleVisitor({
    onNode: (node) => {
      const schema = node.schema as unknown as Record<string, unknown>;
      if (schema.kind !== "schema") return false;
      const path = normalisePath(node.path);
      if (path === null) return false;
      const type = schema.type as string;
      if (WRAPPER_TYPES.has(type)) {
        const own = collectActions(schema).filter((a) => a !== schema.wrapped);
        carried.set(path, [...(carried.get(path) ?? []), ...own]);
        return true;
      }
      if (CONTAINER_TYPES.has(type)) {
        const metas = readPrivacyMetadata(schema);
        if (metas.some((m) => m.kind === "dynamic")) {
          dynamicRoots.push(path);
          push(path, collectActions(schema));
          return true;
        }
        if (metas.length > 0) {
          push(path, collectActions(schema));
          return false;
        }
        return true;
      }
      push(path, collectActions(schema));
      return false;
    },
  });

  for (const [key, schema] of Object.entries(fields)) {
    if (key === "_id") continue;
    navigator.navigate(schema as never, visitor, { path: [key], depth: 1 });
  }
  return { leaves: [...byPath.values()], dynamicRoots };
}

interface Signals {
  readonly spaces: readonly string[];
  readonly email: boolean;
  readonly unique: boolean;
  readonly dateAction: boolean;
  readonly metadata: readonly PrivacyMetadata[];
  readonly types: ReadonlySet<string>;
  readonly picklist: readonly string[] | null;
}

function readSignals(actions: readonly unknown[]): Signals {
  const spaces = new Set<string>();
  const types = new Set<string>();
  const metadata: PrivacyMetadata[] = [];
  let email = false;
  let unique = false;
  let dateAction = false;
  let picklist: string[] | null = null;
  for (const action of actions) {
    const a = action as Record<string, unknown>;
    if (a.kind === "schema") {
      if (!WRAPPER_TYPES.has(a.type as string)) types.add(a.type as string);
      const prefix = extractIdPrefix(a);
      if (prefix) spaces.add(prefix);
      if (a.type === "picklist" && Array.isArray(a.options)) {
        picklist = a.options.map(String);
      }
    }
    if (a.type === "email") email = true;
    if (typeof a.type === "string" && DATE_ACTIONS.has(a.type)) {
      dateAction = true;
    }
    if (a.type === "metadata") {
      const meta = a.metadata as Record<PropertyKey, unknown> | undefined;
      const index = meta?.[INDEX_SYMBOL] as { unique?: boolean } | undefined;
      if (index?.unique) unique = true;
      const privacy = meta?.[PRIVACY_SYMBOL] as PrivacyMetadata | undefined;
      if (privacy) metadata.push(privacy);
    }
  }
  return {
    spaces: [...spaces],
    email,
    unique,
    dateAction,
    metadata,
    types,
    picklist,
  };
}

interface Draft {
  path: string;
  tier: PrivacyTier;
  role: PrivacyPathRole;
  relation?: PrivacyRelation;
  declaredMention?: boolean;
  spaces: readonly string[];
  space?: string;
  consistent?: PrivacyConsistency;
  mirrorOf?: string;
  dynamicRoot?: string;
  uniqueOnly?: boolean;
  normalize?: PrivacyNormalize;
  overrides?: PrivacyTreatments;
  values?: readonly string[];
  note?: string;
}

function classifyLeaf(leaf: Leaf): Draft {
  const s = readSignals(leaf.actions);
  const last = <K extends PrivacyMetadata["kind"]>(kind: K) => {
    const found = s.metadata.filter((
      m,
    ): m is Extract<PrivacyMetadata, { kind: K }> => m.kind === kind);
    return found.length > 0 ? found[found.length - 1] : undefined;
  };
  if (last("dynamic")) {
    return { path: leaf.path, tier: "declared", role: "dynamic", spaces: [] };
  }
  const exempt = last("exempt");
  if (exempt) {
    return {
      path: leaf.path,
      tier: "declared",
      role: "none",
      spaces: [],
      note: exempt.reason,
    };
  }
  const field = last("field");
  if (s.spaces.length > 0) {
    return {
      path: leaf.path,
      tier: "certain",
      role: "reference",
      spaces: s.spaces,
      overrides: field?.treatment,
      declaredMention: field?.relation === "mention",
      note: s.spaces.length > 1 ? "polymorphic reference" : undefined,
    };
  }
  const mirror = last("mirror");
  if (mirror) {
    return {
      path: leaf.path,
      tier: "declared",
      role: "derived",
      spaces: [],
      mirrorOf: mirror.source,
      normalize: mirror.normalize,
    };
  }
  if (field) {
    return {
      path: leaf.path,
      tier: "declared",
      role: field.role,
      spaces: [],
      space: field.space,
      consistent: field.consistent,
      overrides: field.treatment,
    };
  }
  if (s.email || s.unique) {
    return {
      path: leaf.path,
      tier: "certain",
      role: "direct",
      spaces: [],
      uniqueOnly: !s.email,
      note: [s.email ? "v.email" : null, s.unique ? "unique index" : null]
        .filter(Boolean).join(", "),
    };
  }
  const allTechnical = s.types.size > 0 &&
    [...s.types].every((t) =>
      TECHNICAL_TYPES.has(t) || (t === "string" && s.dateAction)
    );
  if (allTechnical) {
    return {
      path: leaf.path,
      tier: "inferred",
      role: "technical",
      spaces: [],
      values: s.picklist ?? undefined,
      note: [...s.types].join("|"),
    };
  }
  return {
    path: leaf.path,
    tier: "unknown",
    role: "none",
    spaces: [],
    note: [...s.types].join("|") || undefined,
  };
}

export function defaultTreatments(
  role: PrivacyPathRole,
): Readonly<Record<PrivacyDirection, PrivacyTreatment>> {
  return DEFAULT_TREATMENTS[role];
}

function treatments(
  role: PrivacyPathRole,
  tier: PrivacyTier,
  overrides: PrivacyTreatments | undefined,
): Record<PrivacyDirection, PrivacyTreatment> {
  const base = tier === "unknown" || tier === "dynamic"
    ? DEFAULT_TREATMENTS.unknown
    : DEFAULT_TREATMENTS[role];
  return { ...base, ...overrides };
}

function finalise(draft: Draft): PrivacyPath {
  return {
    path: draft.path,
    tier: draft.tier,
    role: draft.role,
    ...(draft.relation !== undefined && { relation: draft.relation }),
    spaces: draft.spaces,
    ...(draft.space !== undefined && { space: draft.space }),
    ...(draft.consistent !== undefined && { consistent: draft.consistent }),
    ...(draft.mirrorOf !== undefined && { mirrorOf: draft.mirrorOf }),
    ...(draft.dynamicRoot !== undefined && { dynamicRoot: draft.dynamicRoot }),
    ...(draft.normalize !== undefined && { normalize: draft.normalize }),
    treatment: treatments(draft.role, draft.tier, draft.overrides),
    ...(draft.values !== undefined && { values: draft.values }),
    ...(draft.note !== undefined && draft.note !== "" && { note: draft.note }),
  };
}

function classifyLeaves(collected: Leaves): Draft[] {
  return collected.leaves.map((leaf) => {
    const draft = classifyLeaf(leaf);
    const root = collected.dynamicRoots.find((r) =>
      draft.path.startsWith(`${r}.`)
    );
    if (root === undefined) return draft;
    draft.dynamicRoot = root;
    if (draft.tier === "unknown") draft.tier = "dynamic";
    return draft;
  });
}

interface RawTarget {
  readonly key: string;
  readonly bucket: keyof DatabaseState;
  readonly collection: string;
  readonly type?: string;
  readonly space: string;
  readonly idMetadata: readonly PrivacyMetadata[];
  readonly drafts: Draft[];
}

function targetIdSpace(idSchema: unknown, autoKey: string | null): string {
  if (idSchema !== undefined) return extractIdPrefix(idSchema);
  return autoKey ?? "";
}

function enumerateTargets(schemas: SchemasDefinition): RawTarget[] {
  const out: RawTarget[] = [];
  const add = (
    bucket: keyof DatabaseState,
    collection: string,
    type: string | undefined,
    fields: SchemaContent,
    autoKey: string | null,
  ) => {
    out.push({
      key: targetKey(bucket, collection, type),
      bucket,
      collection,
      type,
      space: targetIdSpace(fields._id, autoKey),
      idMetadata: fields._id === undefined
        ? []
        : readSignals(collectActions(fields._id)).metadata,
      drafts: classifyLeaves(collectLeaves(fields)),
    });
  };
  for (const [name, fields] of Object.entries(schemas.collections ?? {})) {
    add("collections", name, undefined, fields, null);
  }
  for (const [name, types] of Object.entries(schemas.multiCollections ?? {})) {
    for (const [type, fields] of Object.entries(types)) {
      add("multiCollections", name, type, fields, type);
    }
  }
  for (const [model, types] of Object.entries(schemas.multiModels ?? {})) {
    for (const [type, fields] of Object.entries(types)) {
      add("multiModels", model, type, fields, type);
    }
  }
  for (
    const [name, scoped] of Object.entries(schemas.scopedMultiCollections ?? {})
  ) {
    for (const [type, fields] of Object.entries(scoped.types)) {
      add("scopedMultiCollections", name, type, fields, type);
    }
  }
  out.sort((a, b) => {
    const order = BUCKET_ORDER[a.bucket] - BUCKET_ORDER[b.bucket];
    return order !== 0 ? order : a.key.localeCompare(b.key);
  });
  return out;
}

export function buildPrivacyPlan(options: PrivacyPlanOptions): PrivacyPlan {
  const raw = enumerateTargets(options.schemas);
  const findings: PrivacyFinding[] = [];

  const persons = new Map<string, PrivacyPerson>();
  const personMeta = new Map<
    string,
    Extract<PrivacyMetadata, { kind: "person" }>
  >();
  for (const target of raw) {
    const person = target.idMetadata.find((m) => m.kind === "person");
    if (!person || person.kind !== "person") continue;
    if (persons.has(target.space)) {
      findings.push({
        level: "info",
        target: target.key,
        message: `person space "${target.space}" is also declared by ${
          persons.get(target.space)!.target
        }; the first in canonical order is kept`,
      });
      continue;
    }
    persons.set(target.space, {
      space: target.space,
      target: target.key,
      delegatesTo: person.of,
    });
    personMeta.set(target.space, person);
  }
  for (const person of persons.values()) {
    for (const space of person.delegatesTo) {
      if (!persons.has(space)) {
        findings.push({
          level: "error",
          target: person.target,
          message:
            `person space "${person.space}" delegates to "${space}", which is not a person space`,
        });
      }
    }
  }

  if (persons.size === 0 && raw.length > 0) {
    findings.push({
      level: "error",
      target: "*",
      message:
        "no person space declared: nothing can be classified as personal data; declare personId(...) on the _id of every collection whose documents are persons",
    });
  }

  const spaceOwners = new Map<string, string>();
  for (const target of raw) {
    if (target.space && !spaceOwners.has(target.space)) {
      spaceOwners.set(target.space, target.key);
    }
  }

  const owners = new Map<string, PrivacyOwner>();
  const byKey = new Map(raw.map((t) => [t.key, t]));

  const resolveOwner = (target: RawTarget): PrivacyOwner => {
    const person = persons.get(target.space);
    const isPerson = person !== undefined && person.target === target.key;
    const references = target.drafts.filter((d) =>
      d.role === "reference" && !d.declaredMention
    );
    const personRefs = references.filter((d) =>
      d.spaces.some((s) => persons.has(s))
    );

    if (isPerson) {
      const via: string[] = [];
      for (const space of person.delegatesTo) {
        const hits = references.filter((d) =>
          d.spaces.includes(space) &&
          d.spaces.every((x) => person.delegatesTo.includes(x))
        );
        if (hits.length === 0) {
          findings.push({
            level: "error",
            target: target.key,
            message:
              `delegation to "${space}" declared but no field references that space`,
          });
        }
        via.push(...hits.map((d) => d.path));
      }
      return {
        kind: "self",
        spaces: [target.space],
        via,
        chain: [target.space],
      };
    }

    const exempt = target.idMetadata.find((m) => m.kind === "exempt");
    if (exempt && exempt.kind === "exempt") {
      return {
        kind: "exempt",
        spaces: [],
        via: [],
        chain: [],
        reason: exempt.reason,
      };
    }

    const declared = target.idMetadata.find((m) => m.kind === "owner");
    if (declared && declared.kind === "owner") {
      const spaces: string[] = [];
      const via: string[] = [];
      for (const entry of declared.of) {
        if (spaceOwners.has(entry) || persons.has(entry)) {
          const hits = references.filter((d) => d.spaces.includes(entry));
          if (hits.length === 1) {
            spaces.push(entry);
            via.push(hits[0].path);
          } else if (hits.length === 0) {
            findings.push({
              level: "error",
              target: target.key,
              message:
                `owner "${entry}" declared but no field references that space`,
            });
          } else {
            findings.push({
              level: "error",
              target: target.key,
              message:
                `owner "${entry}" is referenced by ${hits.length} fields (${
                  hits.map((h) => h.path).join(", ")
                }); declare the path instead of the space`,
            });
          }
          continue;
        }
        const leaf = references.find((d) => d.path === entry);
        if (!leaf) {
          findings.push({
            level: "error",
            target: target.key,
            path: entry,
            message:
              `owner "${entry}" is neither a known space nor a reference field of this document`,
          });
          continue;
        }
        spaces.push(...leaf.spaces);
        via.push(leaf.path);
      }
      return { kind: "declared", spaces, via, chain: [] };
    }

    if (personRefs.length === 0) {
      return { kind: "none", spaces: [], via: [], chain: [] };
    }
    if (personRefs.length === 1) {
      const only = personRefs[0];
      return {
        kind: "inferred",
        spaces: only.spaces.filter((s) => persons.has(s)),
        via: [only.path],
        chain: [],
      };
    }
    findings.push({
      level: "warning",
      target: target.key,
      message: `${personRefs.length} fields reference person spaces (${
        personRefs.map((d) => `${d.path}: ${d.spaces.join("|")}`).join(", ")
      }); declare the owner with personal(_id, { of })`,
    });
    return { kind: "ambiguous", spaces: [], via: [], chain: [] };
  };

  for (const target of raw) owners.set(target.key, resolveOwner(target));

  const chainOf = (
    space: string,
    seen: Set<string>,
  ): readonly string[] | null => {
    if (persons.has(space)) return [space];
    if (seen.has(space)) return null;
    seen.add(space);
    const ownerKey = spaceOwners.get(space);
    if (!ownerKey) return null;
    const owner = owners.get(ownerKey);
    if (!owner || owner.spaces.length === 0) return null;
    const tails = owner.spaces.map((s) => chainOf(s, seen)).filter((
      c,
    ): c is readonly string[] => c !== null);
    if (tails.length === 0) return null;
    return [space, ...tails[0]];
  };

  const targets = new Map<string, PrivacyTarget>();
  const summary: Record<PrivacyTier, number> = {
    certain: 0,
    inferred: 0,
    declared: 0,
    unknown: 0,
    dynamic: 0,
    none: 0,
  };

  for (const target of raw) {
    const owner = owners.get(target.key)!;
    const person = persons.get(target.space);
    const isPerson = person !== undefined && person.target === target.key;
    let chain: readonly string[] = owner.chain;
    if (owner.kind === "declared" || owner.kind === "inferred") {
      const chains = owner.spaces.map((s) => chainOf(s, new Set()));
      const resolved = chains.filter((c): c is readonly string[] => c !== null);
      chain = resolved.length > 0 ? resolved[0] : [];
      owner.spaces.forEach((s, i) => {
        if (chains[i] === null) {
          findings.push({
            level: "error",
            target: target.key,
            message: `owner space "${s}" does not resolve to a person space`,
          });
        }
      });
    }

    const paths: PrivacyPath[] = [];
    for (const draft of target.drafts) {
      const d: Draft = { ...draft };
      if (d.role === "reference") {
        const personSpace = d.spaces.some((s) => persons.has(s));
        if (owner.via.includes(d.path)) {
          d.relation = isPerson ? "delegation" : "owner";
        } else if (personSpace) {
          d.relation = "mention";
        } else {
          d.relation = "relation";
        }
      } else if (owner.kind === "exempt") {
        d.tier = "none";
        d.role = "none";
      } else if (owner.kind === "none") {
        if (d.uniqueOnly) {
          d.tier = "none";
          d.role = "none";
        } else if (
          PERSONAL_ROLES.has(d.role) &&
          (d.tier === "certain" || d.tier === "declared")
        ) {
          d.tier = "unknown";
          d.note = `${d.role} signal in a document without owner`;
        } else if (
          d.tier === "unknown" || d.tier === "inferred" || d.tier === "dynamic"
        ) {
          d.tier = "none";
          d.role = "none";
        }
      }
      const done = finalise(d);
      summary[done.tier] += 1;
      paths.push(done);
    }
    paths.sort((a, b) => a.path.localeCompare(b.path));

    if (owner.kind === "none") {
      const strong = paths.filter((p) => p.tier === "unknown");
      if (strong.length > 0) {
        findings.push({
          level: "warning",
          target: target.key,
          message:
            `${strong.length} personal signal(s) in a document without owner (${
              strong.map((p) => p.path).join(", ")
            }); declare an owner or notPersonal(_id, reason)`,
        });
      }
    }

    for (const p of paths) {
      if (p.mirrorOf === undefined) continue;
      const space = p.mirrorOf.split(".")[0];
      if (!spaceOwners.has(space) && !persons.has(space)) {
        findings.push({
          level: "warning",
          target: target.key,
          path: p.path,
          message:
            `mirrorOf "${p.mirrorOf}" points at unknown space "${space}"`,
        });
      }
    }

    targets.set(target.key, {
      key: target.key,
      bucket: target.bucket,
      collection: target.collection,
      ...(target.type !== undefined && { type: target.type }),
      space: target.space,
      person: isPerson,
      delegatesTo: isPerson ? person.delegatesTo : [],
      owner: { ...owner, chain },
      paths,
    });
  }

  void byKey;
  return { persons, targets, findings, summary };
}
