import type { ComputedDescriptor } from "./computed.ts";
import * as v from "./schema.ts";
import { extractIndexes } from "./indexes.ts";
import {
  computedOf,
  fieldsOf,
  indexesOf,
  isTypeDefinition,
  type TypeInput,
} from "./type-definition.ts";

interface LeadingIndex {
  readonly path: string;
  readonly global: boolean;
}

function leadingIndexes(input: unknown): LeadingIndex[] {
  if (input === null || typeof input !== "object") return [];
  const entries = fieldsOf(input as TypeInput) as v.ObjectEntries;
  const onFields = extractIndexes(v.object(entries)).map(
    ({ path, metadata }) => ({
      path,
      global: metadata.global === true,
    }),
  );
  const composites = indexesOf(input as TypeInput).flatMap((descriptor) => {
    const first = Object.keys(descriptor.key)[0];
    return first === undefined
      ? []
      : [{ path: first, global: descriptor.global === true }];
  });
  return [...onFields, ...composites];
}

export type ComputedLocation =
  | { readonly kind: "collection"; readonly collection: string }
  | {
      readonly kind: "multi";
      readonly collection: string;
      readonly type: string;
    }
  | {
      readonly kind: "scoped";
      readonly collection: string;
      readonly type: string;
    };

export interface ComputedField {
  readonly subject: string;
  readonly name: string;
  readonly descriptor: ComputedDescriptor;
  readonly at: ComputedLocation;
  readonly source: ComputedLocation;
  readonly far?: ComputedLocation;
  readonly scoped: boolean;
  readonly farScoped: boolean;
}

export interface ComputedSchemas {
  readonly collections?: Readonly<Record<string, unknown>>;
  readonly multiCollections?: Readonly<
    Record<string, Readonly<Record<string, unknown>>>
  >;
  readonly multiModels?: Readonly<
    Record<string, Readonly<Record<string, unknown>>>
  >;
  readonly scopedMultiCollections?: Readonly<
    Record<
      string,
      {
        readonly scope?: unknown;
        readonly types: Readonly<Record<string, unknown>>;
      }
    >
  >;
}

export class ComputedTopologyError extends Error {
  override readonly name = "ComputedTopologyError";
}

export class ComputedTopology {
  readonly fields: readonly ComputedField[];

  constructor(fields: readonly ComputedField[]) {
    this.fields = Object.freeze([...fields]);
  }

  field(subject: string, name: string): ComputedField {
    const found = this.fields.find(
      (field) => field.subject === subject && field.name === name,
    );
    if (!found)
      throw new ComputedTopologyError(
        `no computed field "${name}" on "${subject}"`,
      );
    return found;
  }

  fieldsOf(subject: string): readonly ComputedField[] {
    return this.fields.filter((field) => field.subject === subject);
  }
}

export function locationFilter(
  location: ComputedLocation,
): Record<string, string> {
  return location.kind === "collection" ? {} : { _type: location.type };
}

function sameCollection(a: ComputedLocation, b: ComputedLocation): boolean {
  return a.collection === b.collection;
}

function describe(location: ComputedLocation): string {
  return location.kind === "collection"
    ? `collection "${location.collection}"`
    : `"${location.type}" in "${location.collection}"`;
}

function assertReadIndexed(
  label: string,
  location: ComputedLocation,
  input: unknown,
  path: string,
  readsWithinScope: boolean,
): void {
  if (path === "_id") return;
  const candidates = leadingIndexes(input).filter(
    (index) => index.path === path,
  );
  const acrossScopes = location.kind === "scoped" && !readsWithinScope;
  const usable = acrossScopes
    ? candidates.some((index) => index.global)
    : candidates.length > 0;
  if (usable) return;
  throw new ComputedTopologyError(
    `computed field ${label}: recomputing it reads ${describe(location)} by "${path}"${acrossScopes ? " across every scope" : ""}, ` +
      `and no declared index leads with "${path}"${acrossScopes ? " without the scope prefix" : ""}. ` +
      `Declare withIndex(${acrossScopes ? "..., { global: true }" : "..."}) on "${path}" or a composite index leading with it, or every such write scans the whole collection.`,
  );
}

export function computedTopology(schemas: ComputedSchemas): ComputedTopology {
  const locations = new Map<string, ComputedLocation[]>();
  const inputs = new Map<string, unknown>();
  const subjects: Array<{
    type: string;
    input: unknown;
    at: ComputedLocation;
  }> = [];
  const place = (type: string, input: unknown, at: ComputedLocation) => {
    locations.set(type, [...(locations.get(type) ?? []), at]);
    inputs.set(type, input);
    subjects.push({ type, input, at });
  };

  for (const [collection, input] of Object.entries(schemas.collections ?? {})) {
    place(collection, input, { kind: "collection", collection });
  }
  for (const [collection, types] of Object.entries(
    schemas.multiCollections ?? {},
  )) {
    for (const [type, input] of Object.entries(types))
      place(type, input, { kind: "multi", collection, type });
  }
  for (const [collection, scoped] of Object.entries(
    schemas.scopedMultiCollections ?? {},
  )) {
    for (const [type, input] of Object.entries(scoped.types))
      place(type, input, { kind: "scoped", collection, type });
  }
  for (const [model, types] of Object.entries(schemas.multiModels ?? {})) {
    for (const [type, input] of Object.entries(types)) {
      if (
        isTypeDefinition(input) &&
        Object.keys(computedOf(input)).length > 0
      ) {
        throw new ComputedTopologyError(
          `computed fields on "${type}" of multi-model "${model}" are not supported: a model template has no single physical collection`,
        );
      }
    }
  }

  const resolve = (
    type: string,
    role: string,
    subject: string,
    name: string,
  ): ComputedLocation => {
    const found = locations.get(type) ?? [];
    if (found.length === 0)
      throw new ComputedTopologyError(
        `computed field "${subject}.${name}": ${role} type "${type}" is not declared in any collection`,
      );
    if (found.length > 1) {
      throw new ComputedTopologyError(
        `computed field "${subject}.${name}": ${role} type "${type}" is declared in ${found.map(describe).join(" and ")}; a source type must live in one place`,
      );
    }
    return found[0]!;
  };

  const fields: ComputedField[] = [];
  for (const { type: subject, input, at } of subjects) {
    if (!isTypeDefinition(input)) continue;
    for (const [name, descriptor] of Object.entries(
      computedOf(input as TypeInput),
    )) {
      if ((locations.get(subject) ?? []).length > 1) {
        throw new ComputedTopologyError(
          `computed field "${subject}.${name}": subject type "${subject}" is declared in more than one collection`,
        );
      }
      const source = resolve(descriptor.source.type, "source", subject, name);
      const far = descriptor.through
        ? resolve(descriptor.through.source.type, "far", subject, name)
        : undefined;
      if (
        descriptor.sameScope &&
        (at.kind !== "scoped" || source.kind !== "scoped")
      ) {
        throw new ComputedTopologyError(
          `computed field "${subject}.${name}": sameScope() needs a scoped subject and a scoped source`,
        );
      }
      if (
        at.kind === "scoped" &&
        source.kind === "scoped" &&
        !sameCollection(at, source) &&
        !descriptor.sameScope
      ) {
        throw new ComputedTopologyError(
          `computed field "${subject}.${name}": the source lives in another scoped collection; declare sameScope()`,
        );
      }
      const aggregate = descriptor.aggregate;
      if (
        aggregate.kind === "collect" &&
        aggregate.maxEntries === undefined &&
        (at.kind === "collection" || !sameCollection(at, source))
      ) {
        throw new ComputedTopologyError(
          `computed field "${subject}.${name}": a collect on a global subject or over another collection needs maxEntries()`,
        );
      }
      const scoped = at.kind === "scoped" && source.kind === "scoped";
      const farScoped =
        far !== undefined &&
        at.kind === "scoped" &&
        far.kind === "scoped" &&
        (sameCollection(at, far) || descriptor.sameScope);
      if (far && at.kind === "scoped" && far.kind === "scoped" && !farScoped) {
        throw new ComputedTopologyError(
          `computed field "${subject}.${name}": the far type lives in another scoped collection; declare sameScope()`,
        );
      }
      const label = `"${subject}.${name}"`;
      const sourceInput = inputs.get(descriptor.source.type);
      assertReadIndexed(label, source, sourceInput, descriptor.by, scoped);
      if (descriptor.through) {
        assertReadIndexed(
          label,
          source,
          sourceInput,
          descriptor.through.via,
          scoped,
        );
      }
      fields.push(
        Object.freeze({
          subject,
          name,
          descriptor,
          at,
          source,
          ...(far && { far }),
          scoped,
          farScoped,
        }),
      );
    }
  }
  return new ComputedTopology(fields);
}
