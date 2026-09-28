import type { ComputedDescriptor } from "./computed.ts";
import {
  computedOf,
  isTypeDefinition,
  type TypeInput,
} from "./type-definition.ts";

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

export function computedTopology(schemas: ComputedSchemas): ComputedTopology {
  const locations = new Map<string, ComputedLocation[]>();
  const subjects: Array<{
    type: string;
    input: unknown;
    at: ComputedLocation;
  }> = [];
  const place = (type: string, input: unknown, at: ComputedLocation) => {
    locations.set(type, [...(locations.get(type) ?? []), at]);
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
