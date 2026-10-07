import { computeTruthFromDocuments, toSubjects } from "../computed-apply.ts";
import {
  type ComputedLocation,
  computedTopology,
} from "../computed-topology.ts";
import { withComputedValue } from "../migration/computed-operation.ts";
import type { DatabaseState, SchemasDefinition } from "../migration/types.ts";

type Content = { content: Record<string, unknown>[] };

function contentAt(
  state: DatabaseState,
  location: ComputedLocation,
): Content | undefined {
  switch (location.kind) {
    case "collection":
      return state.collections[location.collection];
    case "multi":
      return state.multiCollections[location.collection];
    case "scoped":
      return state.scopedMultiCollections[location.collection];
  }
}

function belongsTo(location: ComputedLocation, doc: Record<string, unknown>) {
  return location.kind === "collection" || doc._type === location.type;
}

export function recomputeComputedFields(
  state: DatabaseState,
  schemas: SchemasDefinition,
): void {
  for (const field of computedTopology(schemas).fields) {
    const subjects = contentAt(state, field.at);
    if (!subjects) continue;
    const documentsAt = (location: ComputedLocation) =>
      (contentAt(state, location)?.content ?? []).filter((doc) =>
        belongsTo(location, doc),
      );
    const truth = computeTruthFromDocuments(
      field,
      toSubjects(documentsAt(field.at)),
      documentsAt(field.source),
      field.far ? documentsAt(field.far) : [],
    );
    subjects.content = subjects.content.map((doc) =>
      belongsTo(field.at, doc)
        ? withComputedValue(doc, field.name, truth.get(String(doc._id)))
        : doc,
    );
  }
}
