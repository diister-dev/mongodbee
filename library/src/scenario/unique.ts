import type { DatabaseState } from "../migration/types.ts";
import type { PrivacyTarget } from "../privacy/plan.ts";
import type { UniquePartition } from "../privacy/unique-keys.ts";
import { docsOf } from "./state.ts";

export interface PartitionedDoc {
  readonly doc: Record<string, unknown>;
  readonly partition: UniquePartition;
}

export function partitionedDocs(
  state: DatabaseState,
  target: PrivacyTarget,
): PartitionedDoc[] {
  if (target.bucket === "multiModels") {
    return Object.entries(state.multiModels)
      .filter(([, instance]) => instance.modelType === target.collection)
      .flatMap(([name, instance]) =>
        instance.content
          .filter((doc) => doc._type === target.type)
          .map((doc) => ({ doc, partition: { instance: name, scope: "" } })),
      );
  }
  return docsOf(state, target).map((doc) => ({
    doc,
    partition: { instance: "", scope: String(doc._scope ?? "") },
  }));
}
