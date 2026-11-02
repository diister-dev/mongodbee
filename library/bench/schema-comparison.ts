import process from "node:process";
import { migrationDefinition } from "../src/migration/definition.ts";
import {
  createSchemaComparer,
  diffSchemas,
  flattenSchema,
  simplifySchema,
  validateLastMigrationMatchesProjectSchema,
} from "../src/migration/schema-validation.ts";
import {
  referenceFlattenSchema,
  referenceSimplifySchema,
} from "../test/fixtures/schema-flatten-reference.ts";
import { largeSchemas } from "../test/fixtures/large-schema.ts";

const TYPES = (process.env.BENCH_TYPES ?? "20,40,80").split(",").map(Number);
const DEPTHS = (process.env.BENCH_DEPTHS ?? "2,4,6").split(",").map(Number);

function ms(run: () => unknown): number {
  const start = performance.now();
  run();
  return Math.round(performance.now() - start);
}

console.log(
  "types depth | keys/side | reference flatten ms | flatten ms | reference comparison ms | comparison ms | compared nodes",
);
for (const types of TYPES) {
  for (const depth of DEPTHS) {
    const project = largeSchemas({ types, depth });
    const snapshot = largeSchemas({ types, depth });
    const last = migrationDefinition("001", "snapshot", {
      parent: null,
      schemas: snapshot,
      migrate: (b) => b.compile(),
    });
    const scoped = project.scopedMultiCollections;
    let keys = 0;
    const referenceFlatten = ms(() => {
      keys = Object.keys(
        referenceFlattenSchema(referenceSimplifySchema(scoped)),
      ).length;
    });
    const flatten = ms(() => flattenSchema(simplifySchema(scoped)));
    const referenceComparison = ms(() =>
      diffSchemas(
        referenceFlattenSchema(referenceSimplifySchema(project)),
        referenceFlattenSchema(referenceSimplifySchema(snapshot)),
      ),
    );
    let nodes = 0;
    const comparison = ms(() => {
      nodes = validateLastMigrationMatchesProjectSchema(
        last,
        project,
      ).comparedNodes;
    });
    if (!createSchemaComparer().equal(project, snapshot)) {
      throw new Error("the two builds of the synthetic schema differ");
    }
    console.log(
      `${types} ${depth} | ${keys} | ${referenceFlatten} | ${flatten} | ${referenceComparison} | ${comparison} | ${nodes}`,
    );
  }
}
