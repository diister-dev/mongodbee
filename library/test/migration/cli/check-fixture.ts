import { mkdir, writeFile } from "node:fs/promises";
import * as path from "node:path";
import process from "node:process";

export type CheckFixtureVariant =
  | "valid"
  | "failing-simulation"
  | "schema-drift";

const SCHEMA_V1 = `{
  collections: {
    users: {
      name: v.string(),
      email: v.pipe(v.string(), v.email()),
    },
  },
}`;

const SCHEMA_V2 = `{
  collections: {
    users: {
      name: v.string(),
      email: v.pipe(v.string(), v.email()),
      role: v.picklist(["admin", "viewer"]),
    },
  },
}`;

const SCHEMA_V3 = `{
  collections: {
    users: {
      name: v.string(),
      email: v.pipe(v.string(), v.email()),
      role: v.picklist(["admin", "viewer"]),
      active: v.boolean(),
    },
  },
}`;

function migration(
  id: string,
  name: string,
  parent: string | null,
  schemas: string,
  body: string,
): string {
  const parentImport = parent ? `import parent from "./${parent}.ts";\n` : "";
  return `import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";
${parentImport}
export default migrationDefinition("${id}", "${name}", {
  parent: ${parent ? "parent" : "null"},
  schemas: ${schemas},
  migrate(migration) {
    ${body}
  },
});
`;
}

export async function writeCheckFixture(
  dir: string,
  variant: CheckFixtureVariant,
): Promise<void> {
  await mkdir(path.join(dir, "migrations"), { recursive: true });
  await writeFile(
    path.join(dir, "mongodbee.config.json"),
    JSON.stringify({
      paths: { migrations: "./migrations", schemas: "./schemas.ts" },
      database: {
        name: "studio_check_fixture",
        connection: {
          uri: process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017",
        },
      },
    }),
  );

  await writeFile(
    path.join(dir, "migrations", "2025_01_01_000001_create_users.ts"),
    migration(
      "2025_01_01_000001_create_users",
      "create users",
      null,
      SCHEMA_V1,
      `return migration
      .createCollection("users")
      .seed([
        { name: "Ada", email: "ada@example.com" },
        { name: "Linus", email: "linus@example.com" },
      ])
      .end()
      .compile();`,
    ),
  );

  const secondBody =
    variant === "failing-simulation"
      ? `return migration
      .collection("users")
      .transform({
        up: (doc) => ({ ...doc, role: "owner" }),
        down: (doc) => {
          const { role: _role, ...rest } = doc;
          return rest;
        },
      })
      .end()
      .compile();`
      : `return migration
      .collection("users")
      .transform({
        up: (doc) => ({ ...doc, role: "viewer" }),
        down: (doc) => {
          const { role: _role, ...rest } = doc;
          return rest;
        },
      })
      .end()
      .compile();`;

  await writeFile(
    path.join(dir, "migrations", "2025_01_02_000002_add_role.ts"),
    migration(
      "2025_01_02_000002_add_role",
      "add role",
      "2025_01_01_000001_create_users",
      SCHEMA_V2,
      secondBody,
    ),
  );

  await writeFile(
    path.join(dir, "schemas.ts"),
    `import * as v from "valibot";

export const schemas = ${variant === "schema-drift" ? SCHEMA_V3 : SCHEMA_V2};
`,
  );
}

export function normalizeCliOutput(output: string, dir: string): string {
  return output.split(dir).join("<project>");
}
