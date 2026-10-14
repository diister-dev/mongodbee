import { test } from "../+harness.ts";
import { assertEquals, assertThrows } from "../+assert.ts";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import {
  DEFAULT_FROZEN_ALLOW,
  extractImports,
  isAllowedSpecifier,
  scanFrozenImports,
} from "../../src/migration/frozen-imports.ts";
import { frozenImportsPolicy } from "../../src/migration/config/loader.ts";

const specifiers = (source: string) =>
  extractImports(source).map((i) => i.specifier);

test("extractImports reads every static, re-export, side-effect and dynamic form", () => {
  const found = extractImports(`import a from "a";
import * as b from 'b';
import c, { d, e as f } from "c";
import {
  g,
  h,
} from "multi";
import "side";
export { x } from "re";
export * from "star";
export * as ns from "star-ns";
const lazy = await import("lazy");
const opts = import("with-options", { with: { type: "json" } });
`);
  assertEquals(
    found.map((i) => [i.specifier, i.kind, i.line]),
    [
      ["a", "import", 1],
      ["b", "import", 2],
      ["c", "import", 3],
      ["multi", "import", 7],
      ["side", "side-effect", 8],
      ["re", "re-export", 9],
      ["star", "re-export", 10],
      ["star-ns", "re-export", 11],
      ["lazy", "dynamic", 12],
      ["with-options", "dynamic", 13],
    ],
  );
  assertEquals(
    found.every((i) => i.literal && !i.typeOnly),
    true,
  );
});

test("extractImports ignores specifiers inside comments, strings, templates and regexes", () => {
  assertEquals(
    specifiers(`// import a from "commented";
/* import b from "block";
   export * from "block2"; */
const s = 'import c from "in-string"';
const d = "export * from 'in-double'";
const t = \`import e from "in-template" \${"x"} import("in-template-2")\`;
const r = /import f from "in-regex"/;
const q = obj.import("member");
const m = import.meta.url;
export const fromValue = "from";
import real from "real";
`),
    ["real"],
  );
});

test("extractImports still lexes code inside a template substitution", () => {
  assertEquals(
    specifiers(
      `const t = \`a\${await import("nested")}b\`;\nimport z from "z";`,
    ),
    ["nested", "z"],
  );
});

test("extractImports reports a computed dynamic import as non-literal", () => {
  const [found] = extractImports(
    `const name = "x";\nawait import(\`./\${name}.ts\`);`,
  );
  assertEquals(found.literal, false);
  assertEquals(found.specifier, `\`./\${name}.ts\``);
  assertEquals(found.line, 2);
  const [plain] = extractImports("await import(path)");
  assertEquals([plain.literal, plain.specifier], [false, "path"]);
  const [template] = extractImports("await import(`static`)");
  assertEquals([template.literal, template.specifier], [true, "static"]);
});

test("extractImports flags type-only imports and re-exports", () => {
  const found = extractImports(`import type { A } from "types-a";
import type B from "types-b";
export type { C } from "types-c";
import type from "default-named-type";
import { type D } from "inline";
`);
  assertEquals(
    found.map((i) => [i.specifier, i.typeOnly]),
    [
      ["types-a", true],
      ["types-b", true],
      ["types-c", true],
      ["default-named-type", false],
      ["inline", false],
    ],
  );
});

test("extractImports does not run a statement into the next one without semicolons", () => {
  assertEquals(
    extractImports(`export { a }
import b from "b"
export const c = "c"
`).map((i) => [i.specifier, i.kind]),
    [["b", "import"]],
  );
});

test("isAllowedSpecifier accepts the defaults in their package forms", () => {
  for (const allowed of [
    "node:crypto",
    "node:fs/promises",
    "@diister/mongodbee",
    "@diister/mongodbee/migration",
    "jsr:@diister/mongodbee@^0.23.0/migration",
    "npm:@diister/mongodbee@0.23.0",
    "valibot",
    "npm:valibot@1.5.0",
    "jsr:@valibot/valibot",
  ]) {
    assertEquals(isAllowedSpecifier(allowed, DEFAULT_FROZEN_ALLOW), true);
  }
  for (const flagged of [
    "@diister/mongodbee-studio",
    "valibot-extra",
    "#app/schemas.ts",
    "$lib/thing",
    "@/models",
    "@std/assert",
    "lodash",
    "file:///abs/app.ts",
  ]) {
    assertEquals(isAllowedSpecifier(flagged, DEFAULT_FROZEN_ALLOW), false);
  }
  assertEquals(
    isAllowedSpecifier("@std/assert", [...DEFAULT_FROZEN_ALLOW, "@std/"]),
    true,
  );
});

async function withMigrationsDir(
  files: Record<string, string>,
  work: (dir: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(path.join(tmpdir(), "mongodbee_frozen_"));
  try {
    for (const [name, content] of Object.entries(files)) {
      await mkdir(path.dirname(path.join(root, name)), { recursive: true });
      await writeFile(path.join(root, name), content);
    }
    await work(path.join(root, "migrations"));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

const allow = [...DEFAULT_FROZEN_ALLOW];

test("scanFrozenImports follows relative imports transitively inside the directory", async () => {
  await withMigrationsDir(
    {
      "migrations/001.ts": `import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";
import { normalise } from "./helpers/normalise.ts";`,
      "migrations/002.ts": `import parent from "./001.ts";`,
      "migrations/helpers/normalise.ts": `import { deep } from "./deep";`,
      "migrations/helpers/deep.ts": `import { schemas } from "#app/schemas.ts";`,
    },
    async (dir) => {
      const offenders = await scanFrozenImports(dir, ["001.ts", "002.ts"], {
        level: "error",
        allow,
      });
      assertEquals(
        offenders.map((o) => [o.file, o.line, o.specifier, o.severity]),
        [["helpers/deep.ts", 1, "#app/schemas.ts", "error"]],
      );
    },
  );
});

test("scanFrozenImports flags a relative import that leaves the directory, without following it", async () => {
  await withMigrationsDir(
    {
      "migrations/001.ts": `import { roles } from "../src/roles.ts";`,
      "src/roles.ts": `import { db } from "$lib/db";`,
    },
    async (dir) => {
      const offenders = await scanFrozenImports(dir, ["001.ts"], {
        level: "error",
        allow,
      });
      assertEquals(
        offenders.map((o) => [o.specifier, o.reason]),
        [
          [
            "../src/roles.ts",
            "relative import leaves the migrations directory",
          ],
        ],
      );
    },
  );
});

test("scanFrozenImports reports aliases, computed imports and type-only imports with their severity", async () => {
  await withMigrationsDir(
    {
      "migrations/001.ts": `import { a } from "@/models";
import type { B } from "#app/types.ts";
const mod = await import(\`./\${"x"}.ts\`);
import { missing } from "./missing.ts";
import { assert } from "@std/assert";`,
    },
    async (dir) => {
      const asError = await scanFrozenImports(dir, ["001.ts"], {
        level: "error",
        allow: [...allow, "@std/"],
      });
      assertEquals(
        asError.map((o) => [o.line, o.specifier, o.severity]),
        [
          [1, "@/models", "error"],
          [2, "#app/types.ts", "warning"],
          [3, `\`./\${"x"}.ts\``, "error"],
          [4, "./missing.ts", "error"],
        ],
      );
      const asWarn = await scanFrozenImports(dir, ["001.ts"], {
        level: "warn",
        allow,
      });
      assertEquals(
        asWarn.map((o) => [o.specifier, o.severity]),
        [
          ["@/models", "warning"],
          ["#app/types.ts", "warning"],
          [`\`./\${"x"}.ts\``, "warning"],
          ["./missing.ts", "warning"],
          ["@std/assert", "warning"],
        ],
      );
    },
  );
});

test("frozenImportsPolicy defaults to warn and extends the allowlist", () => {
  assertEquals(frozenImportsPolicy({}), { level: "warn", allow: allow });
  assertEquals(
    frozenImportsPolicy({ migration: { validation: { frozen: false } } }).level,
    "off",
  );
  assertEquals(
    frozenImportsPolicy({ migration: { validation: { frozen: "error" } } })
      .level,
    "error",
  );
  assertEquals(
    frozenImportsPolicy({
      migration: {
        validation: { frozen: { level: "error", allow: ["@std/"] } },
      },
    }),
    { level: "error", allow: [...allow, "@std/"] },
  );
  assertThrows(
    () =>
      frozenImportsPolicy({
        migration: { validation: { frozen: "loud" as never } },
      }),
    Error,
    "migration.validation.frozen",
  );
});
