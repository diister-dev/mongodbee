// Verrou — a failing subcommand must fail the PROCESS, not just print.
//
// Regression it guards: `main()` wrapped `cmd.handler(...)` in a try/catch that
// printed the error and returned normally. `main()` therefore resolved, the
// outer handler's `process.exit(1)` never ran, and EVERY subcommand exited 0 —
// `check` printed "Migration chain validation failed" and reported success,
// `migrate` the same. No pipeline step could gate on either: a CI job running
// `mongodbee check` was green on a chain it had just declared broken.
//
// Both directions are locked here. A fix that exits non-zero unconditionally
// would be just as useless as the bug.

import { test } from "../../+harness.ts";
import { mkdir, writeFile } from "node:fs/promises";
import { assertEquals, assertStringIncludes } from "../../+assert.ts";
import { runCli, withTempDir } from "./shared.ts";

/** A chain whose declared schema contradicts `schemas.ts` — check must reject it. */
async function writeDivergentChain(dir: string): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `export default { migrationsDir: "./migrations", schemasPath: "./schemas.ts" };\n`,
  );
  await writeFile(
    `${dir}/schemas.ts`,
    `import * as v from "@diister/mongodbee/schema";\n` +
      `import { dbId } from "@diister/mongodbee/ids";\n` +
      `export default { collections: { "+t": { _id: dbId("t"), label: v.string() } } };\n`,
  );
  await writeFile(
    `${dir}/migrations/2025_01_01_000000_AAAAAAAAAA.ts`,
    `import { migrationDefinition } from "@diister/mongodbee/migration";\n` +
      `import * as v from "@diister/mongodbee/schema";\n` +
      `import { dbId } from "@diister/mongodbee/ids";\n` +
      // `label` is a number here and a string in schemas.ts — the chain cannot
      // be consistent, so `check` must fail.
      `export default migrationDefinition("2025_01_01_000000_AAAAAAAAAA", "init", {\n` +
      `  schemas: { collections: { "+t": { _id: dbId("t"), label: v.number() } } },\n` +
      `  migrate(m) { m.createCollection("+t"); },\n` +
      `});\n`,
  );
}

test("cli: a failing `check` exits non-zero", async () => {
  await withTempDir(async (tempDir) => {
    await writeDivergentChain(tempDir);
    const { code, stderr } = await runCli(tempDir, ["check"]);
    assertStringIncludes(
      stderr,
      "Error:",
      "the failure must still be reported to the operator",
    );
    assertEquals(
      code,
      1,
      "a broken chain reported success — no CI step could gate on `check`",
    );
  });
});

test("cli: a succeeding command still exits zero", async () => {
  await withTempDir(async (tempDir) => {
    const { code } = await runCli(tempDir, ["help"]);
    assertEquals(code, 0, "an exit code that is always non-zero gates nothing");
  });
});
