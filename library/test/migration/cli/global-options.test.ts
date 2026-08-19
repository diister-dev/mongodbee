// Verrou — `--config` must actually aim the command it is given to.
//
// The flag is documented as a GLOBAL option, but each command re-read it from
// its own `configPath` field and only `migrate` ever mapped the CLI's `config`
// onto it. The six others fell back to auto-discovery, silently: a `status`,
// `check`, `rollback` or `sync` pointed at another configuration ran against
// whatever `mongodbee.config.*` happened to sit in the working directory, and
// reported success against the database the operator thought they had just
// steered away from.
//
// Worth a subprocess rather than a direct command call: the mapping lives in
// `main.ts`, so a test that calls `statusCommand({ configPath })` passes on the
// broken version too.

import { assertEquals, assertStringIncludes } from "@std/assert";
import { runCli, withTempDir } from "./shared.ts";

const AUTO_DISCOVERED_DB = "mongodbee_test_autodiscovered";
const EXPLICIT_DB = "mongodbee_test_explicit";

/** A directory holding two configurations that name different databases. */
async function writeTwoConfigs(dir: string): Promise<void> {
  await Deno.mkdir(`${dir}/migrations`, { recursive: true });
  await Deno.writeTextFile(
    `${dir}/schemas.ts`,
    `export default { collections: {} };\n`,
  );
  const config = (dbName: string) =>
    `export default { database: { connection: { uri: "mongodb://localhost:27017" }, name: "${dbName}" }, ` +
    `paths: { migrations: "./migrations", schemas: "./schemas.ts" } };\n`;
  await Deno.writeTextFile(
    `${dir}/mongodbee.config.ts`,
    config(AUTO_DISCOVERED_DB),
  );
  await Deno.writeTextFile(`${dir}/elsewhere.config.ts`, config(EXPLICIT_DB));
}

Deno.test("cli: --config aims a command other than migrate", async () => {
  await withTempDir(async (tempDir) => {
    await writeTwoConfigs(tempDir);

    const { stdout } = await runCli(tempDir, [
      "status",
      "--config",
      "./elsewhere.config.ts",
    ]);

    assertStringIncludes(stdout, EXPLICIT_DB);
    // Both directions: a fix that merely prints the right name somewhere while
    // still loading the discovered file would pass the assertion above.
    assertEquals(
      stdout.includes(AUTO_DISCOVERED_DB),
      false,
      "the auto-discovered configuration must not be the one that got loaded",
    );
  });
});

Deno.test("cli: --config aims the new baseline command too", async () => {
  await withTempDir(async (tempDir) => {
    await writeTwoConfigs(tempDir);

    const { stdout } = await runCli(tempDir, [
      "baseline",
      "--config",
      "./elsewhere.config.ts",
      "--force",
    ]);

    assertStringIncludes(stdout, EXPLICIT_DB);
    assertEquals(stdout.includes(AUTO_DISCOVERED_DB), false);
  });
});
