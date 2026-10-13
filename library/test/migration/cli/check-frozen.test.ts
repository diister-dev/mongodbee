import { test } from "../../+harness.ts";
import { assert, assertEquals } from "../../+assert.ts";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { installLibrary } from "./shared.ts";
import { runCheck } from "../../../src/migration/cli/commands/check.ts";
import { writeCheckFixture } from "./check-fixture.ts";

const FIRST_MIGRATION = "2025_01_01_000001_create_users.ts";

/**
 * The valid fixture, with its first migration reaching for a module outside
 * the migrations directory — one that still loads, so a `warn` run goes on to
 * the simulation.
 */
async function withThawedFixture(
  frozen: unknown,
  work: (dir: string) => Promise<void>,
  thaw = 'import { ROLES } from "../app/roles.ts";\nvoid ROLES;',
): Promise<void> {
  const dir = await mkdtemp(path.join(tmpdir(), "mongodbee_check_frozen_"));
  try {
    await installLibrary(dir);
    await writeCheckFixture(dir, "valid");
    const configPath = path.join(dir, "mongodbee.config.json");
    const config = JSON.parse(await readFile(configPath, "utf8"));
    if (frozen !== undefined) config.migration = { validation: { frozen } };
    await writeFile(configPath, JSON.stringify(config));
    await mkdir(path.join(dir, "app"), { recursive: true });
    await writeFile(
      path.join(dir, "app", "roles.ts"),
      'export const ROLES = ["admin", "viewer"];\n',
    );
    const migration = path.join(dir, "migrations", FIRST_MIGRATION);
    await writeFile(migration, `${thaw}\n${await readFile(migration, "utf8")}`);
    await work(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

async function check(dir: string) {
  const lines: string[] = [];
  const report = await runCheck(
    { cwd: dir, mode: "quick" },
    { log: (line) => lines.push(line), write: () => {}, tty: false },
  );
  return { report, output: lines.join("\n") };
}

test({
  name: "runCheck warns about a thawed migration by default and still simulates",
  timeout: 60_000,
  fn: async () => {
    await withThawedFixture(undefined, async (dir) => {
      const { report, output } = await check(dir);
      assertEquals(report.valid, true);
      assertEquals(report.stage, "simulation");
      assertEquals(
        report.imports.offenders.map((o) => [
          o.file,
          o.line,
          o.specifier,
          o.severity,
        ]),
        [[FIRST_MIGRATION, 1, "../app/roles.ts", "warning"]],
      );
      assert(
        output.includes(
          `⚠ ${FIRST_MIGRATION}:1 "../app/roles.ts": relative import leaves the migrations directory`,
        ),
      );
    });
  },
});

test({
  name: "runCheck stops at the imports stage when frozen is error",
  timeout: 60_000,
  fn: async () => {
    await withThawedFixture("error", async (dir) => {
      const { report, output } = await check(dir);
      assertEquals(report.valid, false);
      assertEquals(report.stage, "imports");
      assertEquals(report.failure?.message, "Frozen import check failed");
      assertEquals(report.schema, undefined);
      assertEquals(report.migrations, []);
      assertEquals(report.imports.offenders[0]?.severity, "error");
      assert(output.includes("✗ Frozen import check failed"));
    });
  },
});

test({
  name: "runCheck skips the imports stage when frozen is false",
  timeout: 60_000,
  fn: async () => {
    await withThawedFixture(false, async (dir) => {
      const { report, output } = await check(dir);
      assertEquals(report.valid, true);
      assertEquals(report.imports.offenders, []);
      assert(!output.includes("Checking that migrations are frozen"));
    });
  },
});

test({
  name: "runCheck only warns about a type-only import, even when frozen is error",
  timeout: 60_000,
  fn: async () => {
    await withThawedFixture(
      "error",
      async (dir) => {
        const { report } = await check(dir);
        assertEquals(report.valid, true);
        assertEquals(report.stage, "simulation");
        assertEquals(
          report.imports.offenders.map((o) => [o.specifier, o.severity]),
          [["#app/types.ts", "warning"]],
        );
      },
      'import type { Role } from "#app/types.ts";',
    );
  },
});
