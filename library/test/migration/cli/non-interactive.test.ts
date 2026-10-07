/**
 * The CLI without a terminal: agents, CI, a pipe nobody closes.
 *
 * A confirmation used to read stdin until it closed. Spawned with a stdin
 * pipe that stays open (what most agent and CI runners do), the process
 * waited forever, so every caller had to append `< /dev/null`. Two things
 * are locked here, on the real binary with exactly that stdin:
 *
 * - a confirmation fails at once, and says how to proceed (`--force`);
 * - every database command terminates on its own, connection closed.
 *
 * @module
 */

import { test } from "../../+harness.ts";
import process from "node:process";
import { spawn } from "node:child_process";
import type { Readable } from "node:stream";
import { Buffer } from "node:buffer";
import { writeFile } from "node:fs/promises";
import { assert, assertEquals, assertStringIncludes } from "../../+assert.ts";
import { MongoClient } from "../../../src/mongodb.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import {
  CLI_ENTRY,
  getMigrationPath,
  getMigrationsDir,
  listMigrationFiles,
  readFile,
  withTempDir,
} from "./shared.ts";

const TEST_MONGODB_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";

/** Well under what a hang would take, well over a cold start. */
const DEADLINE_MS = 30_000;

interface Run {
  code: number | null;
  stdout: string;
  stderr: string;
  ms: number;
  killed: boolean;
}

/**
 * Runs the CLI with a stdin PIPE that is never written to nor closed, and
 * kills it at the deadline. `killed` says whether it had to.
 */
async function runWithOpenStdin(cwd: string, args: string[]): Promise<Run> {
  const { FORCE_COLOR: _forceColor, ...env } = process.env;
  const started = Date.now();
  const child = spawn(process.execPath, [CLI_ENTRY, ...args], {
    cwd,
    env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  let killed = false;
  const timer = setTimeout(() => {
    killed = true;
    child.kill("SIGKILL");
  }, DEADLINE_MS);

  const read = async (stream: Readable): Promise<string> => {
    const chunks: Buffer[] = [];
    for await (const chunk of stream) chunks.push(chunk as Buffer);
    return Buffer.concat(chunks).toString("utf8");
  };
  const [stdout, stderr, code] = await Promise.all([
    read(child.stdout),
    read(child.stderr),
    new Promise<number | null>((resolve) =>
      child.on("close", (c) => resolve(c)),
    ),
  ]);
  clearTimeout(timer);
  child.stdin.destroy();
  return { code, stdout, stderr, ms: Date.now() - started, killed };
}

async function setup(tempDir: string, dbName: string): Promise<void> {
  await initCommand({ cwd: tempDir });
  await writeFile(
    `${tempDir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await generateCommand({ name: "create_users", cwd: tempDir });

  // A migration whose transform is marked lossy: its rollback asks for a
  // confirmation.
  const files = await listMigrationFiles(getMigrationsDir(tempDir));
  const migrationPath = getMigrationPath(tempDir, files[files.length - 1]);
  let content = await readFile(migrationPath);
  assert(content !== null);
  content = `import * as v from "valibot";\n${content}`
    .replace(
      `collections: {`,
      `collections: {\n    users: { name: v.string() },\n`,
    )
    .replace(
      "migrate(migration) {",
      `migrate(migration) {\n    migration.createCollection("users");\n` +
        `    migration.collection("users").transform({ up: (d) => d, down: (d) => d, lossy: true });`,
    );
  await writeFile(migrationPath, content);
  let schemas = await readFile(`${tempDir}/schemas.ts`);
  assert(schemas !== null);
  schemas = `import * as v from "valibot";\n${schemas}`.replace(
    "collections: {",
    `collections: {\n      users: { name: v.string() },\n`,
  );
  await writeFile(`${tempDir}/schemas.ts`, schemas);
}

function assertTerminated(run: Run, label: string): void {
  assertEquals(
    run.killed,
    false,
    `${label} did not terminate within ${DEADLINE_MS}ms (stdin left open)\n${run.stdout}\n${run.stderr}`,
  );
}

test({
  name: "cli without a terminal: confirmations fail fast and every command terminates",
  timeout: 240_000,
  fn: async () => {
    await withTempDir(async (tempDir) => {
      const dbName = `mongodbee_test_noninteractive_${crypto
        .randomUUID()
        .replace(/-/g, "")
        .substring(0, 8)}`;
      const client = new MongoClient(TEST_MONGODB_URI);
      await client.connect();
      try {
        await setup(tempDir, dbName);

        // baseline asks before writing the ledger.
        const baseline = await runWithOpenStdin(tempDir, ["baseline"]);
        assertTerminated(baseline, "baseline");
        assertEquals(baseline.code, 1, baseline.stdout + baseline.stderr);
        assertStringIncludes(baseline.stderr, "not a terminal");
        assertStringIncludes(baseline.stderr, "--force");

        const migrate = await runWithOpenStdin(tempDir, [
          "migrate",
          "--mode",
          "quick",
        ]);
        assertTerminated(migrate, "migrate");
        assertEquals(migrate.code, 0, migrate.stdout + migrate.stderr);

        // Nothing pending: no simulation at all.
        const again = await runWithOpenStdin(tempDir, ["migrate"]);
        assertTerminated(again, "migrate (nothing pending)");
        assertEquals(again.code, 0, again.stdout + again.stderr);
        assertStringIncludes(again.stdout, "No pending migrations");
        assertEquals(
          again.stdout.includes("Validating migrations with simulation"),
          false,
          "nothing pending must not simulate anything",
        );

        for (const args of [["status"], ["check", "--mode", "quick"]]) {
          const run = await runWithOpenStdin(tempDir, args);
          assertTerminated(run, args.join(" "));
          assertEquals(run.code, 0, run.stdout + run.stderr);
        }

        // The rollback of a lossy migration asks first.
        const rollback = await runWithOpenStdin(tempDir, ["rollback"]);
        assertTerminated(rollback, "rollback");
        assertEquals(rollback.code, 1, rollback.stdout + rollback.stderr);
        assertStringIncludes(rollback.stderr, "--force");

        const forced = await runWithOpenStdin(tempDir, ["rollback", "--force"]);
        assertTerminated(forced, "rollback --force");
        assertEquals(forced.code, 0, forced.stdout + forced.stderr);
      } finally {
        await client.db(dbName).dropDatabase();
        await client.close();
      }
    });
  },
});
