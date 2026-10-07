/**
 * A finished command ends its process even when a handle leaked.
 *
 * @module
 */

import { test } from "../../+harness.ts";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { assert, assertEquals, assertStringIncludes } from "../../+assert.ts";
import { runScript } from "./shared.ts";

const GUARD = fileURLToPath(
  new URL("../../../src/migration/cli/utils/exit-guard.ts", import.meta.url),
);

async function runLeaking(args: string[]) {
  const dir = await mkdtemp(path.join(tmpdir(), "mongodbee_exit_guard_"));
  try {
    const entry = path.join(dir, "leak.ts");
    // A handle nobody closes: without the guard this process never ends.
    await writeFile(
      entry,
      `import { armExitGuard } from ${JSON.stringify(GUARD)};\n` +
        `const leak = setInterval(() => {}, 1000);\n` +
        `process.exitCode = 3;\n` +
        `armExitGuard(process.argv.slice(2), 300);\n` +
        `if (process.argv.includes("studio")) setTimeout(() => { clearInterval(leak); console.log("still running"); }, 1500);\n`,
    );
    const started = Date.now();
    const result = await runScript(entry, args, dir);
    return { ...result, ms: Date.now() - started };
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("exit guard - a leaked handle no longer keeps the process alive", async () => {
  const run = await runLeaking(["migrate"]);
  assertEquals(run.code, 3, "the command's exit code is kept");
  assertStringIncludes(run.stderr, '"migrate" finished but a handle');
  assert(run.ms < 5000, `took ${run.ms}ms`);
});

test("exit guard - studio is left running", async () => {
  const run = await runLeaking(["--port", "4000", "studio"]);
  assertStringIncludes(run.stdout, "still running");
  assertEquals(run.stderr.includes("finished but a handle"), false);
});
