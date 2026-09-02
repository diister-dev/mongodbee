/**
 * Tests for the CLI confirmation prompt
 *
 * `confirm` used to resolve `false` for EVERY answer: `rl.close()` emits
 * `close` synchronously, and the `line` handler closed the interface before
 * resolving, so the `close` handler's `resolve(false)` always won.
 */

import { assertEquals } from "@std/assert";
import { PassThrough } from "node:stream";
import { confirm } from "../../../src/migration/cli/utils/confirm.ts";

function streams() {
  const input = new PassThrough();
  const output = new PassThrough();
  output.resume(); // drain whatever readline echoes
  return { input, output };
}

Deno.test("confirm - 'yes' confirms", async () => {
  const { input, output } = streams();
  const answer = confirm("Proceed?", { input, output });
  input.write("yes\n");
  assertEquals(await answer, true);
});

Deno.test("confirm - answer is case-insensitive and trimmed", async () => {
  const { input, output } = streams();
  const answer = confirm("Proceed?", { input, output });
  input.write("  YeS \r\n");
  assertEquals(await answer, true);
});

Deno.test("confirm - anything but yes refuses", async () => {
  for (const line of ["no\n", "y\n", "\n", "yes please\n"]) {
    const { input, output } = streams();
    const answer = confirm("Proceed?", { input, output });
    input.write(line);
    assertEquals(await answer, false, JSON.stringify(line));
  }
});

Deno.test("confirm - input closing without an answer refuses", async () => {
  const { input, output } = streams();
  const answer = confirm("Proceed?", { input, output });
  input.end();
  assertEquals(await answer, false);
});

Deno.test("confirm - answer arriving before the prompt is still read", async () => {
  const { input, output } = streams();
  input.write("yes\n");
  assertEquals(await confirm("Proceed?", { input, output }), true);
});

Deno.test("confirm - two prompts in a row each get their own answer", async () => {
  const first = streams();
  const p1 = confirm("First?", first);
  first.input.write("yes\n");
  assertEquals(await p1, true);

  const second = streams();
  const p2 = confirm("Second?", second);
  second.input.write("no\n");
  assertEquals(await p2, false);
});
