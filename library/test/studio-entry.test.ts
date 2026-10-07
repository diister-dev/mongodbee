import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import {
  loadStudioCommand,
  STUDIO_PACKAGE,
  STUDIO_UNAVAILABLE_MESSAGE,
} from "../src/migration/cli/commands/studio-entry.ts";

test("the studio command loads the studio package when it is installed", async () => {
  const seen: unknown[] = [];
  const handler = await loadStudioCommand(async (specifier) => {
    assertEquals(specifier, STUDIO_PACKAGE);
    return {
      studioCommand: async (options: unknown) => {
        seen.push(options);
      },
    };
  });
  assert(handler);
  await handler({ port: 4983 });
  assertEquals(seen, [{ port: 4983 }]);
});

test("the studio command reports a missing studio package instead of failing", async () => {
  const missing = Object.assign(
    new Error(`Cannot find package '${STUDIO_PACKAGE}' imported from bin.js`),
    { code: "ERR_MODULE_NOT_FOUND" },
  );
  const handler = await loadStudioCommand(async () => {
    throw missing;
  });
  assertEquals(handler, undefined);
  assert(
    STUDIO_UNAVAILABLE_MESSAGE.includes(
      `npm install --save-dev ${STUDIO_PACKAGE}`,
    ),
  );
});

test("the studio command surfaces an error from inside the studio package", async () => {
  await assertRejects(() =>
    loadStudioCommand(async () => {
      throw new Error("Cannot find module 'svelte' imported from studio");
    }),
  );
});
