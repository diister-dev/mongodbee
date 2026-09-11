import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { isMainModule } from "../../src/migration/utils/platform.ts";

// The CLI's entry files run `main()` only when `isMainModule(import.meta)`
// says they are the entry point. Deno and Bun set `import.meta.main`; when
// the entry is a `jsr:` or `npm:` specifier that flag is the only usable
// signal, because the module URL is not a file path — comparing it with
// argv[1] said "no", and `deno run jsr:@diister/mongodbee/migration/cli/bin`
// exited 0 without doing anything.

test("isMainModule: trusts the runtime's import.meta.main when it is set", () => {
  assertEquals(
    isMainModule({
      url: "https://jsr.io/@diister/mongodbee/0.23.0/src/migration/cli/bin.ts",
      main: true,
    }),
    true,
  );
  assertEquals(
    isMainModule({
      url: "https://jsr.io/@diister/mongodbee/0.23.0/src/migration/cli/bin.ts",
      main: false,
    }),
    false,
  );
  assertEquals(isMainModule({ url: import.meta.url, main: false }), false);
});

test("isMainModule: without the flag, a module that is not argv[1] is not the entry point", () => {
  // A URL that is not a file path cannot match argv[1].
  assertEquals(
    isMainModule({
      url: "https://jsr.io/@diister/mongodbee/0.23.0/src/migration/cli/bin.ts",
    }),
    false,
  );
  // A file that exists but is not the one the runtime was asked to run.
  assertEquals(
    isMainModule({
      url: new URL("../../src/migration/utils/platform.ts", import.meta.url)
        .href,
    }),
    false,
  );
});
