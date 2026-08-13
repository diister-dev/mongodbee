/**
 * Verrou — the check report may change, what the check DETECTS may not.
 *
 * `deno task db check` printed ~150 lines on a 12-migration chain, 92 of them
 * the same handful of chain-invariant `⚠ Mock identity correlation:` sentences
 * re-emitted under every migration, and the single `✗ Invalid` that mattered
 * landed at line 131 of 142. The reporter now folds warnings chain-wide and
 * defers errors to a closing verdict block.
 *
 * These lock the presentation contract AND the fact that it is only
 * presentation: same verdicts, same throw, and `MigrationValidationResult`
 * still carries the raw per-migration `warnings`/`errors` the consumer's
 * in-process CI gate reads.
 */
import { assert, assertEquals, assertRejects } from "@std/assert";
import { stripAnsiCode } from "@std/fmt/colors";
import { migrationDefinition } from "../../../src/migration/definition.ts";
import type { MigrationDefinition } from "../../../src/migration/types.ts";
import { validateMigrationsWithSimulation } from "../../../src/migration/cli/utils/validate-migrations.ts";
import { dbId, refId } from "../../../src/ids.ts";
import * as v from "../../../src/schema.ts";

// `ghost` is referenced but minted by nobody, so EVERY migration of the chain
// raises the same "no _id schema mints it" warning — the exact shape the owner
// saw twelve times over.
const CHAIN_INVARIANT = 'Identifier space "ghost" is referenced';

function collections(extra: string[]) {
  const schemas: Record<string, unknown> = {
    a: { _id: dbId("a"), ref: refId("ghost") },
  };
  for (const name of extra) schemas[name] = { _id: dbId(name) };
  return schemas as never;
}

/** A chain of `length` migrations, each carrying the same dangling reference. */
function chain(length: number): MigrationDefinition[] {
  const migrations: MigrationDefinition[] = [];
  let parent: MigrationDefinition | null = null;
  for (let i = 0; i < length; i++) {
    const extra = Array.from({ length: i }, (_, j) => `t${j}`);
    const migration: MigrationDefinition = migrationDefinition(
      `2025_01_${String(i + 1).padStart(2, "0")}_0000_${
        "A".repeat(25)
      }${i}@step_${i}`,
      `step_${i}`,
      {
        parent,
        schemas: { collections: collections(extra), multiModels: {} },
        migrate(m) {
          if (i === 0) m.createCollection("a");
          else m.createCollection(`t${i - 1}`);
          return m.compile();
        },
      },
    );
    migrations.push(migration);
    parent = migration;
  }
  return migrations;
}

/** Same chain, but the last migration seeds a document that violates its schema. */
function chainWithBrokenTail(length: number): MigrationDefinition[] {
  const migrations = chain(length - 1);
  const parent = migrations.at(-1)!;
  const i = length - 1;
  const extra = Array.from({ length: i }, (_, j) => `t${j}`);
  migrations.push(
    migrationDefinition(
      `2025_01_${String(i + 1).padStart(2, "0")}_0000_${
        "B".repeat(25)
      }${i}@broken`,
      "broken_tail",
      {
        parent,
        schemas: {
          collections: {
            ...(collections(extra) as Record<string, unknown>),
            [`t${i - 1}`]: { _id: dbId(`t${i - 1}`), n: v.number() },
          } as never,
          multiModels: {},
        },
        migrate(m) {
          m.createCollection(`t${i - 1}`);
          m.collection(`t${i - 1}`).seed(
            [{ _id: `t${i - 1}:x`, n: "not-a-number" }] as never,
          );
          return m.compile();
        },
      },
    ),
  );
  return migrations;
}

function recorder() {
  const chunks: string[] = [];
  return {
    write: (chunk: string) => void chunks.push(chunk),
    text: () => stripAnsiCode(chunks.join("")),
  };
}

const occurrences = (haystack: string, needle: string) =>
  haystack.split(needle).length - 1;

Deno.test("check report: a chain-invariant warning is printed once, not once per migration", async () => {
  const out = recorder();
  const migrations = chain(6);

  const results = await validateMigrationsWithSimulation(migrations, {
    tty: false,
    write: out.write,
    powerLevel: "quick",
  });

  const text = out.text();
  assert(
    text.includes(CHAIN_INVARIANT),
    "the finding must still be reported at all",
  );
  assertEquals(
    occurrences(text, CHAIN_INVARIANT),
    1,
    "six migrations, one printed line",
  );
  assert(
    /×\d+ · every migration \(6\)/.test(text),
    "the fold states how many migrations raised it",
  );

  // Presentation only: the returned results still carry the raw warnings the
  // consumer's CI gate reads, per migration, undeduplicated.
  assertEquals(results.length, 6);
  const raising = results.filter((r) =>
    r.warnings.some((w) => w.includes(CHAIN_INVARIANT))
  );
  assert(
    raising.length > 1,
    "the per-migration warning arrays are untouched by the digest",
  );
});

Deno.test("check report: --verbose restores the per-migration firehose", async () => {
  const quiet = recorder();
  const loud = recorder();
  const migrations = chain(4);

  await validateMigrationsWithSimulation(migrations, {
    tty: false,
    write: quiet.write,
    powerLevel: "quick",
  });
  await validateMigrationsWithSimulation(chain(4), {
    tty: false,
    write: loud.write,
    powerLevel: "quick",
    verbose: true,
  });

  assertEquals(occurrences(quiet.text(), CHAIN_INVARIANT), 1);
  assert(
    occurrences(loud.text(), CHAIN_INVARIANT) > 1,
    "--verbose prints the warning under every migration that raised it",
  );
});

Deno.test("check report: a failure still throws and is the last thing on screen", async () => {
  const out = recorder();
  const migrations = chainWithBrokenTail(6);

  await assertRejects(
    () =>
      validateMigrationsWithSimulation(migrations, {
        tty: false,
        write: out.write,
        powerLevel: "quick",
      }),
    Error,
    "Migration validation failed",
  );

  const text = out.text();
  assert(text.includes("✗ Validation FAILED"), "the verdict names the failure");
  assert(text.includes("broken_tail"), "the failing migration is named");
  assert(
    text.lastIndexOf("✗ Validation FAILED") > text.lastIndexOf("⚠ "),
    "the verdict comes AFTER the warning digest, so it cannot be buried",
  );
});

Deno.test("check report: the non-TTY sink receives no cursor escapes", async () => {
  const chunks: string[] = [];
  await validateMigrationsWithSimulation(chain(3), {
    tty: false,
    write: (chunk) => void chunks.push(chunk),
    powerLevel: "quick",
  });

  // Colours (SGR) are fine — the CI hazard is cursor movement, which turns a
  // piped log into overwritten garbage.
  const raw = chunks.join("");
  assertEquals(raw.includes("\r"), false, "no carriage returns in CI logs");
  assertEquals(raw.includes("\x1b[K"), false, "no erase-line in CI logs");
});

Deno.test("check report: a clean chain still ends on the green banner", async () => {
  const out = recorder();
  const results = await validateMigrationsWithSimulation(chain(3), {
    tty: false,
    write: out.write,
    powerLevel: "quick",
  });

  assertEquals(results.every((r) => r.valid), true);
  assert(
    out.text().trimEnd().endsWith(
      "✓ All migrations are valid and ready to apply!",
    ),
  );
});
