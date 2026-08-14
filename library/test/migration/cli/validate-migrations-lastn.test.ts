/**
 * Verrou — `--last N` must be a WINDOW, not a relabelled full check, and
 * narrowing the window must not change what the window itself detects.
 *
 * Two regressions live here.
 *
 * 1. Cost. The migrations outside the window used to be "fast-forwarded",
 *    which ran the FULL simulation for each of them purely to hand a
 *    propagated state to the first migration in the window. On the reference
 *    12-migration chain that fast-forward cost 171.7s (131.4s of it in
 *    `prepareStateForNextMigration`), so `check --last 1` took LONGER than the
 *    full `check` it was meant to shortcut. The window is now seeded from the
 *    parent's declared schemas instead — `SimulationValidator`'s standalone
 *    path, which replays the ancestors' operations on an empty database for
 *    their real seeds and then mock-populates.
 *
 * 2. Honesty. That fast-forward computed a verdict for every skipped migration
 *    and pushed `valid: true` regardless, so a broken migration in the skipped
 *    range rode a green "✓ All migrations are valid" banner. Nothing validates
 *    those migrations any more, so nothing may report on them either: they are
 *    ABSENT from the results, and the banner says how many were left out.
 *    `migrate` keeps the safety property that mattered there by widening its
 *    own window to cover every migration it is about to apply — see
 *    migrate-lastn-covers-pending.test.ts.
 */
import { assert, assertEquals, assertRejects } from "@std/assert";
import { migrationDefinition } from "../../../src/migration/definition.ts";
import {
  MigrationValidationFailedError,
  type MigrationValidationResult,
  validateMigrationsWithSimulation,
} from "../../../src/migration/cli/utils/validate-migrations.ts";
import { createSimulationValidator } from "../../../src/migration/validators/simulation.ts";
import {
  createEmptyDatabaseState,
  type MigrationDefinition,
  type SimulationDatabaseState,
} from "../../../src/migration/types.ts";
import * as v from "../../../src/schema.ts";

/** The validator options `validateMigrationsWithSimulation` builds. */
const VALIDATOR_OPTIONS = {
  maxOperations: 1000,
  stateRetentionRatio: 0.5,
  powerLevel: "normal" as const,
};

/**
 * The chain-replay seeding, spelled out: this is exactly what the
 * fast-forward did before the window was seeded standalone. It is the
 * REFERENCE the windowed run has to reproduce.
 */
async function validateByChainReplay(
  migrations: MigrationDefinition[],
  windowSize: number,
): Promise<MigrationValidationResult[]> {
  const validator = createSimulationValidator(VALIDATOR_OPTIONS);
  let state: SimulationDatabaseState = createEmptyDatabaseState();

  const advance = async (migration: MigrationDefinition) => {
    const result = await validator.validateMigration(migration, state);
    if (result.success && result.data?.stateAfterMigration) {
      state = validator.prepareStateForNextMigration(
        result.data.stateAfterMigration as SimulationDatabaseState,
        migration.schemas,
      );
    }
    return result;
  };

  for (const migration of migrations.slice(0, -windowSize)) {
    await advance(migration);
  }

  const results: MigrationValidationResult[] = [];
  for (const migration of migrations.slice(-windowSize)) {
    const result = await advance(migration);
    results.push({
      migration,
      valid: result.success,
      errors: result.errors,
      warnings: result.warnings,
    });
  }
  return results;
}

/**
 * Chain: two clean migrations, then two that genuinely fail.
 *
 * Both failures are worded independently of the DATA that triggers them — a
 * forgotten `createCollection`, and a required field no document carries.
 * Seeding the window differently necessarily draws different mock values, and
 * an error like `Expected number but received "XaMm"` quotes one; comparing
 * those would measure the random stream, not the detection.
 */
function failingTailChain(): MigrationDefinition[] {
  const root = migrationDefinition(
    "2025_01_01_0000_AAAAAAAAAAAAAAAAAAAAAAAAAA@root",
    "root",
    {
      parent: null,
      schemas: {
        collections: { users: { _id: v.string(), name: v.string() } },
        multiModels: {},
      },
      migrate(m) {
        m.createCollection("users");
        m.collection("users").seed(
          [{ _id: "u1", name: "Seeded" }] as never,
        );
        return m.compile();
      },
    },
  );

  const slugged = migrationDefinition(
    "2025_01_02_0000_BBBBBBBBBBBBBBBBBBBBBBBBBB@slugged",
    "slugged",
    {
      parent: root,
      schemas: {
        collections: {
          users: { _id: v.string(), name: v.string(), slug: v.string() },
        },
        multiModels: {},
      },
      migrate(m) {
        m.collection("users").transform({
          up: (doc: Record<string, unknown>) => ({
            ...doc,
            slug: String(doc.name).toLowerCase(),
          }),
          down: (doc: Record<string, unknown>) => {
            const { slug: _slug, ...rest } = doc;
            return rest;
          },
        } as never);
        return m.compile();
      },
    },
  );

  // Fails structurally: `tags` is declared but never created in migrate().
  const forgotCreate = migrationDefinition(
    "2025_01_03_0000_CCCCCCCCCCCCCCCCCCCCCCCCCC@forgot_create",
    "forgot_create",
    {
      parent: slugged,
      schemas: {
        collections: {
          users: { _id: v.string(), name: v.string(), slug: v.string() },
          tags: { _id: v.string(), label: v.string() },
        },
        multiModels: {},
      },
      migrate(m) {
        return m.compile();
      },
    },
  );

  // Fails on the DATA: `email` becomes required with no transformation to
  // fill it, so every document carried in from the parent violates the new
  // schema. Only data reaching the window can trigger this — an empty
  // `users` collection would validate clean.
  const requiredEmail = migrationDefinition(
    "2025_01_04_0000_DDDDDDDDDDDDDDDDDDDDDDDDDD@required_email",
    "required_email",
    {
      parent: forgotCreate,
      schemas: {
        collections: {
          users: {
            _id: v.string(),
            name: v.string(),
            slug: v.string(),
            email: v.string(),
          },
          tags: { _id: v.string(), label: v.string() },
        },
        multiModels: {},
      },
      migrate(m) {
        m.createCollection("tags");
        return m.compile();
      },
    },
  );

  return [root, slugged, forgotCreate, requiredEmail];
}

/** The reporter emits colour; assertions read the text underneath. */
function stripAnsi(text: string): string {
  // deno-lint-ignore no-control-regex
  return text.replace(/\x1b\[[0-9;]*m/g, "");
}

Deno.test("validate --last N: the window's verdicts are what the chain replay produced", async () => {
  const chain = failingTailChain();
  const WINDOW = 2;

  const reference = await validateByChainReplay(chain, WINDOW);
  // Both tail migrations must genuinely fail, or this proves nothing.
  assertEquals(
    reference.map((r) => r.valid),
    [false, false],
    "fixture must have a genuinely failing tail",
  );

  const error = await assertRejects(
    () => validateMigrationsWithSimulation(chain, { lastN: WINDOW }),
    MigrationValidationFailedError,
  );
  const actual = error.results;

  assertEquals(
    actual.map((r) => r.migration.id),
    reference.map((r) => r.migration.id),
    "the window must hold the same migrations",
  );
  for (let i = 0; i < reference.length; i++) {
    assertEquals(
      actual[i].valid,
      reference[i].valid,
      `verdict changed for ${reference[i].migration.id}`,
    );
    assertEquals(
      actual[i].errors,
      reference[i].errors,
      `errors changed for ${reference[i].migration.id}`,
    );
    assertEquals(
      actual[i].warnings,
      reference[i].warnings,
      `warnings changed for ${reference[i].migration.id}`,
    );
  }
});

Deno.test("validate --last N: migrations outside the window get no result and no green banner", async () => {
  const chain = failingTailChain().slice(0, 2); // the two clean ones
  const output: string[] = [];

  const results = await validateMigrationsWithSimulation(chain, {
    lastN: 1,
    tty: false,
    write: (chunk) => output.push(chunk),
  });

  assertEquals(
    results.map((r) => r.migration.id),
    [chain[1].id],
    "only the windowed migration may have a validation result",
  );

  const rendered = stripAnsi(output.join(""));
  assert(
    rendered.includes("Not validated: 1"),
    `the summary must count what it skipped, got:\n${rendered}`,
  );
  assert(
    !rendered.includes("All migrations are valid"),
    `a windowed run must not claim the full chain, got:\n${rendered}`,
  );
  assert(
    rendered.includes("were NOT validated"),
    `the banner must name the migrations left out, got:\n${rendered}`,
  );
});

Deno.test("validate --last N: a fully valid window still passes (no false negative)", async () => {
  const chain = failingTailChain().slice(0, 2);
  await validateMigrationsWithSimulation(chain, { lastN: 1 });
});

Deno.test("validate: a full check keeps its all-clear banner and validates everything", async () => {
  const chain = failingTailChain().slice(0, 2);
  const output: string[] = [];

  const results = await validateMigrationsWithSimulation(chain, {
    tty: false,
    write: (chunk) => output.push(chunk),
  });

  assertEquals(results.length, chain.length);
  const rendered = stripAnsi(output.join(""));
  assert(rendered.includes("All migrations are valid"));
  assert(!rendered.includes("Not validated"));
});
