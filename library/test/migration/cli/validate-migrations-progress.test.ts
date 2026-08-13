/**
 * Verrou — the in-flight line must be driven by the simulation's own work.
 *
 * Measured on the owner's 12-migration chain, `deno task db check` froze for
 * 17.1s at a stretch and drew ONE distinct spinner frame across the whole
 * run: `SimulationValidator` is `await`-heavy but every promise it awaits
 * resolves synchronously, so draining microtasks never reaches the timer
 * phase and the animation's `setInterval` never fired. Two fixes shipped
 * believing otherwise, both verified against a demo that awaited a real
 * `setTimeout`.
 *
 * These lock the replacement: the validator pushes progress notes out of its
 * hot loops, the CLI re-renders the line from them (a synchronous write, so
 * it paints while the thread is blocked), and the phase that runs AFTER a
 * verdict lands — state propagation, the slowest of the loop — gets a line of
 * its own instead of a silent screen.
 *
 * The last case is the one that matters most: none of this may change what
 * the check DETECTS.
 */
import { assert, assertEquals } from "@std/assert";
import { stripAnsiCode } from "@std/fmt/colors";
import { migrationDefinition } from "../../../src/migration/definition.ts";
import type { MigrationDefinition } from "../../../src/migration/types.ts";
import { validateMigrationsWithSimulation } from "../../../src/migration/cli/utils/validate-migrations.ts";
import { createSimulationValidator } from "../../../src/migration/validators/simulation.ts";
import { dbId } from "../../../src/ids.ts";

/** A chain of `length` migrations, each adding one collection. */
function chain(length: number): MigrationDefinition[] {
  const migrations: MigrationDefinition[] = [];
  let parent: MigrationDefinition | null = null;
  for (let i = 0; i < length; i++) {
    const collections: Record<string, unknown> = {};
    for (let j = 0; j <= i; j++) collections[`c${j}`] = { _id: dbId(`c${j}`) };
    const migration: MigrationDefinition = migrationDefinition(
      `2025_02_${String(i + 1).padStart(2, "0")}_0000_${
        "P".repeat(25)
      }${i}@step_${i}`,
      `step_${i}`,
      {
        parent,
        schemas: { collections: collections as never, multiModels: {} },
        migrate(m) {
          m.createCollection(`c${i}`);
          return m.compile();
        },
      },
    );
    migrations.push(migration);
    parent = migration;
  }
  return migrations;
}

/** Transient redraws start by erasing the row; committed lines end it. */
const isRedraw = (chunk: string) => chunk.startsWith("\r\x1b[K");
const isCommitted = (chunk: string) => chunk.endsWith("\n");

Deno.test("check progress: every in-flight line is redrawn from the work, not once", async () => {
  const chunks: string[] = [];
  await validateMigrationsWithSimulation(chain(3), {
    tty: true,
    write: (chunk) => void chunks.push(chunk),
    powerLevel: "quick",
  });

  // Group the redraws that happened between two committed lines: that window
  // is exactly "a step was in flight and nothing landed".
  const windows: string[][] = [];
  let current: string[] = [];
  for (const chunk of chunks) {
    if (isRedraw(chunk) && chunk !== "\r\x1b[K") current.push(chunk);
    if (isCommitted(chunk)) {
      if (current.length > 0) windows.push(current);
      current = [];
    }
  }
  if (current.length > 0) windows.push(current);

  assert(windows.length > 0, "no in-flight line was ever drawn on a TTY");
  for (const window of windows) {
    assert(
      window.length > 1,
      `a step drew its line once and then went silent: ${
        JSON.stringify(window.map(stripAnsiCode))
      }`,
    );
  }
  const notes = chunks.filter((c) => stripAnsiCode(c).includes(" · "));
  assert(
    notes.length > 0,
    "no progress note reached the screen — the line says nothing",
  );
});

Deno.test("check progress: the notes name the phase the loop is actually in", async () => {
  const notes: string[] = [];
  const validator = createSimulationValidator({
    powerLevel: "quick",
    onProgress: (note) => void notes.push(note),
  });
  const [first, second] = chain(2);

  const root = await validator.validateMigration(first);
  assertEquals(root.success, true);
  const afterRoot = notes.length;

  // Propagation is where the minutes go, and it only has work to do once a
  // state exists to retain and refill.
  const propagated = validator.prepareStateForNextMigration(
    root.data!.stateAfterMigration as never,
    second.schemas,
  );
  const afterPropagation = notes.length;

  const child = await validator.validateMigration(second, propagated);
  assertEquals(child.success, true);

  assert(
    notes.slice(0, afterRoot).some((n) => n.startsWith("applying operation")),
    `no operation note: ${JSON.stringify(notes.slice(0, afterRoot))}`,
  );
  assert(
    notes.slice(0, afterRoot).some((n) => n.startsWith("cloning state for")),
    "the schema-change phase reports nothing",
  );
  assert(
    notes.slice(afterRoot, afterPropagation).some((n) =>
      n.startsWith("mocking ")
    ),
    "state propagation — the slowest phase of the loop — reports nothing",
  );
  assert(
    notes.slice(afterPropagation).some((n) => n.startsWith("checking ")),
    "documents are validated one by one with nothing said about it",
  );
});

// Verrou — the propagation phase runs after the verdict has landed, so it
// owns no step line. It was measured at 5-16s per migration on the owner's
// chain: the single longest silence on screen, and the one nothing pointed at.
Deno.test("check progress: state propagation gets its own in-flight line", async () => {
  const chunks: string[] = [];
  await validateMigrationsWithSimulation(chain(3), {
    tty: true,
    write: (chunk) => void chunks.push(chunk),
    powerLevel: "quick",
  });

  const propagation = chunks.filter((c) =>
    stripAnsiCode(c).includes("propagating state")
  );
  assert(
    propagation.length > 0,
    "the phase that follows each verdict is still silent",
  );
  // Transient only: it must never survive into the committed report.
  assertEquals(
    propagation.some(isCommitted),
    false,
    "a progress line leaked into the report",
  );
});

// Verrou — a `--last N` fast-forward is not a skip.
//
// It runs the FULL simulation — that is how the state reaches the window —
// but it was drawn as a one-shot label on the premise that it "lands
// immediately". It does not: `check --last 2` on the owner's chain froze for
// 16.2s at a stretch, the same defect as the main loop, one call site over.
Deno.test("check progress: fast-forwarded migrations report progress too", async () => {
  const chunks: string[] = [];
  await validateMigrationsWithSimulation(chain(4), {
    tty: true,
    write: (chunk) => void chunks.push(chunk),
    powerLevel: "quick",
    lastN: 1,
  });

  const fastForward = chunks.filter((c) =>
    stripAnsiCode(c).includes("fast-forward")
  );
  assert(fastForward.length > 0, "no fast-forward line was drawn");
  assert(
    fastForward.some((c) => stripAnsiCode(c).includes(" · ")),
    `the fast-forward line says nothing while it works: ${
      JSON.stringify(fastForward.map(stripAnsiCode))
    }`,
  );
  // The collapsed summary stays the only committed trace of the window.
  assertEquals(
    chunks.filter((c) =>
      isCommitted(c) && stripAnsiCode(c).includes("fast-forward [")
    ),
    [],
    "a per-migration fast-forward step must stay transient",
  );
  assert(
    chunks.some((c) =>
      isCommitted(c) &&
      stripAnsiCode(c).includes("3 migration(s) fast-forwarded")
    ),
    "the collapsed summary must survive",
  );
});

// Verrou — progress is REPORTING. Adding it may not move a single verdict.
Deno.test("check progress: reporting progress changes nothing that is detected", async () => {
  const silent: string[] = [];
  const withProgress: string[] = [];

  const quiet = await validateMigrationsWithSimulation(chain(4), {
    tty: false,
    write: (chunk) => void silent.push(chunk),
    powerLevel: "quick",
  });
  const loud = await validateMigrationsWithSimulation(chain(4), {
    tty: true,
    write: (chunk) => void withProgress.push(chunk),
    powerLevel: "quick",
  });

  assertEquals(
    loud.map((r) => ({
      id: r.migration.id,
      valid: r.valid,
      errors: r.errors,
      warnings: r.warnings,
    })),
    quiet.map((r) => ({
      id: r.migration.id,
      valid: r.valid,
      errors: r.errors,
      warnings: r.warnings,
    })),
  );

  // The committed report is the same too — only the transient rows differ.
  const committed = withProgress.filter(isCommitted).join("");
  assertEquals(committed, silent.join(""));
});
