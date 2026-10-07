/**
 * Regex-backed fields in the simulation: generated, or failed fast and quiet.
 *
 * A `path` validated by `/^\/(?:[^/]+\/)*$/` (folder paths) made the mock
 * generator fail every draw and retry for ~15 s per failure, 20 to 40 s per
 * simulated migration, while it printed each rejected value (kilobytes of
 * random characters) to the console: 2 MB of output for one `migrate`.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import {
  createEmptyDatabaseState,
  type MockGenerationFailure,
} from "../../src/migration/types.ts";
import {
  createCorrelationSession,
  getMockGenerationConfig,
  type MockPopulateContext,
  populateCollections,
} from "../../src/migration/validators/mock/mod.ts";
import {
  foldMockGenerationFailures,
  MAX_FAILURE_MESSAGE_LENGTH,
} from "../../src/migration/validators/mock/failures.ts";

const PATH = /^\/(?:[^/]+\/)*$/;

function ctx(): MockPopulateContext {
  return {
    config: getMockGenerationConfig("normal"),
    failures: [] as MockGenerationFailure[],
    session: createCorrelationSession({ schemas: {}, seed: 7 }),
  };
}

/** Runs `work` with every console channel captured. */
function silenced<T>(work: () => T): { value: T; printed: number } {
  const saved = {
    log: console.log,
    error: console.error,
    warn: console.warn,
  };
  let printed = 0;
  const count = () => {
    printed++;
  };
  console.log = count;
  console.error = count;
  console.warn = count;
  try {
    return { value: work(), printed };
  } finally {
    Object.assign(console, saved);
  }
}

test("a folder-path field is generated, valid, without retries", () => {
  const schema = {
    _id: v.string(),
    path: v.pipe(v.string(), v.maxLength(1_024), v.regex(PATH)),
  };
  const state = createEmptyDatabaseState();
  const context = ctx();
  const started = Date.now();
  const { printed } = silenced(() =>
    populateCollections(state, { documents: schema }, "always", context),
  );
  assert(Date.now() - started < 3_000, `took ${Date.now() - started}ms`);
  assertEquals(context.failures, []);
  assertEquals(printed, 0);
  const documents = state.collections.documents.content;
  assertEquals(documents.length, 100);
  for (const document of documents) {
    assert(PATH.test(document.path as string), String(document.path));
    assert((document.path as string).length <= 1_024);
  }
});

test("an unreachable pattern fails fast, quietly, naming the field and pattern", () => {
  const schema = {
    _id: v.string(),
    code: v.pipe(v.string(), v.regex(/^(?=a)b$/)),
  };
  const state = createEmptyDatabaseState();
  const context = ctx();
  const started = Date.now();
  const { printed } = silenced(() =>
    populateCollections(state, { codes: schema }, "always", context),
  );
  assert(Date.now() - started < 1_000, `took ${Date.now() - started}ms`);
  assertEquals(printed, 0, "nothing may be printed, least of all the value");
  assertEquals(context.failures.length, 1);
  const { errors } = foldMockGenerationFailures(
    context.failures,
    { collections: { codes: schema } },
    state,
  );
  assertEquals(errors.length, 1);
  assert(errors[0].includes('"code"'), errors[0]);
  assert(errors[0].includes("/^(?=a)b$/"), errors[0]);
});

test("a failure message is summarized, never carried whole", () => {
  const state = createEmptyDatabaseState();
  const huge = `Invalid format: received "${"x".repeat(50_000)}"`;
  const { warnings } = foldMockGenerationFailures(
    [{ bucket: "collections", collection: "gone", message: huge }],
    { collections: {} },
    state,
  );
  assertEquals(warnings.length, 1);
  assert(
    warnings[0].length < MAX_FAILURE_MESSAGE_LENGTH + 200,
    `${warnings[0].length} characters`,
  );
});
