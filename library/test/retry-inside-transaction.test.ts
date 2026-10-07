import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assert, assertEquals } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import {
  isTransactionScopedError,
  retryOnWriteConflict,
} from "../src/utils/retry.ts";

const counterSchema = { value: v.number() };
const WRITE_CONFLICT = 112;

test("retry: a write conflict inside a transaction is handed to the transaction's owner, never replayed in an aborted transaction", async (t) => {
  await withDatabase(t.name, async (db) => {
    const counters = await collection(db, "counters", counterSchema);
    const id = await counters.insertOne({ value: 0 });

    let releaseHolder!: () => void;
    const holderMayCommit = new Promise<void>((resolve) => {
      releaseHolder = resolve;
    });
    let holderWrote!: () => void;
    const holderHasTheDocument = new Promise<void>((resolve) => {
      holderWrote = resolve;
    });

    const holder = counters.withSession(async () => {
      await counters.updateOne({ _id: id }, { $set: { value: 1 } });
      holderWrote();
      await holderMayCommit;
    });
    await holderHasTheDocument;

    const started = Date.now();
    let caught: unknown;
    try {
      await counters.withSession(async () => {
        await counters.updateOne({ _id: id }, { $set: { value: 2 } });
      });
    } catch (error) {
      caught = error;
    }
    const elapsed = Date.now() - started;
    releaseHolder();
    await holder;

    assert(caught !== undefined, "the conflicting transaction fails");
    assert(
      isTransactionScopedError(caught),
      `the failure is the transaction's to replay, got: ${String(caught)}`,
    );
    assertEquals(
      (caught as { code?: unknown }).code,
      WRITE_CONFLICT,
      "the original conflict surfaces, not the abort a replay would cause",
    );
    assert(
      elapsed < 1_000,
      `no retry backoff was spent inside the aborted transaction (${elapsed} ms)`,
    );
    assertEquals((await counters.getById(id)).value, 1);
  });
});

test("retry: a whole transaction wrapped in a retry is replayed after a conflict", async (t) => {
  await withDatabase(t.name, async (db) => {
    const counters = await collection(db, "counters", counterSchema);
    const id = await counters.insertOne({ value: 0 });

    let releaseHolder!: () => void;
    const holderMayCommit = new Promise<void>((resolve) => {
      releaseHolder = resolve;
    });
    let holderWrote!: () => void;
    const holderHasTheDocument = new Promise<void>((resolve) => {
      holderWrote = resolve;
    });
    const holder = counters.withSession(async () => {
      await counters.updateOne({ _id: id }, { $set: { value: 1 } });
      holderWrote();
      await holderMayCommit;
    });
    await holderHasTheDocument;

    let attempts = 0;
    const replayed = retryOnWriteConflict(
      () =>
        counters.withSession(async () => {
          attempts++;
          if (attempts === 2) releaseHolder();
          const current = await counters.getById(id);
          await counters.updateOne(
            { _id: id },
            { $set: { value: current.value + 10 } },
          );
        }),
      { maxRetries: 10, initialDelay: 20, jitter: false },
    );
    await Promise.all([holder, replayed]);

    assert(
      attempts >= 2,
      `the whole transaction was replayed (${attempts} attempts)`,
    );
    assertEquals(
      (await counters.getById(id)).value,
      11,
      "the replay read the committed value",
    );
  });
});

test("retry: outside a transaction, a transaction-scoped error is still recognised by label", () => {
  const labelled = Object.assign(new Error("WriteConflict"), {
    code: 112,
    hasErrorLabel: (label: string) => label === "TransientTransactionError",
  });
  const aborted = Object.assign(
    new Error("Transaction with { txnNumber: 1 } has been aborted."),
    { code: 251 },
  );
  const plain = Object.assign(new Error("WriteConflict"), {
    code: 112,
    hasErrorLabel: () => false,
  });

  assert(isTransactionScopedError(labelled));
  assert(isTransactionScopedError(aborted));
  assert(!isTransactionScopedError(plain));
  assert(!isTransactionScopedError(new Error("no element")));
});
