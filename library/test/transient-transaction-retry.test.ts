import { test } from "./+harness.ts";
import * as m from "mongodb";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { getSessionContext } from "../src/session.ts";
import {
  isRetryableTransactionFailure,
  isWriteConflictError,
  retryOnWriteConflict,
  TRANSACTION_REPLAY,
} from "../src/utils/retry.ts";

function snapshotUnavailable(): m.MongoServerError {
  const error = new m.MongoServerError({
    code: 246,
    codeName: "SnapshotUnavailable",
    errmsg:
      "Unable to read from a snapshot due to pending collection catalog changes; please retry the operation.",
  });
  error.addErrorLabel("TransientTransactionError");
  return error;
}

test("transaction replay: a transient transaction error is retryable, an application error is not", () => {
  assert(isRetryableTransactionFailure(snapshotUnavailable()));
  assert(
    isRetryableTransactionFailure(
      new m.MongoServerError({ code: 112, errmsg: "WriteConflict" }),
    ),
  );
  assert(!isRetryableTransactionFailure(new Error("no element found")));
  assert(!isWriteConflictError(snapshotUnavailable()));
});

test("transaction replay: a transient failure inside the transaction is replayed with its writes rolled back", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);
    const notes = db.collection<{ attempt: number }>("notes");
    let attempts = 0;
    const write = () =>
      withSession(async (session) => {
        attempts++;
        await notes.insertOne({ attempt: attempts }, { session });
        if (attempts === 1) throw snapshotUnavailable();
      });

    await retryOnWriteConflict(write, TRANSACTION_REPLAY);

    assertEquals(attempts, 2);
    assertEquals(
      (await notes.find({}).toArray()).map((note) => note.attempt),
      [2],
    );

    attempts = 0;
    await assertRejects(() => retryOnWriteConflict(write, { maxRetries: 8 }));
    assertEquals(attempts, 1);
  });
});
