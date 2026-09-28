import { test } from "./+harness.ts";
import * as m from "mongodb";
import * as v from "../src/schema.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import type { Db } from "../src/mongodb.ts";
import { getSessionContext } from "../src/session.ts";
import { afterCommit, insideTransaction } from "../src/transaction-scope.ts";
import { withDatabase } from "./+shared.ts";

function notesOf(db: Db) {
  return collection(db, "notes", { text: v.string() });
}

async function withNotes(
  name: string,
  work: (
    notes: Awaited<ReturnType<typeof notesOf>>,
    withSession: ReturnType<typeof getSessionContext>["withSession"],
  ) => Promise<void>,
) {
  await withDatabase(name, async (db) => {
    await work(await notesOf(db), getSessionContext(db.client).withSession);
  });
}

function transientError(): m.MongoError {
  const error = new m.MongoError("transient");
  error.addErrorLabel("TransientTransactionError");
  return error;
}

test("afterCommit: runs once the transaction has committed, and sees its writes", async (t) => {
  await withNotes(t.name, async (notes, withSession) => {
    const seen: number[] = [];
    await withSession(async () => {
      await notes.insertOne({ text: "committed" });
      await afterCommit(async () => {
        seen.push(await notes.countDocuments({ text: "committed" }));
      });
      assertEquals(seen, []);
    });
    assertEquals(seen, [1]);
  });
});

test("afterCommit: never runs for a transaction that rolls back", async (t) => {
  await withNotes(t.name, async (notes, withSession) => {
    let ran = false;
    await assertRejects(() =>
      withSession(async () => {
        await notes.insertOne({ text: "doomed" });
        await afterCommit(() => {
          ran = true;
        });
        throw new Error("rollback");
      }),
    );
    assertEquals(ran, false);
    assertEquals(await notes.countDocuments({ text: "doomed" }), 0);
  });
});

test("afterCommit: a retried transaction runs the callbacks of its last attempt only", async (t) => {
  await withNotes(t.name, async (notes, withSession) => {
    const ran: number[] = [];
    let attempt = 0;
    await withSession(
      async () => {
        attempt++;
        const current = attempt;
        await notes.insertOne({ text: `attempt ${current}` });
        await afterCommit(() => {
          ran.push(current);
        });
        if (current === 1) throw transientError();
      },
      { retry: true },
    );
    assertEquals(ran, [2]);
  });
});

test("afterCommit: outside a transaction the callback runs right away", async () => {
  const ran: string[] = [];
  assertEquals(insideTransaction(), false);
  await afterCommit(() => {
    ran.push("now");
  });
  assertEquals(ran, ["now"]);
});

test("afterCommit: a nested withSession defers to the outer commit", async (t) => {
  await withNotes(t.name, async (notes, withSession) => {
    const order: string[] = [];
    await withSession(async () => {
      await withSession(async () => {
        await notes.insertOne({ text: "inner" });
        await afterCommit(() => {
          order.push("callback");
        });
      });
      order.push("inner returned");
    });
    assertEquals(order, ["inner returned", "callback"]);
  });
});

test("afterCommit: a failing callback neither fails the commit nor stops the others", async (t) => {
  await withNotes(t.name, async (notes, withSession) => {
    const ran: string[] = [];
    const result = await withSession(async () => {
      await notes.insertOne({ text: "kept" });
      await afterCommit(() => {
        throw new Error("callback failure");
      });
      await afterCommit(() => {
        ran.push("second");
      });
      return "done";
    });
    assertEquals(result, "done");
    assertEquals(ran, ["second"]);
    assertEquals(await notes.countDocuments({ text: "kept" }), 1);
  });
});

test("afterCommit: the callback runs outside the ended session and can write", async (t) => {
  await withNotes(t.name, async (notes, withSession) => {
    let inside: boolean | undefined;
    await withSession(async () => {
      await notes.insertOne({ text: "fact" });
      await afterCommit(async () => {
        inside = insideTransaction();
        await notes.insertOne({ text: "follow-up" });
      });
    });
    assert(inside === false);
    assertEquals(await notes.countDocuments({ text: "follow-up" }), 1);
  });
});
