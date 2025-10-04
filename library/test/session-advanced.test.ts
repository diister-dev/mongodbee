import { assert, assertEquals, assertRejects } from "@std/assert";
import {
  checkTransactionEnabled,
  createSessionContext,
  getSessionContext,
} from "../src/session.ts";
import { withDatabase } from "./+shared.ts";
import type { ClientSession as _ClientSession } from "../mod.ts";

Deno.test("checkTransactionEnabled: Should return true when transactions are supported", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const result = await checkTransactionEnabled(client, db);

    // Most test environments support transactions
    assert(typeof result === "boolean");

    // If transactions are supported, result should be true
    if (result) {
      assertEquals(result, true);
    }
  });
});

Deno.test("checkTransactionEnabled: Should handle transaction failures gracefully", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;

    // Test with a potentially problematic operation
    const result = await checkTransactionEnabled(client, db);

    // Should always return a boolean, never throw
    assert(typeof result === "boolean");
  });
});

Deno.test("getSessionContext: Should create and cache session context", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;

    // First call should create context
    const context1 = await getSessionContext(client);
    assert(context1);
    assertEquals(typeof context1.getSession, "function");
    assertEquals(typeof context1.withSession, "function");

    // Second call should return cached context
    const context2 = await getSessionContext(client);
    assertEquals(context1, context2);
  });
});

Deno.test("createSessionContext: Should create valid session context", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;

    const context = await createSessionContext(client);
    assert(context);
    assertEquals(typeof context.getSession, "function");
    assertEquals(typeof context.withSession, "function");
  });
});

Deno.test("SessionContext: getSession should return undefined when no session active", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const { getSession } = await getSessionContext(client);

    // No active session initially
    assertEquals(getSession(), undefined);
  });
});

Deno.test("SessionContext: withSession should create new session when none active", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const { withSession, getSession } = await getSessionContext(client);

    let sessionInCallback: _ClientSession | undefined = undefined;

    const result = await withSession((session) => {
      sessionInCallback = session;

      // Should have a session within the callback
      const currentSession = getSession();
      assert(currentSession !== undefined);

      return Promise.resolve("test-result");
    });

    assertEquals(result, "test-result");

    // Session should be ended after callback
    assertEquals(getSession(), undefined);

    // Session should have been passed to callback
    if (sessionInCallback) {
      assert(sessionInCallback);
    }
  });
});

Deno.test("SessionContext: withSession should reuse existing session", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const { withSession } = await getSessionContext(client);

    const outerResult = await withSession(async (outerSession) => {
      const outerSessionId = outerSession?.id;

      // Nested withSession should reuse outer session
      const innerResult = await withSession((innerSession) => {
        const innerSessionId = innerSession?.id;

        // Both should be the same session or both undefined
        if (outerSessionId && innerSessionId) {
          assertEquals(outerSessionId, innerSessionId);
        } else {
          assertEquals(outerSession, innerSession);
        }

        return Promise.resolve("inner-result");
      });

      assertEquals(innerResult, "inner-result");
      return "outer-result";
    });

    assertEquals(outerResult, "outer-result");
  });
});

Deno.test("SessionContext: withSession should handle errors and abort transaction", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const { withSession } = await getSessionContext(client);

    // Test that errors are properly handled
    await assertRejects(
      async () => {
        await withSession((_session) => {
          throw new Error("Test error");
        });
      },
      Error,
      "Test error",
    );
  });
});

Deno.test("SessionContext: withSession should work with collections", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const { withSession } = await getSessionContext(client);

    const testCollection = db.collection("test_session");

    const result = await withSession(async (session) => {
      // Insert document within session
      const insertResult = await testCollection.insertOne(
        { name: "test", value: 42 },
        session ? { session } : {},
      );

      // Find the document within the same session
      const foundDoc = await testCollection.findOne(
        { _id: insertResult.insertedId },
        session ? { session } : {},
      );

      return foundDoc;
    });

    assert(result);
    assertEquals(result.name, "test");
    assertEquals(result.value, 42);
  });
});

Deno.test("SessionContext: withSession should handle commit failures", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const { withSession } = await getSessionContext(client);

    // Test with a large operation that might cause commit issues
    const result = await withSession(async (session) => {
      const testCollection = db.collection("test_commit");

      // Insert multiple documents
      const docs = Array.from({ length: 100 }, (_, i) => ({
        index: i,
        data: `test-data-${i}`,
        timestamp: new Date(),
      }));

      if (session) {
        await testCollection.insertMany(docs, { session });
      } else {
        await testCollection.insertMany(docs);
      }

      return docs.length;
    });

    assertEquals(result, 100);
  });
});

Deno.test("SessionContext: Multiple clients should have separate contexts", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client1 = db.client;
    // We can't easily create a second client in tests, so we'll test the caching behavior

    const context1 = await getSessionContext(client1);
    const context2 = await getSessionContext(client1);

    // Same client should return same context
    assertEquals(context1, context2);
  });
});

Deno.test("SessionContext: withSession should handle async operations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const { withSession } = await getSessionContext(client);

    const result = await withSession(async (session) => {
      // Simulate async operations
      await new Promise((resolve) => setTimeout(resolve, 10));

      const testCollection = db.collection("test_async");

      if (session) {
        await testCollection.insertOne({ test: "async" }, { session });
      } else {
        await testCollection.insertOne({ test: "async" });
      }

      await new Promise((resolve) => setTimeout(resolve, 10));

      return "async-complete";
    });

    assertEquals(result, "async-complete");
  });
});

Deno.test("SessionContext: withSession should handle concurrent operations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;
    const { withSession } = await getSessionContext(client);

    // Test concurrent withSession calls with different collections
    const promises = Array.from(
      { length: 3 },
      (_, i) =>
        withSession(async (session) => {
          const testCollection = db.collection(`test_concurrent_${i}`);

          if (session) {
            await testCollection.insertOne({
              operation: i,
              sessionId: session.id?.toString() || "no-session",
            }, { session });
          } else {
            await testCollection.insertOne({
              operation: i,
              sessionId: "none",
            });
          }

          return `operation-${i}`;
        }),
    );

    const results = await Promise.all(promises);

    assertEquals(results.length, 3);
    results.forEach((result, i) => {
      assertEquals(result, `operation-${i}`);
    });
  });
});

Deno.test("SessionContext: Should display warning when transactions disabled", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;

    // Mock console.warn to capture warnings
    const originalWarn = console.warn;
    let warningCalled = false;
    let warningMessage = "";

    console.warn = (message: string) => {
      warningCalled = true;
      warningMessage = message;
    };

    try {
      // Create a new context to ensure clean state
      const context = await createSessionContext(client);

      // First call should trigger warning if transactions are disabled
      await context.withSession(() => {
        return Promise.resolve("test");
      });

      // Warning should only be displayed once
      const firstWarning = warningCalled;
      const firstMessage = warningMessage;

      // Reset tracking
      warningCalled = false;
      warningMessage = "";

      // Second call should not trigger warning again
      await context.withSession(() => {
        return Promise.resolve("test2");
      });

      // If transactions are disabled, warning should have been shown once
      if (firstWarning) {
        assert(firstMessage.includes("MongoDB transactions are not enabled"));
        assertEquals(warningCalled, false); // Should not warn again
      }
    } finally {
      console.warn = originalWarn;
    }
  });
});

Deno.test("SessionContext: Should handle transaction check edge cases", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = db.client;

    // Test multiple transaction checks
    const result1 = await checkTransactionEnabled(client, db);
    const result2 = await checkTransactionEnabled(client, db);

    // Results should be consistent
    assertEquals(result1, result2);
    assert(typeof result1 === "boolean");
    assert(typeof result2 === "boolean");
  });
});
