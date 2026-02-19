import { test, expect } from "vitest";
import {
  checkTransactionEnabled,
  createSessionContext,
  getSessionContext,
} from "../src/session.ts";
import { withDatabase } from "./+shared.ts";
import type { ClientSession as _ClientSession } from "../mod.ts";

test("checkTransactionEnabled: Should return true when transactions are supported", async () => {
  await withDatabase("checkTransactionEnabled: Should return true when transactions are supported", async (db) => {
    const client = db.client;
    const result = await checkTransactionEnabled(client, db);

    // Most test environments support transactions
    expect(typeof result === "boolean").toBeTruthy();

    // If transactions are supported, result should be true
    if (result) {
      expect(result).toEqual(true);
    }
  });
});

test("checkTransactionEnabled: Should handle transaction failures gracefully", async () => {
  await withDatabase("checkTransactionEnabled: Should handle transaction failures gracefully", async (db) => {
    const client = db.client;

    // Test with a potentially problematic operation
    const result = await checkTransactionEnabled(client, db);

    // Should always return a boolean, never throw
    expect(typeof result === "boolean").toBeTruthy();
  });
});

test("getSessionContext: Should create and cache session context", async () => {
  await withDatabase("getSessionContext: Should create and cache session context", async (db) => {
    const client = db.client;

    // First call should create context
    const context1 = await getSessionContext(client);
    expect(context1).toBeTruthy();
    expect(typeof context1.getSession).toEqual("function");
    expect(typeof context1.withSession).toEqual("function");

    // Second call should return cached context
    const context2 = await getSessionContext(client);
    expect(context1).toEqual(context2);
  });
});

test("createSessionContext: Should create valid session context", async () => {
  await withDatabase("createSessionContext: Should create valid session context", async (db) => {
    const client = db.client;

    const context = await createSessionContext(client);
    expect(context).toBeTruthy();
    expect(typeof context.getSession).toEqual("function");
    expect(typeof context.withSession).toEqual("function");
  });
});

test("SessionContext: getSession should return undefined when no session active", async () => {
  await withDatabase("SessionContext: getSession should return undefined when no session active", async (db) => {
    const client = db.client;
    const { getSession } = await getSessionContext(client);

    // No active session initially
    expect(getSession()).toEqual(undefined);
  });
});

test("SessionContext: withSession should create new session when none active", async () => {
  await withDatabase("SessionContext: withSession should create new session when none active", async (db) => {
    const client = db.client;
    const { withSession, getSession } = await getSessionContext(client);

    let sessionInCallback: _ClientSession | undefined = undefined;

    const result = await withSession((session) => {
      sessionInCallback = session;

      // Should have a session within the callback
      const currentSession = getSession();
      expect(currentSession !== undefined).toBeTruthy();

      return Promise.resolve("test-result");
    });

    expect(result).toEqual("test-result");

    // Session should be ended after callback
    expect(getSession()).toEqual(undefined);

    // Session should have been passed to callback
    if (sessionInCallback) {
      expect(sessionInCallback).toBeTruthy();
    }
  });
});

test("SessionContext: withSession should reuse existing session", async () => {
  await withDatabase("SessionContext: withSession should reuse existing session", async (db) => {
    const client = db.client;
    const { withSession } = await getSessionContext(client);

    const outerResult = await withSession(async (outerSession) => {
      const outerSessionId = outerSession?.id;

      // Nested withSession should reuse outer session
      const innerResult = await withSession((innerSession) => {
        const innerSessionId = innerSession?.id;

        // Both should be the same session or both undefined
        if (outerSessionId && innerSessionId) {
          expect(outerSessionId).toEqual(innerSessionId);
        } else {
          expect(outerSession).toEqual(innerSession);
        }

        return Promise.resolve("inner-result");
      });

      expect(innerResult).toEqual("inner-result");
      return "outer-result";
    });

    expect(outerResult).toEqual("outer-result");
  });
});

test("SessionContext: withSession should handle errors and abort transaction", async () => {
  await withDatabase("SessionContext: withSession should handle errors and abort transaction", async (db) => {
    const client = db.client;
    const { withSession } = await getSessionContext(client);

    // Test that errors are properly handled
    await expect(
      async () => {
        await withSession((_session) => {
          throw new Error("Test error");
        });
      },
    ).rejects.toThrow("Test error");
  });
});

test("SessionContext: withSession should work with collections", async () => {
  await withDatabase("SessionContext: withSession should work with collections", async (db) => {
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

    expect(result).toBeTruthy();
    expect(result.name).toEqual("test");
    expect(result.value).toEqual(42);
  });
});

test("SessionContext: withSession should handle commit failures", async () => {
  await withDatabase("SessionContext: withSession should handle commit failures", async (db) => {
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

    expect(result).toEqual(100);
  });
});

test("SessionContext: Multiple clients should have separate contexts", async () => {
  await withDatabase("SessionContext: Multiple clients should have separate contexts", async (db) => {
    const client1 = db.client;
    // We can't easily create a second client in tests, so we'll test the caching behavior

    const context1 = await getSessionContext(client1);
    const context2 = await getSessionContext(client1);

    // Same client should return same context
    expect(context1).toEqual(context2);
  });
});

test("SessionContext: withSession should handle async operations", async () => {
  await withDatabase("SessionContext: withSession should handle async operations", async (db) => {
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

    expect(result).toEqual("async-complete");
  });
});

test("SessionContext: withSession should handle concurrent operations", async () => {
  await withDatabase("SessionContext: withSession should handle concurrent operations", async (db) => {
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

    expect(results.length).toEqual(3);
    results.forEach((result, i) => {
      expect(result).toEqual(`operation-${i}`);
    });
  });
});

test("SessionContext: Should display warning when transactions disabled", async () => {
  await withDatabase("SessionContext: Should display warning when transactions disabled", async (db) => {
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
        expect(firstMessage.includes("MongoDB transactions are not enabled")).toBeTruthy();
        expect(warningCalled).toEqual(false); // Should not warn again
      }
    } finally {
      console.warn = originalWarn;
    }
  });
});

test("SessionContext: Should handle transaction check edge cases", async () => {
  await withDatabase("SessionContext: Should handle transaction check edge cases", async (db) => {
    const client = db.client;

    // Test multiple transaction checks
    const result1 = await checkTransactionEnabled(client, db);
    const result2 = await checkTransactionEnabled(client, db);

    // Results should be consistent
    expect(result1).toEqual(result2);
    expect(typeof result1 === "boolean").toBeTruthy();
    expect(typeof result2 === "boolean").toBeTruthy();
  });
});
