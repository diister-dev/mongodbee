import { test } from "./+harness.ts";
import { assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import {
  createLogger,
  type LogRecord,
  setLogSink,
} from "../src/utils/logger.ts";

test("logger: an application sink receives every warning mongodbee raises, the console is left alone", async (t) => {
  const records: LogRecord[] = [];
  const consoleCalls: unknown[][] = [];
  const originalLog = console.log;
  console.log = (...args: unknown[]) => {
    consoleCalls.push(args);
  };
  setLogSink((record) => records.push(record));
  try {
    await withDatabase(t.name, async (db) => {
      const people = await collection(db, "people", { name: v.string() });
      await people.collection.insertOne({ name: 42 } as never, {
        bypassDocumentValidation: true,
      });
      await people.find({}).toArray();
    });
  } finally {
    setLogSink(undefined);
    console.log = originalLog;
  }
  const warning = records.find((record) => record.namespace === "collection");
  assertEquals(warning?.level, "warn");
  assertEquals(
    warning?.message,
    "1 invalid documents were ignored during find operation",
  );
  assertEquals(
    consoleCalls.filter((call) =>
      String(call[0]).includes("[mongodbee:collection]"),
    ),
    [],
  );
});

test("logger: a sink that throws never loses the record, it falls back to the console", () => {
  const consoleCalls: unknown[][] = [];
  const originalLog = console.log;
  console.log = (...args: unknown[]) => {
    consoleCalls.push(args);
  };
  setLogSink(() => {
    throw new Error("sink down");
  });
  try {
    createLogger("fallback-test").warn("still visible");
  } finally {
    setLogSink(undefined);
    console.log = originalLog;
  }
  assertEquals(consoleCalls.length, 1);
  assertEquals(
    String(consoleCalls[0]![0]).startsWith(
      "[mongodbee:fallback-test] WARN still visible",
    ),
    true,
  );
});
