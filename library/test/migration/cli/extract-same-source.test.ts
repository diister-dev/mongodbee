import { test } from "../../+harness.ts";
import { assertEquals } from "../../+assert.ts";
import {
  type DatabaseFingerprint,
  isSameDatabase,
} from "../../../src/migration/cli/commands/extract.ts";

const fingerprint = (
  processId: string | undefined,
  ...collectionIds: string[]
): DatabaseFingerprint => ({
  processId,
  collectionIds: new Set(collectionIds),
});

test("isSameDatabase: a shared collection identifies the database across processes", () => {
  assertEquals(
    isSameDatabase(
      fingerprint("primary", "a", "b"),
      fingerprint("secondary", "b"),
      true,
    ),
    true,
  );
});

test("isSameDatabase: a shared collection identifies the database whatever its name", () => {
  assertEquals(
    isSameDatabase(fingerprint("one", "a"), fingerprint("two", "a"), false),
    true,
  );
});

test("isSameDatabase: distinct servers holding a same-named database are told apart", () => {
  assertEquals(
    isSameDatabase(fingerprint("one", "a"), fingerprint("two", "b"), true),
    false,
  );
});

test("isSameDatabase: the same process and name identifies a database with no collection", () => {
  assertEquals(
    isSameDatabase(fingerprint("one"), fingerprint("one"), true),
    true,
  );
});

test("isSameDatabase: the same process under another name is another database", () => {
  assertEquals(
    isSameDatabase(fingerprint("one"), fingerprint("one"), false),
    false,
  );
});

test("isSameDatabase: a server without a process id is not taken for the source", () => {
  assertEquals(
    isSameDatabase(fingerprint(undefined), fingerprint(undefined), true),
    false,
  );
});
