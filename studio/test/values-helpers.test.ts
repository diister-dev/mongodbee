import { test } from "../../library/test/+harness.ts";
import { assert, assertEquals } from "../../library/test/+assert.ts";
import { ulid } from "@diister/mongodbee/ids";
import {
  durationParts,
  idTime,
  fieldFamily,
  formatBytes,
  formatCompactDate,
  formatNumberValue,
  hashString,
  hueFor,
  hueForOption,
  isEmail,
  isUrl,
  middleTruncate,
  objectIdTime,
  PALETTE,
  parseTypedId,
  relativeTime,
  splitMigrationId,
  splitObjectId,
  splitUlid,
  ulidTime,
} from "../src/ui/lib/values.ts";

test("values: ULID time decoding, lowercase included", () => {
  assertEquals(ulidTime("01ARZ3NDEKTSV4RRFFQ69G5FAV"), 1469922850259);
  assertEquals(ulidTime("01arz3ndektsv4rrffq69g5fav"), 1469922850259);
  const before = Date.now();
  const fresh = ulid().toLowerCase();
  const decoded = ulidTime(fresh);
  assert(decoded !== null && decoded >= before - 1 && decoded <= Date.now());
  assertEquals(ulidTime("00956ah0m6caqd"), null);
  assertEquals(ulidTime("01ARZ3NDEKTSV4RRFFQ69G5FAU"), null);
  assertEquals(splitUlid("01ARZ3NDEKTSV4RRFFQ69G5FAV"), [
    "01ARZ3NDEK",
    "TSV4RRFFQ69G5FAV",
  ]);
});

test("values: ObjectId timestamp", () => {
  assertEquals(objectIdTime("65f000000000000000000000"), 0x65f00000 * 1000);
  assertEquals(objectIdTime("not-an-object-id"), null);
  assertEquals(splitObjectId("65f0a1b2c3d4e5f6a7b8c9d0"), [
    "65f0a1b2",
    "c3d4e5f6a7",
    "b8c9d0",
  ]);
});

test("values: stable colour hashing", () => {
  assertEquals(hashString("artwork"), hashString("artwork"));
  assertEquals(hueFor("artwork"), hueFor("artwork"));
  assert(PALETTE.includes(hueFor("anything at all")));
  const spread = new Set(
    ["artwork", "visit", "product", "category", "note", "tag", "user"].map(
      (name) => hueFor(name).name,
    ),
  );
  assert(spread.size >= 3, "names land on several hues");
  assertEquals(hueForOption("draft", ["draft", "published"]), PALETTE[0]);
  assertEquals(hueForOption("published", ["draft", "published"]), PALETTE[1]);
  assertEquals(hueForOption("x", undefined, "f"), hueFor("f:x"));
});

test("values: middle truncation", () => {
  assertEquals(middleTruncate("short", 10), "short");
  assertEquals(middleTruncate("abcdefghijklmnop", 9), "abcd…mnop");
  assertEquals(middleTruncate("abcdefghijklmnop", 10).length, 10);
  assertEquals(middleTruncate("abcdef", 2), "abcdef");
});

test("values: shapes and formatting", () => {
  assertEquals(parseTypedId("artwork:07kxfuw17joulk"), {
    prefix: "artwork",
    id: "07kxfuw17joulk",
  });
  assertEquals(parseTypedId("https://example.com"), null);
  assertEquals(parseTypedId("plain"), null);
  assert(isEmail("user@example.com"));
  assert(!isEmail("user@"));
  assert(isUrl("https://example.com/a"));
  assert(!isUrl("example.com"));
  assertEquals(splitMigrationId("2026_09_27_001_initial"), {
    date: "2026_09_27",
    name: "001_initial",
  });
  assertEquals(splitMigrationId("002"), { name: "002" });
  assertEquals(formatCompactDate(Date.UTC(2026, 3, 22)), "2026-04-22");
  assertEquals(
    formatCompactDate(Date.UTC(2026, 3, 22, 9, 5)),
    "2026-04-22 09:05",
  );
  assertEquals(formatNumberValue(1808), "1808");
  assertEquals(formatNumberValue(0.1 + 0.2), "0.3");
  const now = Date.UTC(2026, 0, 10);
  assertEquals(relativeTime(now - 3 * 24 * 3600 * 1000, now), "3 days ago");
  assertEquals(relativeTime(now + 2 * 3600 * 1000, now), "in 2 hours");
});

test("fieldFamily sorts schema nodes into picker groups", () => {
  assertEquals(fieldFamily("_type"), "identity");
  assertEquals(fieldFamily("x"), "other");
  assertEquals(fieldFamily("x", { kind: "string", ref: "user" }), "reference");
  assertEquals(fieldFamily("x", { kind: "string" }), "text");
  assertEquals(fieldFamily("x", { kind: "bigint" }), "number");
  assertEquals(fieldFamily("x", { kind: "picklist" }), "choice");
  assertEquals(fieldFamily("x", { kind: "array" }), "list");
  assertEquals(fieldFamily("x", { kind: "strict_object" }), "object");
  assertEquals(fieldFamily("x", { kind: "custom" }), "other");
});

test("formatBytes picks a binary unit and trims useless decimals", () => {
  assertEquals(formatBytes(0), "0 B");
  assertEquals(formatBytes(1023), "1023 B");
  assertEquals(formatBytes(1024), "1 KiB");
  assertEquals(formatBytes(40960), "40 KiB");
  assertEquals(formatBytes(1536), "1.5 KiB");
  assertEquals(formatBytes(250 * 1024 * 1024), "250 MiB");
  assertEquals(formatBytes(-1), "");
});

test("durationParts picks the unit a reader expects", () => {
  assertEquals(durationParts(840), [["840", "ms"]]);
  assertEquals(durationParts(4260), [["4.2", "s"]]);
  assertEquals(durationParts(12_343), [["12", "s"]]);
  assertEquals(durationParts(125_000), [
    ["2", "min"],
    ["5", "s"],
  ]);
  assertEquals(durationParts(180_000), [["3", "min"]]);
  assertEquals(durationParts(3_840_000), [
    ["1", "h"],
    ["4", "min"],
  ]);
  assertEquals(durationParts(-1), [["0", "ms"]]);
});

test("idTime reads the creation time of ULID, typed and ObjectId ids", () => {
  const id = ulid();
  const time = idTime(id)!;
  assert(Math.abs(time - Date.now()) < 60_000);
  assertEquals(idTime(`user:${id.toLowerCase()}`), time);
  assertEquals(idTime({ $oid: "65a1b2c3d4e5f60718293a4b" }), 0x65a1b2c3 * 1000);
  assertEquals(idTime("65a1b2c3d4e5f60718293a4b"), 0x65a1b2c3 * 1000);
  assertEquals(idTime("not an id"), null);
  assertEquals(idTime(42), null);
});
