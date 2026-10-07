import { refId } from "../../src/ids.ts";
import { unique, withIndex } from "../../src/indexes.ts";
import {
  matchesPartialFilter,
  uniqueEntriesOf,
  uniqueKeysOf,
  uniqueMembership,
} from "../../src/privacy/unique-keys.ts";
import * as v from "../../src/schema.ts";
import { defineType } from "../../src/type-definition.ts";
import { assertEquals } from "../+assert.ts";
import { test } from "../+harness.ts";

const PARTITION = { instance: "", scope: "exposition:a" };

test("unique keys: field-level and composite keys are read from one place", () => {
  const source = defineType({
    schema: v.object({
      email: withIndex(v.string(), { unique: true, insensitive: true }),
      slug: withIndex(v.string(), { unique: true, global: true }),
      expositionId: refId("exposition"),
      badge: v.number(),
      items: v.array(
        v.object({ code: withIndex(v.string(), { unique: true }) }),
      ),
    }),
    indexes: (f) => [unique(f.expositionId, f.badge)],
  });
  const keys = uniqueKeysOf(source);
  assertEquals(
    keys.map((k) => [k.paths, k.global, k.caseInsensitive]),
    [
      [["email"], false, true],
      [["slug"], true, false],
      [["items.code"], false, false],
      [["expositionId", "badge"], false, false],
    ],
  );
  assertEquals(uniqueMembership(keys, "badge"), {
    unique: true,
    composite: true,
    caseInsensitive: false,
  });
  assertEquals(uniqueMembership(keys, "items.*.code").unique, true);
  assertEquals(uniqueMembership(keys, "email").caseInsensitive, true);
});

test("unique keys: a composite entry is the tuple, an array field yields one entry per element", () => {
  const composite = {
    paths: ["a", "b"],
    global: false,
    caseInsensitive: false,
  };
  const result = uniqueEntriesOf(
    composite,
    { a: "x", b: ["1", "2", "1"] },
    PARTITION,
  );
  assertEquals(result.covered && result.entries.length, 2);
});

test("unique keys: case-insensitive keys fold, others do not", () => {
  const key = { paths: ["login"], global: false, caseInsensitive: false };
  const entry = (login: string, caseInsensitive: boolean) => {
    const r = uniqueEntriesOf(
      { ...key, caseInsensitive },
      { login },
      PARTITION,
    );
    return r.covered ? r.entries[0] : undefined;
  };
  assertEquals(entry("Bob", false) === entry("bob", false), false);
  assertEquals(entry("Bob", true) === entry("bob", true), true);
});

test("unique keys: simple partial filters are evaluated, unknown operators stay unknown", () => {
  assertEquals(
    matchesPartialFilter({ type: "badge" }, { type: "badge" }),
    true,
  );
  assertEquals(matchesPartialFilter({ type: "badge" }, { type: "kek" }), false);
  assertEquals(
    matchesPartialFilter({ singleton: { $exists: true } }, { other: 1 }),
    false,
  );
  assertEquals(
    matchesPartialFilter(
      { $and: [{ a: 1 }, { b: { $in: [2, 3] } }] },
      { a: 1, b: 3 },
    ),
    true,
  );
  assertEquals(matchesPartialFilter({ n: { $gt: 3 } }, { n: 5 }), undefined);
  const r = uniqueEntriesOf(
    {
      paths: ["n"],
      global: false,
      caseInsensitive: false,
      partialFilter: { n: { $gt: 3 } },
    },
    { n: 5 },
    PARTITION,
  );
  assertEquals(r.covered, undefined);
});
