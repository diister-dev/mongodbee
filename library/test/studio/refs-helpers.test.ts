import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { resolveReference } from "../../src/studio/ui/lib/refs.ts";
import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { idPrefixOf } from "../../src/studio/api/overview.ts";

const collections = [
  {
    name: "+users",
    kind: "collection",
    types: [{ name: "+users", count: 30, idPrefix: "user" }],
  },
  { name: "+roles", kind: "collection", types: [{ name: "+roles", count: 5 }] },
  {
    name: "+entreprises",
    kind: "multiCollection",
    types: [
      { name: "entreprise", count: 15 },
      { name: "member", count: 27, idPrefix: "entreprise_member" },
      { name: "contact", count: 0 },
    ],
  },
  {
    name: "+expositions",
    kind: "scopedMultiCollection",
    types: [
      { name: "participant", count: 100 },
      { name: "information", count: 12, meta: true as const },
    ],
  },
];

test("resolveReference follows the id prefix of a plain collection", () => {
  assertEquals(resolveReference(collections, "user"), { collection: "+users" });
  assertEquals(resolveReference(collections, "+roles"), {
    collection: "+roles",
  });
});

test("resolveReference finds typed documents by type name, then by id prefix", () => {
  assertEquals(resolveReference(collections, "participant"), {
    collection: "+expositions",
    type: "participant",
  });
  assertEquals(resolveReference(collections, "entreprise_member"), {
    collection: "+entreprises",
    type: "member",
  });
});

test("resolveReference skips empty and meta types and unknown prefixes", () => {
  assertEquals(resolveReference(collections, "contact"), undefined);
  assertEquals(resolveReference(collections, "information"), undefined);
  assertEquals(resolveReference(collections, "ghost"), undefined);
  assertEquals(resolveReference(undefined, "user"), undefined);
});

test("idPrefixOf reads the refId prefix declared on _id", () => {
  assertEquals(
    idPrefixOf({ _id: v.optional(refId("user")), email: v.string() } as never),
    "user",
  );
  assertEquals(idPrefixOf({ email: v.string() } as never), undefined);
  assertEquals(idPrefixOf(undefined), undefined);
});
