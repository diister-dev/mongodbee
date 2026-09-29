import { test } from "../../library/test/+harness.ts";
import { assertEquals } from "../../library/test/+assert.ts";
import * as v from "@diister/mongodbee/schema";
import { nestedPaths, nodeAtPath } from "../src/field-paths.ts";
import { entriesToNodes } from "@diister/mongodbee/inspect";

const fields = entriesToNodes({
  name: v.string(),
  address: v.optional(
    v.object({ city: v.string(), geo: v.object({ lat: v.number() }) }),
  ),
  contacts: v.array(v.object({ email: v.string(), primary: v.boolean() })),
  kind: v.variant("type", [
    v.object({ type: v.literal("a"), size: v.number() }),
    v.object({ type: v.literal("b"), label: v.string() }),
  ]),
});

test("nodeAtPath follows objects, arrays of objects and variant options", () => {
  assertEquals(nodeAtPath(fields, "name")?.kind, "string");
  assertEquals(nodeAtPath(fields, "address.city")?.kind, "string");
  assertEquals(nodeAtPath(fields, "address.geo.lat")?.kind, "number");
  assertEquals(nodeAtPath(fields, "contacts.primary")?.kind, "boolean");
  assertEquals(nodeAtPath(fields, "kind.size")?.kind, "number");
  assertEquals(nodeAtPath(fields, "kind.label")?.kind, "string");
  assertEquals(nodeAtPath(fields, "address.nope"), undefined);
  assertEquals(nodeAtPath(fields, "name.length"), undefined);
});

test("nestedPaths lists every dotted path once, bounded in depth", () => {
  assertEquals(
    nestedPaths(fields).map((entry) => entry.path),
    [
      "address.city",
      "address.geo",
      "address.geo.lat",
      "contacts.email",
      "contacts.primary",
      "kind.type",
      "kind.size",
      "kind.label",
    ],
  );
  assertEquals(
    nestedPaths(fields, 2).map((entry) => entry.path),
    [
      "address.city",
      "address.geo",
      "contacts.email",
      "contacts.primary",
      "kind.type",
      "kind.size",
      "kind.label",
    ],
  );
});
