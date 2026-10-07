import { test } from "../../library/test/+harness.ts";
import { assertEquals } from "../../library/test/+assert.ts";
import {
  diffDocument,
  editableOf,
  hasChanges,
  parseEditable,
  templateOf,
} from "../src/ui/lib/edit.ts";

const original = {
  _id: "user:01",
  _type: "user",
  name: "Ada",
  age: 36,
  address: { city: "London", zip: null },
  tags: ["a"],
};

test("editableOf leaves the managed fields out", () => {
  assertEquals(Object.keys(editableOf(original)), [
    "name",
    "age",
    "address",
    "tags",
  ]);
});

test("diffDocument sets changed and added fields, unsets removed ones, and guards on the old values", () => {
  const change = diffDocument(original, {
    name: "Ada Lovelace",
    address: { zip: null, city: "London" },
    tags: ["a"],
    nick: "A",
  });
  assertEquals(change.changed, ["name"]);
  assertEquals(change.added, ["nick"]);
  assertEquals(change.removed, ["age"]);
  assertEquals(change.set, { name: "Ada Lovelace", nick: "A" });
  assertEquals(change.unset, ["age"]);
  assertEquals(change.expected, { name: "Ada", age: 36 });
  assertEquals(hasChanges(diffDocument(original, editableOf(original))), false);
});

test("parseEditable reports the line of a syntax error and refuses managed fields", () => {
  const broken = parseEditable('{\n  "name": "Ada",\n  "age": \n}');
  assertEquals(broken.ok, false);
  assertEquals(parseEditable('{"_id": "x"}'), {
    ok: false,
    message: "_id cannot be edited here",
  });
  assertEquals(parseEditable("[1]"), {
    ok: false,
    message: "The document must be a JSON object",
  });
  assertEquals(parseEditable('{"a": 1}'), { ok: true, value: { a: 1 } });
});

test("templateOf fills the required fields with a value of their kind", () => {
  assertEquals(
    templateOf({
      _id: { kind: "string" },
      name: { kind: "string" },
      nick: { kind: "string", optional: true },
      role: { kind: "picklist", values: ["admin", "viewer"] },
      managerId: { kind: "string", ref: "user", nullable: true },
      owner: { kind: "string", ref: "user" },
      active: { kind: "boolean", hasDefault: true },
      address: {
        kind: "object",
        entries: {
          city: { kind: "string" },
          zip: { kind: "number", optional: true },
        },
      },
      born: { kind: "date" },
    }),
    {
      name: "",
      role: "admin",
      managerId: null,
      owner: "user:",
      address: { city: "" },
      born: { $date: "1970-01-01T00:00:00.000Z" },
    },
  );
});
