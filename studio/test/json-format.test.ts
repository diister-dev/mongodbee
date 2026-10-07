import { test } from "../../library/test/+harness.ts";
import { assertEquals } from "../../library/test/+assert.ts";
import { compactJson, tokenizeJson } from "../src/ui/lib/json-format.ts";

test("compactJson keeps short containers on one line and breaks long ones", () => {
  const doc = {
    _id: "user:01",
    identity: { name: "Ada", nature: { kind: "person" } },
    tags: [],
    createdAt: { $date: "2026-08-09T20:36:18.033Z" },
    notes: ["x".repeat(50), "y".repeat(50)],
  };
  assertEquals(
    compactJson(doc),
    [
      "{",
      '  "_id": "user:01",',
      '  "identity": { "name": "Ada", "nature": { "kind": "person" } },',
      '  "tags": [],',
      '  "createdAt": { "$date": "2026-08-09T20:36:18.033Z" },',
      '  "notes": [',
      `    "${"x".repeat(50)}",`,
      `    "${"y".repeat(50)}"`,
      "  ]",
      "}",
    ].join("\n"),
  );
  assertEquals(compactJson([1, 2]), "[1, 2]");
  assertEquals(compactJson(null), "null");
  assertEquals(compactJson({}), "{}");
  assertEquals(JSON.parse(compactJson(doc)), doc);
});

test("tokenizeJson separates keys, strings, numbers, literals and punctuation", () => {
  const tokens = tokenizeJson(
    '{ "a": "b:c", "n": -1.5e3, "t": true, "z": null }',
  );
  assertEquals(
    tokens.filter((token) => token.kind !== "space" && token.kind !== "punct"),
    [
      { kind: "key", text: '"a"' },
      { kind: "string", text: '"b:c"' },
      { kind: "key", text: '"n"' },
      { kind: "number", text: "-1.5e3" },
      { kind: "key", text: '"t"' },
      { kind: "literal", text: "true" },
      { kind: "key", text: '"z"' },
      { kind: "literal", text: "null" },
    ],
  );
  assertEquals(
    tokens.map((token) => token.text).join(""),
    '{ "a": "b:c", "n": -1.5e3, "t": true, "z": null }',
  );
});
