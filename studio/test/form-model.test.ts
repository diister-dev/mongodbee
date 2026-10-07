import { test } from "../../library/test/+harness.ts";
import { assertEquals } from "../../library/test/+assert.ts";
import {
  checkHint,
  type FormNode,
  defaultFor,
  getAt,
  issuesAt,
  issuesUnder,
  numberFromText,
  removeAt,
  requiredDefaults,
  setAt,
  switchVariant,
  variantOption,
  variantTags,
} from "../src/ui/lib/form-model.ts";

test("setAt, getAt and removeAt work on nested objects and arrays without mutating", () => {
  const doc = { name: "Ada", address: { city: "London" }, tags: ["a", "b"] };
  const next = setAt(doc, ["address", "zip"], "N1") as typeof doc & {
    address: { zip: string };
  };
  assertEquals(next.address, { city: "London", zip: "N1" });
  assertEquals(doc.address, { city: "London" });
  assertEquals(getAt(setAt(doc, ["tags", 1], "c"), ["tags"]), ["a", "c"]);
  assertEquals(getAt(removeAt(doc, ["tags", 0]), ["tags"]), ["b"]);
  assertEquals(removeAt(doc, ["name"]), {
    address: { city: "London" },
    tags: ["a", "b"],
  });
  assertEquals(getAt(doc, ["address", "city"]), "London");
  assertEquals(getAt(doc, ["missing", "deep"]), undefined);
});

test("defaults follow the schema: required fields only, first choice, empty containers", () => {
  const entries = {
    name: { kind: "string" },
    nick: { kind: "string", optional: true },
    role: { kind: "picklist", values: ["admin", "viewer"] },
    manager: { kind: "string", ref: "user", nullable: true },
    address: { kind: "object", entries: { city: { kind: "string" } } },
    tags: { kind: "array", item: { kind: "string" } },
    active: { kind: "boolean", hasDefault: true },
  };
  assertEquals(requiredDefaults(entries), {
    name: "",
    role: "admin",
    manager: null,
    address: { city: "" },
    tags: [],
  });
  assertEquals(defaultFor({ kind: "number" }), 0);
});

test("variants switch option, keep shared fields and set the discriminator", () => {
  const node: FormNode = {
    kind: "variant",
    discriminator: "type",
    options: [
      {
        kind: "object",
        entries: {
          type: { kind: "literal", literal: "free" },
          note: { kind: "string" },
        },
      },
      {
        kind: "object",
        entries: {
          type: { kind: "literal", literal: "guided" },
          note: { kind: "string" },
          guide: { kind: "string" },
        },
      },
    ],
  };
  assertEquals(variantTags(node), ["free", "guided"]);
  assertEquals(
    variantOption(node, { type: "guided" })?.entries?.guide?.kind,
    "string",
  );
  assertEquals(switchVariant(node, { type: "free", note: "keep" }, "guided"), {
    type: "guided",
    note: "keep",
    guide: "",
  });
});

test("issues are matched to their field and counted under a group", () => {
  const issues = [
    { path: "address.city", message: "Invalid type" },
    { path: "address", message: "Missing" },
    { path: "tags.0", message: "Too short" },
  ];
  assertEquals(issuesAt(issues, ["address", "city"]), ["Invalid type"]);
  assertEquals(issuesUnder(issues, ["address"]), 2);
  assertEquals(issuesAt(issues, ["tags", 0]), ["Too short"]);
});

test("numbers and hints read like a person would write them", () => {
  assertEquals(numberFromText(" 3,5 "), 3.5);
  assertEquals(numberFromText("abc"), undefined);
  assertEquals(
    checkHint({
      kind: "string",
      checks: [{ type: "min_length", requirement: 3 }, { type: "email" }],
    }),
    "at least 3 characters, an email address",
  );
});

test("a one-character minimum reads in the singular", () => {
  assertEquals(
    checkHint({
      kind: "string",
      checks: [{ type: "min_length", requirement: 1 }],
    }),
    "at least 1 character",
  );
});
