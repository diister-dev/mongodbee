import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { indexMetadataOf, INDEX_SYMBOL, withIndex } from "../../src/indexes.ts";
import { fieldsOf, isTypeDefinition } from "../../src/type-definition.ts";
import { schemaToNode } from "../../src/studio/schema-tree.ts";

test("indexMetadataOf reads this copy's symbol and another copy's local symbol", () => {
  const foreign = Symbol("mongodbee.index");
  assertEquals(indexMetadataOf({ [INDEX_SYMBOL]: { unique: true } }), {
    unique: true,
  });
  assertEquals(indexMetadataOf({ [foreign]: { unique: true, global: true } }), {
    unique: true,
    global: true,
  });
  assertEquals(
    indexMetadataOf({ [Symbol("other")]: { unique: true } }),
    undefined,
  );
  assertEquals(indexMetadataOf(undefined), undefined);
  assertEquals(INDEX_SYMBOL, Symbol.for("mongodbee.index"));
});

test("the studio sees withIndex metadata written by another mongodbee copy", () => {
  const foreign = Symbol("mongodbee.index");
  const email = v.pipe(v.string(), {
    kind: "metadata",
    type: "metadata",
    reference: v.metadata,
    metadata: { [foreign]: { unique: true } },
  } as never);
  assertEquals(schemaToNode(email).index, { unique: true });
  assertEquals(schemaToNode(withIndex(v.string(), { unique: true })).index, {
    unique: true,
  });
});

test("type definitions from another copy are detected through the shared brand", () => {
  const schema = v.object({ name: v.string() });
  const foreign = Object.defineProperty(
    { schema, indexes: [] },
    Symbol.for("mongodbee.type-definition"),
    { value: true, enumerable: false },
  );
  assertEquals(isTypeDefinition(foreign), true);
  assertEquals(Object.keys(fieldsOf(foreign as never)), ["name"]);
});
