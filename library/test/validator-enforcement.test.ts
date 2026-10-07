import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { refId } from "../src/ids.ts";
import type { Db } from "../src/mongodb.ts";
import type { StoredDocument } from "../src/stored-document.ts";
import { withDatabase } from "./+shared.ts";

async function disableLikeARollback(db: Db, name: string): Promise<void> {
  await db.command({ collMod: name, validator: {}, validationLevel: "off" });
}

async function validationOf(
  db: Db,
  name: string,
): Promise<{ level: unknown; action: unknown }> {
  const [info] = await db.listCollections({ name }).toArray();
  const options = info && "options" in info ? info.options : undefined;
  return {
    level: options?.validationLevel ?? "strict",
    action: options?.validationAction ?? "error",
  };
}

const opened = [
  {
    kind: "collection",
    name: "users",
    open: (db: Db) =>
      collection(
        db,
        "users",
        { name: v.string() },
        { schemaManagement: "auto" },
      ),
  },
  {
    kind: "multiCollection",
    name: "catalog",
    open: (db: Db) =>
      multiCollection(
        db,
        "catalog",
        { product: { name: v.string() } },
        { schemaManagement: "auto" },
      ),
  },
  {
    kind: "scopedMultiCollection",
    name: "+expositions",
    open: (db: Db) =>
      scopedMultiCollection(db, "+expositions", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: { participant: { name: v.string() } },
      }),
  },
] as const;

for (const { kind, name, open } of opened) {
  test(`Validator enforcement: ${kind} restores strict validation after a rollback disabled it`, async (t) => {
    await withDatabase(t.name, async (db) => {
      await open(db);
      await disableLikeARollback(db, name);
      assertEquals((await validationOf(db, name)).level, "off");

      await open(db);

      assertEquals(await validationOf(db, name), {
        level: "strict",
        action: "error",
      });
      await assertRejects(() =>
        db.collection<StoredDocument>(name).insertOne({ _id: "x", name: 42 }),
      );
    });
  });
}
