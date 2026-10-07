import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assert, assertEquals } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { refId } from "../src/ids.ts";
import type { Db } from "../src/mongodb.ts";
import type { Page } from "../src/page.ts";
import type { StoredDocument } from "../src/stored-document.ts";
import { type LogRecord, setLogSink } from "../src/utils/logger.ts";
import { withDatabase } from "./+shared.ts";

const SECRET = "PII_SENTINEL_SKIPPED_7f3a";
const SCOPE = "exposition:aaaaaaaaaaaaaaaaaaaaaaaaaa";

async function capture<T>(work: () => Promise<T>) {
  const records: LogRecord[] = [];
  setLogSink((record) => records.push(record));
  try {
    return { result: await work(), records };
  } finally {
    setLogSink(undefined);
  }
}

const kinds: {
  kind: string;
  name: string;
  page: (db: Db) => Promise<Page<{ name: unknown }>>;
}[] = [
  {
    kind: "collection",
    name: "people",
    page: async (db) => {
      const people = await collection(db, "people", { name: v.string() });
      await people.insertOne({ name: "Ada" });
      await people.collection.insertOne({ name: 42, note: SECRET } as never, {
        bypassDocumentValidation: true,
      });
      return people.paginate({});
    },
  },
  {
    kind: "multiCollection",
    name: "catalog",
    page: async (db) => {
      const catalog = await multiCollection(
        db,
        "catalog",
        { person: { name: v.string() } },
        { schemaManagement: "auto" },
      );
      await catalog.insertOne("person", { name: "Ada" });
      await db
        .collection<StoredDocument>("catalog")
        .insertOne(
          { _id: "person:invalid", _type: "person", name: 42, note: SECRET },
          { bypassDocumentValidation: true },
        );
      return catalog.paginate("person", {});
    },
  },
  {
    kind: "scopedMultiCollection",
    name: "+expositions",
    page: async (db) => {
      const scoped = await scopedMultiCollection(db, "+expositions", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: { person: { name: v.string() } },
      });
      const view = scoped.scope(SCOPE);
      await view.insertOne("person", { name: "Ada" });
      await db.collection<StoredDocument>("+expositions").insertOne(
        {
          _id: "person:invalid",
          _type: "person",
          _scope: SCOPE,
          name: 42,
          note: SECRET,
        },
        { bypassDocumentValidation: true },
      );
      return view.paginate("person", {});
    },
  },
];

for (const { kind, name, page } of kinds) {
  test(`paginate on ${kind}: an invalid stored document is left out, counted and reported`, async (t) => {
    await withDatabase(t.name, async (db) => {
      const { result, records } = await capture(() => page(db));

      assertEquals(
        result.data.map((row) => row.name),
        ["Ada"],
      );
      assertEquals(result.skippedInvalid, 1);

      const warning = records.find((record) =>
        record.message.includes("fail the schema"),
      );
      assert(warning !== undefined, "a warning reports the skipped document");
      assertEquals(warning.level, "warn");
      assert(
        warning.message.includes(name),
        "the warning names the collection",
      );
      assert(
        !JSON.stringify(records).includes(SECRET),
        "no document value reaches the log",
      );
    });
  });
}

test("paginate: a clean page carries no skippedInvalid and logs nothing", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", { name: v.string() });
    await people.insertOne({ name: "Ada" });
    const { result, records } = await capture(() => people.paginate({}));
    assertEquals("skippedInvalid" in result, false);
    assertEquals(
      records.filter((record) => record.message.includes("fail the schema")),
      [],
    );
  });
});
