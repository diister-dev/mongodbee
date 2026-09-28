import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { refId } from "../src/ids.ts";
import type { Db } from "../src/mongodb.ts";
import type { StoredDocument } from "../src/stored-document.ts";
import { DocumentValidationError } from "../src/validation-error.ts";
import { type LogRecord, setLogSink } from "../src/utils/logger.ts";
import { withDatabase } from "./+shared.ts";

const SCOPE = "exposition:aaaaaaaaaaaaaaaaaaaaaaaaaa";

type Reader = {
  getById: (id: string) => Promise<unknown>;
  find: () => Promise<{ name: unknown }[]>;
};

const kinds: {
  kind: string;
  open: (db: Db) => Promise<{ reader: Reader; validId: string }>;
}[] = [
  {
    kind: "multiCollection",
    open: async (db) => {
      const catalog = await multiCollection(
        db,
        "catalog",
        { person: { name: v.string() } },
        { schemaManagement: "auto" },
      );
      const validId = await catalog.insertOne("person", { name: "Ada" });
      await db
        .collection<StoredDocument>("catalog")
        .insertOne(
          { _id: "person:invalid", _type: "person", name: 42 },
          { bypassDocumentValidation: true },
        );
      return {
        validId,
        reader: {
          getById: (id) => catalog.getById("person", id),
          find: () => catalog.find("person", {}),
        },
      };
    },
  },
  {
    kind: "scopedMultiCollection",
    open: async (db) => {
      const scoped = await scopedMultiCollection(db, "+expositions", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: { person: { name: v.string() } },
      });
      const view = scoped.scope(SCOPE);
      const validId = await view.insertOne("person", { name: "Ada" });
      await db
        .collection<StoredDocument>("+expositions")
        .insertOne(
          { _id: "person:invalid", _type: "person", _scope: SCOPE, name: 42 },
          { bypassDocumentValidation: true },
        );
      return {
        validId,
        reader: {
          getById: (id) => view.getById("person", id),
          find: () => view.find("person", {}),
        },
      };
    },
  },
];

for (const { kind, open } of kinds) {
  test(`${kind}: a single read of an invalid stored document throws DocumentValidationError`, async (t) => {
    await withDatabase(t.name, async (db) => {
      const { reader, validId } = await open(db);
      assertEquals(
        ((await reader.getById(validId)) as { name: string }).name,
        "Ada",
      );
      const failure = await assertRejects(() =>
        reader.getById("person:invalid"),
      );
      assert(failure instanceof DocumentValidationError);
    });
  });

  test(`${kind}: find leaves an invalid stored document out and reports it`, async (t) => {
    await withDatabase(t.name, async (db) => {
      const { reader } = await open(db);
      const records: LogRecord[] = [];
      setLogSink((record) => records.push(record));
      let rows: { name: unknown }[];
      try {
        rows = await reader.find();
      } finally {
        setLogSink(undefined);
      }
      assertEquals(
        rows.map((row) => row.name),
        ["Ada"],
      );
      assert(
        records.some(
          (record) =>
            record.level === "warn" &&
            record.message.startsWith("find(") &&
            record.message.includes("skipped 1"),
        ),
        "the skipped document is reported",
      );
    });
  });
}
