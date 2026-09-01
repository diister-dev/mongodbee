import { assert, assertEquals, assertNotEquals } from "@std/assert";
import { decodeTime } from "@std/ulid";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import {
  createLiveTransformContext,
  migrationTime,
} from "../../src/migration/utils/transform-context.ts";
import { isUlid } from "../../src/utils/ulid-encode.ts";
import * as v from "../../src/schema.ts";

const SCHEMAS = {
  collections: {
    things: {
      _id: v.string(),
      name: v.string(),
      versionId: v.optional(v.string()),
      stampedAt: v.optional(v.date()),
    },
  },
};

const migration = migrationDefinition(
  "2026_08_04_1536_STAMP01@stamp",
  "stamp",
  {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b.collection("things").transform({
        up: (doc, ctx) => ({
          ...doc,
          versionId: ctx.newId(),
          stampedAt: ctx.now(),
        }),
        down: (doc) => {
          const { versionId: _v, stampedAt: _s, ...rest } = doc;
          return rest;
        },
      }).end().compile(),
  },
);

function seeded() {
  const state = createEmptyDatabaseState();
  state.collections.things = {
    content: [{ _id: "a", name: "one" }, { _id: "b", name: "two" }, {
      _id: "c",
      name: "three",
    }],
  };
  return state;
}

async function applied() {
  const operations =
    migration.migrate(migrationBuilder({ schemas: migration.schemas }))
      .operations;
  const applier = createMemoryApplier(migration);
  let state = seeded();
  for (const op of operations) state = await applier.applyOperation(state, op);
  return state.collections.things.content;
}

Deno.test("transform context: ids and clock inside a migration are deterministic in memory", async () => {
  const first = await applied();
  const second = await applied();
  assertEquals(first, second);
  const ids = first.map((d) => d.versionId as string);
  assertEquals(new Set(ids).size, 3);
  for (const id of ids) {
    assert(isUlid(id), id);
    assertEquals(id, id.toLowerCase());
  }
  assert(decodeTime(ids[0].toUpperCase()) < decodeTime(ids[2].toUpperCase()));
  const stamp = first[0].stampedAt as Date;
  assertEquals(stamp.getTime(), migrationTime(migration.id));
  assertEquals(stamp.toISOString(), "2026-08-04T15:36:00.000Z");
});

Deno.test("transform context: a transform that ignores the context still works", async () => {
  const plain = migrationDefinition("2026_08_05_0900_PLAIN01@plain", "plain", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b.collection("things").transform({
        up: (doc) => ({ ...doc, name: String(doc.name).toUpperCase() }),
        down: (doc) => ({ ...doc, name: String(doc.name).toLowerCase() }),
      }).end().compile(),
  });
  const operations =
    plain.migrate(migrationBuilder({ schemas: plain.schemas })).operations;
  const applier = createMemoryApplier(plain);
  let state = seeded();
  for (const op of operations) state = await applier.applyOperation(state, op);
  assertEquals(state.collections.things.content.map((d) => d.name), [
    "ONE",
    "TWO",
    "THREE",
  ]);
});

Deno.test("transform context: the live context mints fresh ulids and reads the wall clock", () => {
  const ctx = createLiveTransformContext("live");
  const a = ctx.newId();
  const b = ctx.newId();
  assert(isUlid(a) && isUlid(b));
  assertNotEquals(a, b);
  assert(Math.abs(ctx.now().getTime() - Date.now()) < 1000);
  assertEquals(migrationTime("no-date-here"), Date.UTC(2000, 0, 1));
});
