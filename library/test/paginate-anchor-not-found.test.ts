// Verrou — anchor-not-found behavior DIVERGES by surface, deliberately.
//
// This is a pin, not an endorsement. On the index-strategy path:
// - `collection` and `multiCollection` silently RESTART: a ghost `afterId`
//   returns page 1 with `position: 1`; a ghost `beforeId` returns the LAST
//   page with `position: 0`. The pinned consumer (0.23.0-beta.11 behavior)
//   relies on the restart, so it stays.
// - `scopedMultiCollection` throws, naming the id and scope.
// On the sortPipeline path ALL THREE surfaces throw — so on collection and
// multi the SAME ghost id restarts or throws depending on which sort option
// was used. Anyone changing any of these contracts must come through here.
//
// If the silent restart is ever retired, this file is the list of behaviors
// the consumer must migrate off first.

import { assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";

Deno.test("anchor not found: collection silently restarts (afterId → page 1, beforeId → last page)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const items = await collection(db, "items", {
      _id: dbId("item"),
      n: v.number(),
    });
    for (let i = 0; i < 7; i++) await items.insertOne({ n: i });
    const all = await db.collection("items").find().sort({ _id: 1 })
      .toArray();

    // Ghost afterId → page 1 again, position pinned at its historical `1`.
    // deno-lint-ignore no-explicit-any
    const fwd: any = await items.paginate({}, {
      limit: 3,
      sort: { _id: 1 },
      afterId: "item:00000000000000000000000000",
    });
    assertEquals(
      fwd.data.map((d: { _id: string }) => String(d._id)),
      all.slice(0, 3).map((d) => String(d._id)),
      "ghost afterId restarts at page 1",
    );
    assertEquals(fwd.total, 7);
    assertEquals(fwd.position, 1, "historical not-found marker");

    // Ghost beforeId → the walk reverses over the WHOLE set: last page,
    // position 0 (wrong-but-historical — position describes page 1).
    // deno-lint-ignore no-explicit-any
    const back: any = await items.paginate({}, {
      limit: 3,
      sort: { _id: 1 },
      beforeId: "item:00000000000000000000000000",
    });
    assertEquals(
      back.data.map((d: { _id: string }) => String(d._id)),
      all.slice(4).map((d) => String(d._id)),
      "ghost beforeId returns the last page",
    );
    assertEquals(back.position, 0, "historical not-found marker");

    // Same ghost id on the sortPipeline path: THROWS. The asymmetry is the
    // contract.
    await assertRejects(
      () =>
        items.paginate({}, {
          limit: 3,
          sort: { n: 1 } as never,
          afterId: "item:00000000000000000000000000",
          // deno-lint-ignore no-explicit-any
          sortPipeline: (s: any) => [s.addFields({ n2: "$n" })],
          // deno-lint-ignore no-explicit-any
        } as any),
      Error,
      "was not found",
    );
  });
});

Deno.test("anchor not found: multiCollection silently restarts (valid prefix, ghost id)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await multiCollection(db, "people", {
      person: { n: v.number() },
    });
    for (let i = 0; i < 5; i++) await people.insertOne("person", { n: i });
    const all = await db.collection("people").find().sort({ _id: 1 })
      .toArray();

    // deno-lint-ignore no-explicit-any
    const fwd: any = await people.paginate("person", {}, {
      limit: 2,
      sort: { _id: 1 },
      afterId: "person:00000000000000000000000000",
    });
    assertEquals(
      fwd.data.map((d: { _id: string }) => String(d._id)),
      all.slice(0, 2).map((d) => String(d._id)),
      "ghost afterId restarts at page 1",
    );
    assertEquals(fwd.position, 1, "historical not-found marker");

    // sortPipeline path throws for the same ghost id.
    await assertRejects(
      () =>
        people.paginate("person", {}, {
          limit: 2,
          sort: { n: 1 } as never,
          afterId: "person:00000000000000000000000000",
          // deno-lint-ignore no-explicit-any
          sortPipeline: (s: any) => [s.addFields({ n2: "$n" })],
          // deno-lint-ignore no-explicit-any
        } as any),
      Error,
      "was not found",
    );
  });
});

Deno.test("anchor not found: scoped throws — the one surface that fails loud", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { item: { n: v.number() } },
    });
    const view = catalog.scope("exposition:expoaaaaa01");
    for (let i = 0; i < 3; i++) await view.insertOne("item", { n: i });

    await assertRejects(
      () =>
        view.paginate("item", undefined, {
          limit: 2,
          afterId: "item:00000000000000000000000000",
          // deno-lint-ignore no-explicit-any
        } as any),
      Error,
      "was not found as type",
    );
    await assertRejects(
      () =>
        view.paginate("item", undefined, {
          limit: 2,
          beforeId: "item:00000000000000000000000000",
          // deno-lint-ignore no-explicit-any
        } as any),
      Error,
      "was not found as type",
    );
  });
});
