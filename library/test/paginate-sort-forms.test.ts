// Verrou — every `m.Sort` form the paginate option's type admits must either
// paginate in the requested order or fail loud. Two silent misreads guarded
// here (both measured on the parent commit):
//
// - `sort: "name"` (legal m.Sort meaning `{name: 1}`) fell through to the
//   "direction" branch and paginated by `{_id: -1}` — the requested order
//   ignored, no error anywhere.
// - `sort: {name: "asc"}` normalized the $sort but the ladder compared the
//   direction with `=== 1`, so page 2 walked BACKWARD and dead-ended: a
//   5-doc walk at limit 2 returned `alpha, bravo, alpha` — 3 docs lost, one
//   duplicated.
//
// Ground truth is the server's own $sort — never a hand-written order.

import { assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const NAMES = ["delta", "alpha", "charlie", "bravo", "echo"];

// deno-lint-ignore no-explicit-any
async function truth(db: any, sort: Record<string, 1 | -1>) {
  const rows = await db.collection("items").aggregate([{ $sort: sort }])
    .toArray();
  return (rows as { name: string }[]).map((r) => r.name);
}

/** Full cursor walk (limit 2) — the misreads only bite from page 2. */
// deno-lint-ignore no-explicit-any
async function walk(items: any, sort: unknown): Promise<string[]> {
  const seen: string[] = [];
  let afterId: string | undefined;
  for (let guard = 0; guard < 10; guard++) {
    const p = await items.paginate({}, {
      sort,
      limit: 2,
      ...(afterId ? { afterId } : {}),
    });
    if (p.data.length === 0) break;
    for (const d of p.data) seen.push(d.name);
    if (p.data.length < 2) break;
    afterId = p.data[p.data.length - 1]._id;
  }
  return seen;
}

Deno.test("paginate sort forms: every m.Sort shape orders as the driver would", async () => {
  await withDatabase("paginate-sort-forms", async (db) => {
    const items = await collection(db, "items", {
      name: v.string(),
      rank: v.number(),
    });
    for (let i = 0; i < NAMES.length; i++) {
      await items.insertOne({ name: NAMES[i], rank: i % 2 });
    }

    const byNameAsc = await truth(db, { name: 1, _id: 1 });
    const byNameDesc = await truth(db, { name: -1, _id: -1 });
    const byRankThenName = await truth(db, { rank: 1, name: -1, _id: -1 });

    // String field name — driver semantics: ascending.
    assertEquals(await walk(items, "name"), byNameAsc, "sort: 'name'");
    // Direction strings inside an object.
    assertEquals(
      await walk(items, { name: "asc" }),
      byNameAsc,
      "sort: {name:'asc'}",
    );
    assertEquals(
      await walk(items, { name: "desc" }),
      byNameDesc,
      "sort: {name:'desc'}",
    );
    // Array of field names.
    assertEquals(await walk(items, ["name"]), byNameAsc, "sort: ['name']");
    // Single [field, direction] pair.
    assertEquals(
      await walk(items, ["name", "desc"]),
      byNameDesc,
      "sort: ['name','desc']",
    );
    // Array of pairs, mixed directions.
    assertEquals(
      await walk(items, [["rank", 1], ["name", -1]]),
      byRankThenName,
      "sort: [['rank',1],['name',-1]]",
    );
    // Map form.
    assertEquals(
      await walk(
        items,
        new Map<string, 1 | -1>([["rank", 1], ["name", -1]]),
      ),
      byRankThenName,
      "sort: Map",
    );

    // Unladderable / malformed forms fail LOUD, not wrong.
    await assertRejects(
      () => items.paginate({}, { sort: { score: { $meta: "textScore" } } }),
      Error,
      "$meta",
    );
    await assertRejects(
      // deno-lint-ignore no-explicit-any
      () => items.paginate({}, { sort: { name: 2 } as any }),
      Error,
      "invalid sort direction",
    );
    await assertRejects(
      // deno-lint-ignore no-explicit-any
      () => items.paginate({}, { sort: [42] as any }),
      Error,
      "invalid sort entry",
    );
  });
});

Deno.test("paginate sort forms: shared normalization reaches the scoped surface", async () => {
  await withDatabase("paginate-sort-forms-scoped", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { item: { name: v.string() } },
    });
    const view = catalog.scope("exposition:expoaaaaa01");
    for (const name of NAMES) await view.insertOne("item", { name });

    const rows = await db.collection("catalog").aggregate([
      { $sort: { name: 1, _id: 1 } },
    ]).toArray();
    const expected = (rows as { name: string }[]).map((r) => r.name);

    const seen: string[] = [];
    let afterId: string | undefined;
    for (let guard = 0; guard < 10; guard++) {
      const p = await view.paginate("item", undefined, {
        // deno-lint-ignore no-explicit-any
        sort: { name: "asc" } as any,
        limit: 2,
        ...(afterId ? { afterId } : {}),
      });
      if (p.data.length === 0) break;
      for (const d of p.data) seen.push((d as { name: string }).name);
      if (p.data.length < 2) break;
      afterId = (p.data[p.data.length - 1] as { _id: string })._id;
    }
    assertEquals(seen, expected, "scoped {name:'asc'} walk");
  });
});
