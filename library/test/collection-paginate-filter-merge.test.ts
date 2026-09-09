// Verrou — the cursor filter must be AND-composed with the user filter, never
// object-spread over it.
//
// Regression it guards: `collection.paginate` merged the page-2+ cursor with
// `{ ...filter, ...cursorFilter }`. The cursor is `{$or: [...rungs]}` (or
// `{_id: {...}}` for the `_id` fast path), so a user filter carrying its own
// `$or` — every text-search filter — or its own `_id`/sort-field constraint
// was silently REPLACED by the cursor from the second page on: the page
// leaked documents the filter excludes, and `position` (computed with the
// same spread) went inconsistent. multiCollection and scopedMultiCollection
// already composed with `$and`; collection was the diverging copy.

import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import * as v from "../src/schema.ts";
import { dbId } from "../src/ids.ts";

test("paginate (collection): a user filter with $or survives the page-2 cursor", async () => {
  await withDatabase("paginate-filter-or-merge", async (db) => {
    const people = await collection(db, "people", {
      _id: dbId("person"),
      name: v.string(),
      group: v.string(),
    });

    const wanted: string[] = [];
    for (let i = 0; i < 6; i++) {
      wanted.push(
        (await people.insertOne({
          name: `a-${i}`,
          group: "a",
        } as never)) as string,
      );
      wanted.push(
        (await people.insertOne({
          name: `b-${i}`,
          group: "b",
        } as never)) as string,
      );
      // Excluded by the filter — must never appear on any page.
      await people.insertOne({ name: `c-${i}`, group: "c" } as never);
    }

    const filter = { $or: [{ group: "a" }, { group: "b" }] };
    const seen: string[] = [];
    let afterId: string | undefined;
    for (let guard = 0; guard < 20; guard++) {
      const page = await people.paginate(filter as never, {
        sort: { name: 1, _id: 1 },
        limit: 5,
        ...(afterId ? { afterId } : {}),
      });
      assertEquals(page.total, wanted.length, "total must match the filter");
      assert(
        (page.position ?? 0) >= 0,
        `position went negative (${page.position}) — the cursor $or ` +
          `replaced the filter $or in the position count`,
      );
      for (const doc of page.data) {
        const d = doc as unknown as { _id: string; group: string };
        assert(
          d.group !== "c",
          `page leaked ${d._id} (group "c") — the cursor $or replaced ` +
            `the user filter's $or`,
        );
        seen.push(d._id);
      }
      if (page.data.length === 0) break;
      afterId = (page.data[page.data.length - 1] as unknown as { _id: string })
        ._id;
      if ((page.position ?? 0) + page.data.length >= (page.total ?? 0)) break;
    }
    assertEquals(new Set(seen).size, wanted.length, "walked exactly the set");
  });
});

test("paginate (collection): a user filter on _id survives the _id fast-path cursor", async () => {
  await withDatabase("paginate-filter-id-merge", async (db) => {
    const people = await collection(db, "people", {
      _id: dbId("person"),
      name: v.string(),
    });

    const all: string[] = [];
    for (let i = 0; i < 10; i++) {
      all.push((await people.insertOne({ name: `p-${i}` } as never)) as string);
    }
    all.sort();
    // Filter to a strict subset by _id: the page-2 cursor {_id: {$gt}} used
    // to overwrite this constraint and walk past the subset's end.
    const subset = all.slice(0, 6);
    const filter = { _id: { $in: subset } };

    const page1 = await people.paginate(filter as never, {
      sort: { _id: 1 },
      limit: 4,
    });
    assertEquals(page1.total, subset.length);
    const lastId = (
      page1.data[page1.data.length - 1] as unknown as {
        _id: string;
      }
    )._id;
    const page2 = await people.paginate(filter as never, {
      sort: { _id: 1 },
      limit: 4,
      afterId: lastId,
    });
    const ids2 = page2.data.map((d) => (d as unknown as { _id: string })._id);
    assertEquals(
      ids2,
      subset.slice(4),
      "page 2 must stay inside the $in subset — the cursor overwrote _id",
    );
  });
});
