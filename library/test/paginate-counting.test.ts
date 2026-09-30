import { test } from "./+harness.ts";
import { assertEquals } from "./+assert.ts";
import { countingPipeline, pageBatchSize } from "../src/paginate-sort.ts";

const match = { $match: { a: 1 } };
const lookup = {
  $lookup: { from: "x", localField: "a", foreignField: "_id", as: "x" },
};

test("countingPipeline: trailing stages that keep every row are left out", () => {
  assertEquals(
    countingPipeline([
      match,
      lookup,
      { $unset: "x" },
      { $project: { a: 1, b: true } },
    ]),
    [match, { $count: "total" }],
  );
});

test("countingPipeline: a stage that can drop, add or fail on a row stays, with all before it", () => {
  const unwind = { $unwind: "$x" };
  assertEquals(countingPipeline([match, lookup, unwind]), [
    match,
    lookup,
    unwind,
    { $count: "total" },
  ]);
  const computed = { $project: { total: { $toInt: "$a" } } };
  assertEquals(countingPipeline([match, lookup, computed]), [
    match,
    lookup,
    computed,
    { $count: "total" },
  ]);
  const set = { $set: { n: { $size: "$x" } } };
  assertEquals(countingPipeline([lookup, set, lookup]), [
    lookup,
    set,
    { $count: "total" },
  ]);
  const filtering = { $match: { x: { $ne: [] } } };
  assertEquals(countingPipeline([lookup, filtering, lookup]), [
    lookup,
    filtering,
    { $count: "total" },
  ]);
  const twoOperators = { $lookup: lookup.$lookup, $match: {} };
  assertEquals(countingPipeline([twoOperators]), [
    twoOperators,
    { $count: "total" },
  ]);
});

test("pageBatchSize: a page batch only without a JS filter", () => {
  assertEquals(pageBatchSize(26, undefined), { batchSize: 26 });
  assertEquals(
    pageBatchSize(26, () => true),
    {},
  );
  assertEquals(pageBatchSize(0, undefined), {});
});
