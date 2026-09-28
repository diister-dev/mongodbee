import { test } from "../../library/test/+harness.ts";
import { assert, assertEquals } from "../../library/test/+assert.ts";
import {
  createWarmState,
  groupOperations,
  keyToRowAction,
  matchScore,
  moveSelection,
  rankOptions,
} from "../src/ui/lib/interaction.ts";
import {
  cubicBezier,
  EASE_DRAWER,
  EASE_ENTER,
  EASE_EXIT,
  EASE_IN_OUT,
  fade,
  isQuiet,
  quiet,
  rise,
} from "../src/ui/lib/motion.ts";
import { placeUnder } from "../src/ui/lib/anchor.ts";
import { splitWarnings } from "../src/ui/lib/check-warnings.ts";

test("warm state: shared window after a card closes", () => {
  const warm = createWarmState(400);
  assertEquals(warm.isWarm(0), false);
  warm.opened();
  assertEquals(warm.isWarm(10), true);
  warm.closed(1000);
  assertEquals(warm.isWarm(1200), true);
  assertEquals(warm.isWarm(1400), true);
  assertEquals(warm.isWarm(1401), false);
  warm.opened();
  warm.opened();
  warm.closed(2000);
  assertEquals(warm.isWarm(5000), true, "still one card open");
  warm.closed(5000);
  assertEquals(warm.isWarm(5500), false);
});

test("row selection reducer", () => {
  assertEquals(moveSelection(null, 0, { type: "next" }), null);
  assertEquals(moveSelection(null, 5, { type: "next" }), 0);
  assertEquals(moveSelection(null, 5, { type: "previous" }), 4);
  assertEquals(moveSelection(3, 5, { type: "next" }), 4);
  assertEquals(moveSelection(4, 5, { type: "next" }), 4);
  assertEquals(moveSelection(0, 5, { type: "previous" }), 0);
  assertEquals(moveSelection(2, 5, { type: "last" }), 4);
  assertEquals(moveSelection(2, 5, { type: "first" }), 0);
  assertEquals(moveSelection(2, 5, { type: "clear" }), null);
  assertEquals(moveSelection(2, 5, { type: "set", index: 9 }), 4);
  assertEquals(keyToRowAction("j"), { type: "next" });
  assertEquals(keyToRowAction("ArrowUp"), { type: "previous" });
  assertEquals(keyToRowAction("x"), null);
});

test("operation stacks group consecutive operations of one kind", () => {
  const ops = [
    { type: "create_collection" },
    { type: "seed_collection" },
    { type: "create_multicollection" },
    { type: "seed_multicollection_type" },
    { type: "seed_multicollection_type" },
    { type: "create_multimodel_instance" },
    { type: "create_scoped_multicollection" },
    { type: "seed_scoped_multicollection_type" },
    { type: "seed_scoped_multicollection_type" },
    { type: "transform_collection" },
  ];
  const groups = groupOperations(ops);
  assertEquals(
    groups.map((g) => g.label),
    [
      "Created 1 collection",
      "Seeded 1 target",
      "Created 1 collection",
      "Seeded 2 targets",
      "Created 2 collections",
      "Seeded 2 targets",
      "Transformed 1 target",
    ],
  );
  assertEquals(groups[4].items.length, 2);
  assertEquals(groupOperations([]), []);
});

test("palette match score ranks prefix over substring over subsequence", () => {
  const prefix = matchScore("art", "artwork");
  const inner = matchScore("work", "artwork");
  const loose = matchScore("awk", "artwork");
  assert(prefix > inner && inner > loose && loose > 0);
  assertEquals(matchScore("zzz", "artwork"), 0);
  assertEquals(matchScore("", "anything"), 1);
});

test("motion easings are monotonic, bounded and never overshoot", () => {
  for (const ease of [
    EASE_ENTER,
    EASE_EXIT,
    EASE_IN_OUT,
    EASE_DRAWER,
    cubicBezier(0.25, 0.1, 0.25, 1),
  ]) {
    let previous = 0;
    for (let i = 0; i <= 100; i++) {
      const value = ease(i / 100);
      assert(value >= previous - 1e-9, "monotonic");
      assert(value >= 0 && value <= 1, "bounded");
      previous = value;
    }
    assertEquals(ease(0), 0);
    assertEquals(ease(1), 1);
  }
  assert(EASE_ENTER(0.3) > 0.5, "enter decelerates");
  assert(EASE_EXIT(0.3) > 0.5, "exit decelerates too, never ease-in");
  assert(
    EASE_IN_OUT(0.2) < 0.2 && EASE_IN_OUT(0.8) > 0.8,
    "in-out is symmetric",
  );
  assert(EASE_DRAWER(0.3) > 0.5, "drawer decelerates");
});

test("rankOptions keeps groups together, best match group first", () => {
  const options = [
    { label: "name", group: "Text" },
    { label: "age", group: "Numbers" },
    { label: "email", group: "Text" },
    { label: "createdAt", group: "Dates" },
    { label: "managerId", group: "References" },
  ];
  assertEquals(
    rankOptions(options, "").map((o) => o.label),
    ["name", "email", "age", "createdAt", "managerId"],
  );
  assertEquals(
    rankOptions(options, "a").map((o) => o.label),
    ["age", "name", "email", "managerId", "createdAt"],
  );
  assertEquals(
    rankOptions(options, "ema").map((o) => o.label),
    ["email"],
  );
  assertEquals(rankOptions(options, "zzz"), []);
});

test("quiet window makes keyboard-driven transitions instant", () => {
  const node = {} as Element;
  quiet(-1);
  assertEquals(isQuiet(), false);
  assertEquals(fade(node).duration, 150);
  quiet(10_000);
  assertEquals(isQuiet(), true);
  assertEquals(fade(node).duration, 0);
  assertEquals(rise(node, { exit: true }).duration, 0);
  quiet(-1);
  assertEquals(rise(node).duration, 150);
});

test("placeUnder opens below, flips above near the bottom and stays on screen", () => {
  const viewport = { width: 1000, height: 800 };
  const below = placeUnder(
    { left: 100, top: 100, bottom: 130, width: 120 },
    viewport,
    260,
  );
  assertEquals(below, {
    left: 100,
    top: 136,
    bottom: 706,
    above: false,
    minWidth: 160,
  });
  const flipped = placeUnder(
    { left: 100, top: 700, bottom: 730, width: 200 },
    viewport,
    260,
  );
  assertEquals(flipped.above, true);
  assertEquals(flipped.bottom, 106);
  const clamped = placeUnder(
    { left: 950, top: 100, bottom: 130, width: 40 },
    viewport,
    100,
  );
  assertEquals(clamped.left, 1000 - 160 - 8);
});

test("splitWarnings dedupes and lifts warnings common to every migration", () => {
  const a = "Mock identity correlation: exposition";
  const b = "Mock identity correlation: artist";
  const del = "Operation 1 (delete_documents) leaves references";
  const rows = [
    { id: "1", status: "valid", warnings: [a, b, a, b] },
    { id: "2", status: "valid", warnings: [a, b, a, b, del] },
    { id: "3", status: "failed", warnings: [b, a] },
    { id: "4", status: "skipped" },
  ];
  const split = splitWarnings(rows);
  assertEquals(split.shared, [
    { text: a, occurrences: 5 },
    { text: b, occurrences: 5 },
  ]);
  assertEquals(split.specific, { "1": [], "2": [del], "3": [], "4": [] });
  const running = splitWarnings(rows, false);
  assertEquals(running.shared, []);
  assertEquals(running.specific["1"], [a, b]);
  assertEquals(splitWarnings([rows[1]]).specific["2"], [a, b, del]);
});
