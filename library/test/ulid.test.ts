/**
 * ULID generation and timestamp decoding.
 *
 * `decodeTime` exists because a consumer already depending on MongoDBee for
 * ids should not need a second ULID implementation in its process just to read
 * a timestamp back. Its contract is deliberately a superset of
 * `@std/ulid`'s — the vectors below were produced by that package, so anything
 * it accepts is accepted here identically, with lowercase as the one addition
 * (`newId()` lowercases, and rejecting that would make this unable to read the
 * ids the same module hands out).
 *
 * @module
 */

import { test } from "./+harness.ts";
import { assert, assertEquals, assertThrows } from "./+assert.ts";
import { decodeTime, newId, ulid } from "../src/ids.ts";

/**
 * Generated with `jsr:@std/ulid@1.0.0`, the implementation this one replaced:
 * `ids.map(decodeTime)` there produced exactly these timestamps.
 */
const STD_VECTORS: ReadonlyArray<readonly [string, number]> = [
  // Five ids drawn in the same millisecond — they differ only in the random
  // half, which `decodeTime` never reads, so on their own they are one vector.
  ["01M2626XKER9R3CBJCP71EBXH8", 1789057529454],
  ["01M2626XKEP8SC6702HYFXV91E", 1789057529454],
  ["01M2626XKEN90VBBHTWVWD7XWQ", 1789057529454],
  ["01M2626XKEA9CPBQ9VHQ60R6CG", 1789057529454],
  ["01M2626XKEZ2R5YN9RK6PQ9ZKW", 1789057529454],
  // …so these carry the spread: `@std/ulid`'s `encodeTime` for 0, 1, the
  // epoch-ish middle and the 48-bit ceiling.
  ["00000000000000000000000000", 0],
  ["00000000010000000000000000", 1],
  ["01JZZZZZZZ0000000000000000", 1752346656767],
  ["7ZZZZZZZZZ0000000000000000", 281474976710655],
];

test("ulid - shape matches the specification", () => {
  for (let i = 0; i < 100; i++) {
    const id = ulid();
    assertEquals(id.length, 26, `expected 26 characters, got ${id}`);
    assert(
      /^[0-9A-HJKMNP-TV-Z]{26}$/.test(id),
      `${id} is not Crockford base32 (I, L, O and U must not appear)`,
    );
  }
});

test("ulid - encodes the time it was given", () => {
  const seed = 1_700_000_000_000;
  assertEquals(decodeTime(ulid(seed)), seed);

  // …and the current time when given none.
  const before = Date.now();
  const now = decodeTime(ulid());
  const after = Date.now();
  assert(
    now >= before && now <= after,
    `${now} is outside [${before}, ${after}]`,
  );
});

test("decodeTime - reads timestamps @std/ulid produced", () => {
  for (const [id, expected] of STD_VECTORS) {
    assertEquals(decodeTime(id), expected, `${id} decoded to the wrong time`);
  }
});

test("decodeTime - accepts either case, unlike @std", () => {
  // The one deliberate divergence: `newId()` lowercases, so a strict-uppercase
  // decoder could not read this module's own output.
  for (const [id, expected] of STD_VECTORS) {
    assertEquals(decodeTime(id.toLowerCase()), expected);
  }
  const before = Date.now();
  const id = newId();
  const after = Date.now();
  assertEquals(id, id.toLowerCase(), "newId is expected to be lowercase");
  const decoded = decodeTime(id);
  assert(
    decoded >= before && decoded <= after,
    `decodeTime(newId()) gave ${decoded}, outside [${before}, ${after}]`,
  );
});

test("decodeTime - rejects what is not a ULID", () => {
  assertThrows(() => decodeTime(""), Error, "26 characters");
  assertThrows(() => decodeTime("01M2626XKE"), Error, "26 characters");
  assertThrows(
    () => decodeTime(`${STD_VECTORS[0]![0]}X`),
    Error,
    "26 characters",
  );

  // I, L, O and U are excluded from the alphabet precisely to avoid being
  // misread as 1, 1, 0 and V.
  for (const bad of ["I", "L", "O", "U"]) {
    assertThrows(
      () => decodeTime(bad + STD_VECTORS[0]![0].slice(1)),
      Error,
      "Invalid ULID character",
    );
  }

  // A timestamp past the 48-bit field: "8" in the leading position overflows.
  assertThrows(() => decodeTime("8".repeat(26)), Error, "exceeds the maximum");
});

test("ulid - round-trips the full timestamp range", () => {
  const TIME_MAX = 2 ** 48 - 1;
  for (const t of [0, 1, 1_000, Date.now(), TIME_MAX]) {
    assertEquals(decodeTime(ulid(t)), t, `round-trip failed for ${t}`);
  }
});

test("ulid - draws distinct randomness across a pool refill", () => {
  // The pool is 1024 bytes and each id consumes 16, so 64 ids exhaust it —
  // 500 crosses several refills. A refill that failed to redraw would show up
  // here as repeats.
  const seen = new Set<string>();
  for (let i = 0; i < 500; i++) seen.add(ulid(1_700_000_000_000));
  assertEquals(seen.size, 500, "ulid repeated itself within one millisecond");
});

test("ulid - refuses a seed it could not encode faithfully", () => {
  // `encodeTime` divides by 32 ten times and indexes the alphabet with the
  // remainder. Without a guard, NaN produced a 106-character string of
  // "undefined", a negative seed a 34-character one, and 2**50 silently
  // wrapped its timestamp to 0 — each of which would have been stored as an
  // id. `@std/ulid` threw RangeError for all of these; so does this.
  for (const seed of [
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    2 ** 48,
    2 ** 50,
  ]) {
    assertThrows(() => ulid(seed), RangeError, "ULID timestamp");
  }
});

test("decodeTime - is not fooled by Unicode that upper-cases into the alphabet", () => {
  // `ALPHABET.indexOf(char.toUpperCase())` accepted these: `toUpperCase` maps
  // ſ to S, and expands ﬅ and ﬆ to the two characters "ST", which `indexOf`
  // then matched as a substring. A code-point table admits only the 64
  // characters intended.
  for (const ch of [
    "\u017F",
    "\uFB05",
    "\uFB06",
    "\u0131",
    "\uFF21",
    " ",
    "-",
  ]) {
    assertThrows(
      () => decodeTime(`01M2626XK${ch}0000000000000000`),
      RangeError,
      "Invalid ULID character",
    );
  }
});

test("decodeTime - reports the same error class @std did", () => {
  // A consumer catching RangeError specifically must keep catching it.
  assertThrows(() => decodeTime(""), RangeError);
  assertThrows(() => decodeTime("8".repeat(26)), RangeError);
});
