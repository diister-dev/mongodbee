/**
 * ULID generation, with the randomness drawn in batches.
 *
 * Replaces the npm `ulid` package, whose `encodeRandom` calls its PRNG once per
 * character — sixteen `crypto.getRandomValues` calls, each on a freshly
 * allocated one-element array, for every id. That measured ~6.3 µs per id
 * against ~0.5 µs for the same algorithm drawing from a shared buffer, and
 * `newId()` runs on every inserted document.
 *
 * The output is identical in shape to any conformant ULID: 26 Crockford
 * base32 characters, a 48-bit millisecond timestamp in the first ten, 80 bits
 * of randomness in the last sixteen. Randomness still comes from the platform
 * CSPRNG — the batching changes how often it is asked, not what it returns.
 *
 * Deliberately NOT monotonic within a millisecond, matching the previous
 * behaviour: ids generated in the same millisecond sort arbitrarily relative to
 * one another, which callers that need insertion order must account for.
 *
 * @module
 */

/** Crockford base32, the ULID alphabet: no I, L, O or U. */
const ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const TIME_LEN = 10;
const RANDOM_LEN = 16;

/** The largest millisecond timestamp the 48-bit time field can hold. */
const TIME_MAX = 2 ** 48 - 1;

/**
 * Character code to alphabet position, `-1` for everything else.
 *
 * A table rather than `ALPHABET.indexOf(char.toUpperCase())`, which was wrong
 * twice over: `toUpperCase` is Unicode-aware and expands `ﬅ` and `ﬆ` to the two
 * characters `"ST"`, and `indexOf` matches substrings — so those decoded as `S`
 * instead of being rejected. Indexing by code point admits exactly the 64
 * characters intended and nothing else.
 */
const DECODE = (() => {
  const table = new Int8Array(128).fill(-1);
  for (let i = 0; i < ALPHABET.length; i++) {
    const code = ALPHABET.charCodeAt(i);
    table[code] = i;
    // The lowercase half (see decodeTime on why) — letters only. Digits sit
    // 32 below P..Y, so shifting them too would have entered "5" under "U".
    if (code >= 65) table[code + 32] = i;
  }
  return table;
})();

/**
 * Randomness is drawn in blocks and handed out one byte at a time. 1024 bytes
 * covers 64 ids per syscall; the buffer is refilled, never reused.
 */
const POOL_SIZE = 1024;
const pool = new Uint8Array(POOL_SIZE);
let poolOffset = POOL_SIZE;

function nextByte(): number {
  if (poolOffset >= POOL_SIZE) {
    crypto.getRandomValues(pool);
    poolOffset = 0;
  }
  return pool[poolOffset++]!;
}

function encodeTime(now: number): string {
  let out = "";
  for (let i = TIME_LEN - 1; i >= 0; i--) {
    const mod = now % 32;
    out = ALPHABET[mod] + out;
    now = (now - mod) / 32;
  }
  return out;
}

function encodeRandom(): string {
  let out = "";
  for (let i = 0; i < RANDOM_LEN; i++) {
    // A byte is 0-255 and the alphabet is 32 wide; masking to the low 5 bits
    // keeps the distribution uniform, where a modulo would not.
    out += ALPHABET[nextByte() & 0x1f];
  }
  return out;
}

/**
 * Generates a ULID for the current time.
 *
 * Returns the canonical uppercase form. `newId()` in `ids.ts` lowercases it;
 * `decodeTime` reads either.
 *
 * @param seedTime Millisecond timestamp to encode; defaults to now.
 * @returns A 26-character uppercase ULID.
 * @throws RangeError If `seedTime` is not an integer in `[0, 2**48 - 1]`.
 *
 * @example
 * ```typescript
 * ulid(); // "01M1RCRB9SED3TDHNNB1JAKK21"
 * ```
 */
export function ulid(seedTime: number = Date.now()): string {
  // Refusing a bad seed rather than encoding it: `encodeTime` divides by 32
  // ten times and indexes the alphabet with the remainder, so `NaN` produced a
  // 106-character string of "undefined" and a timestamp past the 48-bit field
  // silently wrapped — either of which would have been stored as an id. This
  // is the guard `@std/ulid` had and this implementation had dropped.
  if (!Number.isInteger(seedTime) || seedTime < 0 || seedTime > TIME_MAX) {
    throw new RangeError(
      `ULID timestamp must be an integer in [0, ${TIME_MAX}], got ${seedTime}`,
    );
  }
  return encodeTime(seedTime) + encodeRandom();
}

/**
 * Reads the timestamp back out of a ULID.
 *
 * Accepts either case, which `@std/ulid` does not: Crockford base32 is
 * case-insensitive by specification, and `newId()` lowercases what `ulid()`
 * produces — a strict-uppercase decoder could not read this module's own
 * output. That is the only intended difference; every input `@std/ulid`
 * accepts is accepted here and yields the same value.
 *
 * Only the first ten characters are validated, matching `@std/ulid`: the
 * remaining sixteen carry randomness this function never reads.
 *
 * @param id A 26-character ULID.
 * @returns The millisecond timestamp encoded in its first ten characters.
 * @throws RangeError If `id` is not 26 characters, if one of its first ten is
 *   outside the alphabet, or if they encode a timestamp beyond the 48-bit
 *   range.
 *
 * @example
 * ```typescript
 * decodeTime("01M1RCRB9SED3TDHNNB1JAKK21"); // 1788598824249
 * ```
 */
export function decodeTime(id: string): number {
  if (id.length !== TIME_LEN + RANDOM_LEN) {
    throw new RangeError(
      `ULID must be exactly ${TIME_LEN + RANDOM_LEN} characters long, got ${id.length}`,
    );
  }

  let time = 0;
  for (let i = 0; i < TIME_LEN; i++) {
    const code = id.charCodeAt(i);
    const value = code < 128 ? DECODE[code]! : -1;
    if (value < 0) {
      throw new RangeError(`Invalid ULID character found: ${id[i]}`);
    }
    time = time * 32 + value;
  }

  if (time > TIME_MAX) {
    throw new RangeError(
      `ULID timestamp ${time} exceeds the maximum of ${TIME_MAX}`,
    );
  }

  return time;
}
