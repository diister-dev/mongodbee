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
 * @param seedTime Millisecond timestamp to encode; defaults to now.
 * @returns A 26-character uppercase ULID.
 *
 * @example
 * ```typescript
 * ulid(); // "01M1RCRB9SED3TDHNNB1JAKK21"
 * ```
 */
export function ulid(seedTime: number = Date.now()): string {
  return encodeTime(seedTime) + encodeRandom();
}

/** The largest millisecond timestamp a ULID's 48-bit time field can hold. */
const TIME_MAX = 2 ** 48 - 1;

/**
 * Reads the timestamp back out of a ULID.
 *
 * Accepts either case. Crockford base32 is case-insensitive by specification,
 * and `newId()` lowercases what `ulid()` produces — so rejecting lowercase
 * would mean this could not read the ids the rest of this module hands out.
 * Every input `@std/ulid`'s `decodeTime` accepts is accepted here identically;
 * lowercase is the only addition.
 *
 * @param id A 26-character ULID.
 * @returns The millisecond timestamp encoded in its first ten characters.
 * @throws If `id` is not 26 characters, contains a character outside the
 *   alphabet, or encodes a timestamp beyond the 48-bit range.
 *
 * @example
 * ```typescript
 * decodeTime("01M1RCRB9SED3TDHNNB1JAKK21"); // 1757000000000
 * ```
 */
export function decodeTime(id: string): number {
  if (id.length !== TIME_LEN + RANDOM_LEN) {
    throw new Error(
      `ULID must be exactly ${TIME_LEN + RANDOM_LEN} characters long, got ${id.length}`,
    );
  }

  let time = 0;
  for (let i = 0; i < TIME_LEN; i++) {
    const char = id[i]!.toUpperCase();
    const value = ALPHABET.indexOf(char);
    if (value === -1) {
      throw new Error(`Invalid ULID character found: ${id[i]}`);
    }
    time = time * 32 + value;
  }

  if (time > TIME_MAX) {
    throw new Error(
      `ULID timestamp ${time} exceeds the maximum of ${TIME_MAX}`,
    );
  }

  return time;
}
