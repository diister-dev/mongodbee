/**
 * @fileoverview A reseedable random source for the mock generator
 *
 * Faker's default randomizer is a Mersenne Twister drawing 53-bit floats.
 * This is the same generator, number for number — a seed yields exactly the
 * stream Faker would draw — so a document generated through it is the one
 * `faker: { seed }` would have produced. What changes is the cost of
 * reseeding, which the simulation does once per mock document: Faker keeps
 * the state in a plain array of doubles, and seeding it measured ~58 µs on
 * V8 against ~3 µs for this typed-array state; building a new Faker per
 * document to get a fresh seed cost another ~150 µs.
 *
 * @module
 */

const STATE_SIZE = 624;
const SHIFT_SIZE = 397;
const MATRIX_A = 0x9908b0df;
const UPPER_MASK = 0x80000000;
const LOWER_MASK = 0x7fffffff;
const TWO_POW_26 = 67108864;
const TWO_POW_MINUS_53 = 1.1102230246251565e-16;

/** The randomizer contract Faker accepts (`new Faker({ randomizer })`). */
export interface SeedableRandomizer {
  next(): number;
  seed(seed: number | number[]): void;
}

export function createSeedableRandomizer(): SeedableRandomizer {
  // Uint32Array keeps every word modulo 2^32, which is all the generator's
  // bitwise arithmetic ever reads of Faker's double-valued words.
  const state = new Uint32Array(STATE_SIZE);
  let index = 0;

  const mix = (upper: number, lower: number, far: number) => {
    const y = (upper & UPPER_MASK) | (lower & LOWER_MASK);
    return far ^ (y >>> 1) ^ (-(y & 1) & MATRIX_A);
  };

  const twist = () => {
    for (let i = 0; i < STATE_SIZE - SHIFT_SIZE; i++) {
      state[i] = mix(state[i]!, state[i + 1]!, state[i + SHIFT_SIZE]!);
    }
    for (let i = STATE_SIZE - SHIFT_SIZE; i < STATE_SIZE - 1; i++) {
      state[i] = mix(
        state[i]!,
        state[i + 1]!,
        state[i + SHIFT_SIZE - STATE_SIZE]!,
      );
    }
    state[STATE_SIZE - 1] = mix(
      state[STATE_SIZE - 1]!,
      state[0]!,
      state[SHIFT_SIZE - 1]!,
    );
  };

  const nextU32 = () => {
    let y = state[index]!;
    y ^= y >>> 11;
    y ^= (y << 7) & 0x9d2c5680;
    y ^= (y << 15) & 0xefc60000;
    y ^= y >>> 18;
    if (++index >= STATE_SIZE) {
      twist();
      index = 0;
    }
    return y >>> 0;
  };

  return {
    next: () =>
      ((nextU32() >>> 5) * TWO_POW_26 + (nextU32() >>> 6)) * TWO_POW_MINUS_53,
    seed: (seed) => {
      if (typeof seed !== "number") {
        throw new Error("The mock randomizer only takes a numeric seed");
      }
      state[0] = seed;
      for (let i = 1; i < STATE_SIZE; i++) {
        const previous = state[i - 1]! ^ (state[i - 1]! >>> 30);
        state[i] = Math.imul(1812433253, previous) + i;
      }
      twist();
      index = 0;
    },
  };
}
