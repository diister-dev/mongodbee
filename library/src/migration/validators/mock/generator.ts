/**
 * @fileoverview Single-document mock generation for the simulation validator
 *
 * Thin wrappers around `@diister/valibot-mock` — the ONLY places the
 * simulation talks to the generator, so every generation path produces
 * documents through the exact same code.
 *
 * @module
 */

import * as v from "valibot";
import { createMockGenerator, SKIP } from "@diister/valibot-mock";
import type { MockGeneratorOptions } from "@diister/valibot-mock";
import { createLogger } from "../../../utils/logger.ts";
import { createSeedableRandomizer } from "./randomizer.ts";

const log = createLogger("simulation");

/**
 * Options threaded into ONE generator invocation.
 *
 * `seed` makes the invocation deterministic, so a simulation can be replayed
 * identically. `resolve` is the caller-side hook implementing the link
 * phase: `_id` injection and reference draws from the identifier pools
 * (see `correlation.ts`).
 */
export interface MockDocumentOptions {
  seed?: number;
  resolve?: MockGeneratorOptions["resolve"];
}

/**
 * One generator per schema, reseeded per invocation.
 *
 * `createMockGenerator` builds a Faker, and a Faker binds every method of
 * every module and seeds a Mersenne Twister when it is built: per document,
 * that was most of a mock document's cost. A schema now gets one generator,
 * built over a randomizer this module owns, and each invocation reseeds that
 * randomizer — the stream Faker draws is the one a fresh `faker: { seed }`
 * instance would have drawn, so every document is the same as before.
 *
 * The per-invocation `resolve` hook reaches the generator through a slot
 * the generator's own hook reads. A generator in use is never reentered: a hook that generates a value of
 * the same schema gets a fresh, uncached generator for it.
 */
interface CachedGenerator {
  busy: boolean;
  generate(seed: number, resolve: MockDocumentOptions["resolve"]): unknown;
}

const generators = new WeakMap<object, CachedGenerator>();

function buildGenerator(schema: v.GenericSchema): CachedGenerator {
  const randomizer = createSeedableRandomizer();
  let hook: MockDocumentOptions["resolve"];
  const generator = createMockGenerator(schema as any, {
    faker: { randomizer },
    resolve: (node) => (hook ? hook(node) : SKIP),
  });
  return {
    busy: false,
    generate(seed, resolve) {
      randomizer.seed(seed);
      hook = resolve;
      this.busy = true;
      try {
        return generator.generate();
      } finally {
        hook = undefined;
        this.busy = false;
      }
    },
  };
}

function randomSeed(): number {
  // What Faker seeds itself with when no seed is given.
  return Math.ceil(Math.random() * (2 ** 53 - 1));
}

function generateWith(
  key: object,
  schema: () => v.GenericSchema,
  options: MockDocumentOptions | undefined,
): unknown {
  let cached = generators.get(key);
  if (!cached) {
    cached = buildGenerator(schema());
    generators.set(key, cached);
  } else if (cached.busy) {
    cached = buildGenerator(schema());
  }
  return cached.generate(options?.seed ?? randomSeed(), options?.resolve);
}

/**
 * The `v.object()` a fields record is generated and validated as, built once
 * per record rather than once per document.
 */
type DocumentSchema = v.ObjectSchema<v.ObjectEntries, undefined>;

const documentSchemas = new WeakMap<object, DocumentSchema>();

function documentSchema(fields: Record<string, unknown>): DocumentSchema {
  let schema = documentSchemas.get(fields);
  if (!schema) {
    schema = v.object(fields as v.ObjectEntries);
    documentSchemas.set(fields, schema);
  }
  return schema;
}

/**
 * Removes object keys holding explicit `undefined`, recursively. A BSON
 * document cannot store `undefined` — a real document either has the field
 * or lacks the key — so a mock carrying one is unfaithful: spread-based
 * merge migrations would let it clobber a real value.
 */
function stripUndefinedKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(stripUndefinedKeys);
  }
  if (value !== null && typeof value === "object") {
    const proto = Object.getPrototypeOf(value);
    if (proto === Object.prototype || proto === null) {
      const out: Record<string, unknown> = {};
      for (const [key, entry] of Object.entries(value)) {
        if (entry === undefined) continue;
        out[key] = stripUndefinedKeys(entry);
      }
      return out;
    }
  }
  return value;
}

/**
 * Generates a mock document from a Valibot schema for testing purposes
 * Uses valibot-mock to generate realistic test data
 *
 * Throws when the generator cannot produce a value for the schema (for
 * example a `v.never()` field, or constraints the generator cannot satisfy
 * within its attempt budget), and when a `resolve` hook injects a value the
 * schema rejects. Callers are expected to record that failure, never to
 * swallow it — a silently empty collection asserts nothing downstream.
 *
 * @param schema - Valibot schema representing document structure
 * @param options - Per-invocation seed and resolve hook
 * @returns Mock document matching the schema
 */
export function generateMockDocument(
  schema: Record<string, unknown>,
  options?: MockDocumentOptions,
): Record<string, unknown> {
  const schemaObject = documentSchema(schema);
  const mockData = stripUndefinedKeys(
    generateWith(schema, () => schemaObject, options),
  ) as Record<string, unknown>;

  // Validate the generated data matches the schema
  const validation = v.safeParse(schemaObject, mockData);
  if (validation.success) {
    return validation.output;
  }
  // If validation fails (shouldn't happen), fallback to simple mock
  log.warn(
    "Generated mock data did not validate against schema, using the raw mock instead",
  );
  return mockData;
}

/**
 * Generates a mock scope value from a scoped multi-collection's `scope` schema
 *
 * Unlike {@link generateMockDocument}, the scope schema is a bare Valibot
 * schema (not a record of fields), so it is fed to the mock generator as-is.
 */
export function generateMockScopeValue(
  schema: v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>,
  options?: MockDocumentOptions,
): unknown {
  const mockValue = generateWith(schema, () => schema, options);

  const validation = v.safeParse(schema, mockValue);
  if (validation.success) {
    return validation.output;
  }
  log.warn(
    "Generated mock scope value did not validate against the scope schema, using the raw mock instead",
  );
  return mockValue;
}
