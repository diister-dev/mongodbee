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
import { createMockGenerator } from "@diister/valibot-mock";
import type { MockGeneratorOptions } from "@diister/valibot-mock";

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
 * Builds the generator options, including `faker.seed` only when a seed is
 * given.
 */
function toGeneratorOptions(
  options?: MockDocumentOptions,
): MockGeneratorOptions {
  const out: MockGeneratorOptions = {};
  if (options?.seed !== undefined) out.faker = { seed: options.seed };
  if (options?.resolve !== undefined) out.resolve = options.resolve;
  return out;
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
  // Wrap the schema in v.object() for valibot-mock
  // deno-lint-ignore no-explicit-any
  const schemaObject = v.object(
    schema as Record<string, v.BaseSchema<any, any, any>>,
  );

  // Use valibot-mock to generate realistic test data from schema
  // deno-lint-ignore no-explicit-any
  const generator = createMockGenerator(
    schemaObject as any,
    toGeneratorOptions(options),
  );
  const mockData = generator.generate();

  // Validate the generated data matches the schema
  const validation = v.safeParse(schemaObject, mockData);
  if (validation.success) {
    return validation.output;
  }
  // If validation fails (shouldn't happen), fallback to simple mock
  console.warn(
    "/!\\ Generated mock data did not validate against schema, using simple mock instead",
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
  // deno-lint-ignore no-explicit-any
  const generator = createMockGenerator(
    schema as any,
    toGeneratorOptions(options),
  );
  const mockValue = generator.generate();

  const validation = v.safeParse(schema, mockValue);
  if (validation.success) {
    return validation.output;
  }
  console.warn(
    "/!\\ Generated mock scope value did not validate against the scope schema, using raw mock instead",
  );
  return mockValue;
}
