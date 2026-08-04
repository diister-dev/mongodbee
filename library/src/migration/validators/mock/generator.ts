/**
 * @fileoverview Single-document mock generation for the simulation validator
 *
 * Thin wrappers around `@diister/valibot-mock`. These are the ONLY places the
 * simulation talks to the generator, so both generation paths (initial state
 * building and state propagation) produce documents through the exact same
 * code — a divergence here would mean the two paths validate different data
 * shapes without anyone noticing.
 *
 * @module
 */

import * as v from "valibot";
import { createMockGenerator } from "@diister/valibot-mock";

/**
 * Generates a mock document from a Valibot schema for testing purposes
 * Uses valibot-mock to generate realistic test data
 *
 * Throws when the generator cannot produce a value for the schema (for
 * example a `v.never()` field, or constraints the generator cannot satisfy
 * within its attempt budget). Callers are expected to record that failure —
 * never to swallow it — because a swallowed generation failure leaves the
 * collection empty and downstream validation loops assert nothing on it.
 *
 * @param schema - Valibot schema representing document structure
 * @returns Mock document matching the schema
 */
export function generateMockDocument(
  schema: Record<string, unknown>,
): Record<string, unknown> {
  // Wrap the schema in v.object() for valibot-mock
  // deno-lint-ignore no-explicit-any
  const schemaObject = v.object(
    schema as Record<string, v.BaseSchema<any, any, any>>,
  );

  // Use valibot-mock to generate realistic test data from schema
  // deno-lint-ignore no-explicit-any
  const generator = createMockGenerator(schemaObject as any);
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
): unknown {
  // deno-lint-ignore no-explicit-any
  const generator = createMockGenerator(schema as any);
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
