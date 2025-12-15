import * as v from "valibot";
import { FlatType } from "../types/flat.ts";

/**
 * Represents an element of a key path that can be either a string or a predicate function
 *
 * This type is used to define parts of a document path in MongoDB dot notation.
 * It can be either a literal string (exact match) or a function that validates a path segment.
 */
export type KeyFullPathElement = string | ((v: string) => boolean);

/**
 * Represents a full path of keys in dot notation
 *
 * An array of path elements that describes a complete path to a field in MongoDB dot notation.
 * Example: ["user", "address", "street"] represents "user.address.street"
 */
export type KeyFullPath = KeyFullPathElement[];

/**
 * Entry used internally to store path and schema information
 *
 * A tuple containing both the path to a field and its validation schema.
 */
export type SchemaPathEntry = [KeyFullPath, v.BaseSchema<any, any, any>];

/**
 * Checks if a path string matches a key path pattern
 *
 * This function validates if a dot notation path string matches a given path pattern.
 * It supports both exact matches with strings and flexible matching with predicate functions.
 *
 * @param fullPath - The pattern to match against, as an array of path elements
 * @param p - The path string to check (e.g., "user.address.street")
 * @returns True if the path matches the pattern, false otherwise
 * @example
 * ```typescript
 * // Exact match
 * checkPath(["user", "address", "street"], "user.address.street"); // true
 *
 * // Using a predicate function to match numeric indices
 * const arrayPattern = ["items", (v) => !isNaN(Number(v))];
 * checkPath(arrayPattern, "items.0"); // true
 * checkPath(arrayPattern, "items.abc"); // false
 * ```
 */
export function checkPath(fullPath: KeyFullPath, p: unknown): boolean {
  if (typeof p !== "string") return false;
  const split = p.split(".");
  if (split.length !== fullPath.length) return false;

  for (let i = 0; i < split.length; i++) {
    const k = fullPath[i];
    if (typeof k === "string") {
      if (k !== split[i]) return false;
    } else if (typeof k === "function") {
      if (!k(split[i])) return false;
    } else {
      return false;
    }
  }

  return true;
}

/**
 * Extracts all possible paths from a valibot schema to support dot notation
 *
 * This function analyzes a Valibot schema and extracts all possible paths in dot notation format
 * along with their corresponding subschemas. It handles nested objects and arrays, including
 * support for array indices with both numeric indices and MongoDB's $[] operator.
 *
 * @param schema - The valibot schema to extract paths from
 * @returns Array of path entries containing path pattern and schema
 * @example
 * ```typescript
 * const userSchema = v.object({
 *   name: v.string(),
 *   address: v.object({
 *     city: v.string(),
 *     zipCode: v.number()
 *   }),
 *   tags: v.array(v.string())
 * });
 *
 * // Results will include entries for:
 * // ["name"] -> string schema
 * // ["address"] -> address object schema
 * // ["address", "city"] -> string schema
 * // ["address", "zipCode"] -> number schema
 * // ["tags"] -> array schema
 * // ["tags", "$[]"] -> string schema
 * // ["tags", (v) => !isNaN(Number(v))] -> string schema
 * const paths = extractSchemaPaths(userSchema);
 * ```
 */
export function extractSchemaPaths(
  schema: v.BaseSchema<any, any, any>,
): SchemaPathEntry[] {
  const entries: SchemaPathEntry[] = [];
  const toProcess: Array<
    { key: KeyFullPath; value: v.BaseSchema<any, any, any> }
  > = [
    { key: [], value: schema },
  ];

  while (toProcess.length > 0) {
    const { key, value } = toProcess.pop()!;

    if (value.type === "object") {
      const objectValue = value as v.ObjectSchema<any, any>;
      for (const k in objectValue.entries) {
        const v = objectValue.entries[k as keyof typeof objectValue];
        toProcess.push({
          key: [...key, k],
          value: v,
        });
      }
    } else if (value.type === "array") {
      // Support for array using wildcard notation
      toProcess.push({
        key: [...key, `$[]`],
        value: (value as v.ArraySchema<any, any>).item,
      });

      // Support for array using numeric indices
      toProcess.push({
        key: [...key, (v: string) => !isNaN(Number(v))],
        value: (value as v.ArraySchema<any, any>).item,
      });
    }

    if (key.length > 0) {
      entries.push([key, value]);
    }
  }

  return entries;
}

/**
 * Creates a custom valibot schema that validates objects using dot notation
 *
 * This function creates a custom Valibot validator that can validate objects where keys
 * are in MongoDB dot notation format (e.g., "user.address.street"). It extracts all possible
 * paths from the provided schema and validates each field against its corresponding subschema.
 *
 * @param schema - The original schema to transform
 * @returns A custom valibot schema that supports dot notation validation
 * @example
 * ```typescript
 * const userSchema = v.object({
 *   name: v.string(),
 *   address: v.object({
 *     city: v.string(),
 *     zipCode: v.number()
 *   })
 * });
 *
 * const dotSchema = createDotNotationSchema(userSchema);
 *
 * // This will validate successfully
 * v.parse(dotSchema, {
 *   "name": "John",
 *   "address.city": "New York"
 * });
 * ```
 */
export function createDotNotationSchema<T extends v.BaseSchema<any, any, any>>(
  schema: T,
): v.CustomSchema<any, any> {
  const entries = extractSchemaPaths(schema);

  return v.custom((input) => {
    if (typeof input !== "object" || input === null) return false;

    for (const key in input) {
      const value = input[key as keyof typeof input];
      const found = entries.find(([fullPath, _]) => checkPath(fullPath, key));

      if (!found) {
        continue;
      }

      const [_, pathSchema] = found;
      const result = v.safeParse(pathSchema, value);

      if (!result.success) {
        return false;
      }
    }

    return true;
  });
}

/**
 * Type helper for dot notation schema output type
 *
 * Represents the output type of a schema when used with dot notation,
 * allowing for partial updates using MongoDB's dot notation syntax.
 *
 * @template T - The base schema type
 */
export type DotNotationSchemaOutput<T extends v.BaseSchema<any, any, any>> =
  Partial<FlatType<v.InferOutput<T>>>;

/**
 * Type helper for dot notation schema input type
 *
 * Represents the input type of a schema when used with dot notation,
 * allowing for partial updates using MongoDB's dot notation syntax.
 *
 * @template T - The base schema type
 */
export type DotNotationSchemaInput<T extends v.BaseSchema<any, any, any>> =
  Partial<FlatType<v.InferInput<T>>>;

/**
 * Gets a nested value from an object using dot notation path
 *
 * This function retrieves a value from a nested object structure using a
 * dot-separated path string (e.g., "user.address.street").
 *
 * @param obj - The object to retrieve the value from
 * @param path - The dot notation path string (e.g., "data.email")
 * @returns The value at the specified path, or undefined if not found
 * @example
 * ```typescript
 * const doc = { data: { email: "test@example.com", name: "John" } };
 * getNestedValue(doc, "data.email"); // "test@example.com"
 * getNestedValue(doc, "data.name"); // "John"
 * getNestedValue(doc, "data.missing"); // undefined
 * ```
 */
export function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.');
  let current: unknown = obj;
  for (const part of parts) {
    if (current === null || current === undefined || typeof current !== 'object') {
      return undefined;
    }
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}
