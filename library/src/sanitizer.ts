/**
 * Utilities for sanitizing documents before MongoDB operations
 */

/**
 * Recursively removes undefined values from an object
 * This is needed because MongoDB doesn't support undefined as a BSON type
 *
 * @param obj - The object to sanitize
 * @returns A new object with undefined values removed
 */
export function removeUndefined(obj: any): any {
  if (obj === null || obj === undefined) {
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map(removeUndefined).filter((item) => item !== undefined);
  }

  if (typeof obj === "object" && obj.constructor === Object) {
    const result: any = {};
    for (const [key, value] of Object.entries(obj)) {
      if (value !== undefined) {
        result[key] = removeUndefined(value);
      }
    }
    return result;
  }

  return obj;
}

/**
 * Recursively converts undefined values to null
 * Alternative approach if you prefer explicit null values
 *
 * @param obj - The object to sanitize
 * @returns A new object with undefined values converted to null
 */
export function undefinedToNull(obj: any): any {
  if (obj === undefined) {
    return null;
  }

  if (obj === null) {
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map(undefinedToNull);
  }

  if (typeof obj === "object" && obj.constructor === Object) {
    const result: any = {};
    for (const [key, value] of Object.entries(obj)) {
      result[key] = undefinedToNull(value);
    }
    return result;
  }

  return obj;
}

/**
 * Configuration for sanitization behavior
 */
export interface SanitizeOptions {
  /** How to handle undefined values: 'remove' | 'convert-to-null' */
  undefinedBehavior: "remove" | "convert-to-null";
  /** Whether to sanitize nested objects */
  deep: boolean;
}

/**
 * Main sanitization function with configurable behavior
 *
 * @param obj - The object to sanitize
 * @param options - Sanitization options
 * @returns Sanitized object
 */
export function sanitizeDocument(
  obj: any,
  options: SanitizeOptions = { undefinedBehavior: "remove", deep: true },
): any {
  if (
    !options.deep && typeof obj === "object" && obj !== null &&
    !Array.isArray(obj)
  ) {
    // Shallow sanitization - only top level
    const result: any = {};
    for (const [key, value] of Object.entries(obj)) {
      if (options.undefinedBehavior === "remove" && value === undefined) {
        continue;
      }
      result[key] =
        options.undefinedBehavior === "convert-to-null" && value === undefined
          ? null
          : value;
    }
    return result;
  }

  // Deep sanitization
  return options.undefinedBehavior === "remove"
    ? removeUndefined(obj)
    : undefinedToNull(obj);
}

/**
 * Special symbol to explicitly mark fields for removal
 * Use this when you want to explicitly remove a field from a document
 */
export const REMOVE_FIELD = Symbol("REMOVE_FIELD");

/**
 * Helper to explicitly mark a field for removal
 * @example
 * collection.updateOne(filter, { $set: { fieldToRemove: removeField() } })
 */
export function removeField() {
  return REMOVE_FIELD;
}

/**
 * Enhanced sanitization that handles explicit field removal
 *
 * @param obj - The object to sanitize
 * @param options - Sanitization options
 * @returns Sanitized object with proper field removal handling
 */
export function sanitizeForMongoDB(obj: unknown, options: {
  /** How to handle undefined values: 'remove' | 'ignore' | 'error' */
  undefinedBehavior: "remove" | "ignore" | "error";
  /** Whether to sanitize nested objects (should always be true) */
  deep: boolean;
} = { undefinedBehavior: "remove", deep: true }): unknown {
  function processValue(value: unknown): unknown {
    if (value === REMOVE_FIELD) {
      return undefined; // Will be removed by removeUndefined
    }

    if (value === undefined) {
      switch (options.undefinedBehavior) {
        case "remove":
          return undefined; // Will be removed
        case "ignore":
          return IGNORE_FIELD; // Special marker to skip this field
        case "error":
          throw new Error(
            "Undefined values are not allowed. Use removeField() to explicitly remove fields.",
          );
        default:
          return undefined;
      }
    }

    if (value === null) {
      return null;
    }

    if (Array.isArray(value)) {
      return value.map(processValue).filter((item) =>
        item !== undefined && item !== IGNORE_FIELD
      );
    }

    if (
      typeof value === "object" && value !== null &&
      value.constructor === Object
    ) {
      const result: Record<string, unknown> = {};
      for (const [key, val] of Object.entries(value)) {
        const processed = processValue(val);
        if (processed !== undefined && processed !== IGNORE_FIELD) {
          result[key] = processed;
        }
      }
      return result;
    }

    return value;
  }

  return processValue(obj);
}

// Special marker for fields to ignore during updates
const IGNORE_FIELD = Symbol("IGNORE_FIELD");
