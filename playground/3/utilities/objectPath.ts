/**
 * @fileoverview Object flattening and unflattening utilities with support for nested objects and arrays
 */

type FlatObject = Record<string, unknown>;

/**
 * Flattens a nested object or array into a flat object with dot-notation keys
 * 
 * @example
 * ```ts
 * const nested = {
 *   user: {
 *     name: "Alice",
 *     hobbies: ["reading", "coding"]
 *   },
 *   active: true
 * };
 * 
 * flattenObject(nested)
 * // {
 * //   "user.name": "Alice",
 * //   "user.hobbies.0": "reading",
 * //   "user.hobbies.1": "coding",
 * //   "active": true
 * // }
 * ```
 * 
 * @param obj - Object to flatten (can be nested object, array, or primitive)
 * @param prefix - Internal parameter for recursion, prefix for keys
 * @returns Flattened object with dot-notation keys
 */
export function flattenObject(obj: unknown, prefix = ""): FlatObject {
  const flat: FlatObject = {};

  if (obj === null || obj === undefined) {
    if (prefix) flat[prefix] = obj;
    return flat;
  }

  if (typeof obj !== "object") {
    flat[prefix] = obj;
    return flat;
  }

  if (Array.isArray(obj)) {
    if (obj.length === 0) {
      flat[prefix] = [];
      return flat;
    }

    for (let i = 0; i < obj.length; i++) {
      const key = prefix ? `${prefix}.${i}` : `${i}`;
      Object.assign(flat, flattenObject(obj[i], key));
    }
    return flat;
  }

  // Handle regular objects
  const keys = Object.keys(obj);
  if (keys.length === 0) {
    if (prefix) flat[prefix] = {}; // Only add empty object if we have a prefix
    return flat;
  }

  for (const key of keys) {
    const newKey = prefix ? `${prefix}.${key}` : key;
    const value = (obj as Record<string, unknown>)[key];
    Object.assign(flat, flattenObject(value, newKey));
  }

  // Sort keys to ensure consistent order
  const sortedFlat: FlatObject = {};
  const sortedKeys = Object.keys(flat).sort((a, b) => {
    const aDepth = a.split('.').length;
    const bDepth = b.split('.').length;
    if (aDepth !== bDepth) return aDepth - bDepth;
    return a.localeCompare(b);
  });
  for (const key of sortedKeys) {
    sortedFlat[key] = flat[key];
  }
  return sortedFlat;
}

/**
 * Reconstructs a nested object from a flattened object with dot-notation keys
 * 
 * @example
 * ```ts
 * const flattened = {
 *   "user.name": "Alice",
 *   "user.hobbies.0": "reading",
 *   "user.hobbies.1": "coding",
 *   "active": true
 * };
 * 
 * unflattenObject(flattened)
 * // {
 * //   user: {
 * //     name: "Alice",
 * //     hobbies: ["reading", "coding"]
 * //   },
 * //   active: true
 * // }
 * ```
 * 
 * @param flat - Flattened object with dot-notation keys
 * @returns Reconstructed nested object
 */
export function unflattenObject(flat: FlatObject): unknown {
  // deno-lint-ignore no-explicit-any
  const result: any = {};

  // Sort keys to ensure proper reconstruction order
  const sortedKeys = Object.keys(flat).sort((a, b) => {
    const aDepth = a.split('.').length;
    const bDepth = b.split('.').length;
    if (aDepth !== bDepth) return aDepth - bDepth;
    return a.localeCompare(b);
  });

  for (const key of sortedKeys) {
    const value = flat[key];
    const parts = key.split('.');
    
    // Navigate to the parent object
    // deno-lint-ignore no-explicit-any
    let current: any = result;
    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      const nextPart = parts[i + 1];
      
      if (!(part in current)) {
        // Create array or object based on next part
        current[part] = /^\d+$/.test(nextPart) ? [] : {};
      }
      
      current = current[part];
    }

    const lastPart = parts[parts.length - 1];
    
    // Set the final value
    if (/^\d+$/.test(lastPart)) {
      // Array index
      const index = parseInt(lastPart, 10);
      current[index] = value;
    } else {
      // Object property
      current[lastPart] = value;
    }
  }

  return result;
}

/**
 * Checks if an object is deeply nested (contains objects or arrays as values)
 * 
 * @example
 * ```ts
 * isNestedObject({ name: "Alice" })  // false
 * isNestedObject({ user: { name: "Alice" } })  // true
 * isNestedObject({ items: ["a", "b"] })  // true
 * ```
 * 
 * @param obj - Object to check
 * @returns True if object contains nested structures
 */
export function isNestedObject(obj: unknown): boolean {
  if (!obj || typeof obj !== "object") return false;
  
  // Arrays are considered nested structures
  if (Array.isArray(obj)) return true;
  
  // Objects are nested if they have object/array values
  const values = Object.values(obj as object);
  
  return values.some(value => 
    value !== null && 
    typeof value === "object"
  );
}

/**
 * Gets the depth of nesting in an object
 * 
 * @example
 * ```ts
 * getObjectDepth({ name: "Alice" })  // 1
 * getObjectDepth({ user: { profile: { name: "Alice" } } })  // 3
 * getObjectDepth(["a", ["b", "c"]])  // 2
 * ```
 * 
 * @param obj - Object to measure
 * @returns Maximum nesting depth
 */
export function getObjectDepth(obj: unknown): number {
  if (!obj || typeof obj !== "object") return 0;
  
  let maxDepth = 1;
  
  const values = Array.isArray(obj) ? obj : Object.values(obj as object);
  
  for (const value of values) {
    if (value !== null && typeof value === "object") {
      maxDepth = Math.max(maxDepth, 1 + getObjectDepth(value));
    }
  }
  
  return maxDepth;
}

/**
 * Compares two objects for equality after flattening them
 * @example
 * ```ts
 * flattenEquals(
 *  { user: { name: "Alice", hobbies: ["reading"] } },
 *  { "user.name": "Alice", "user.hobbies.0": "reading" }
 * )  // true
 * ```
 * @param obj1 - First object to compare
 * @param obj2 - Second object to compare
 * @returns True if objects are equal after flattening
 */
export function flattenEquals(obj1: unknown, obj2: unknown): boolean {
  const flat1 = flattenObject(obj1);
  const flat2 = flattenObject(obj2);
  return JSON.stringify(flat1) === JSON.stringify(flat2);
}