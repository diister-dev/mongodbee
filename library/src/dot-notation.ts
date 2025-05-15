
import * as v from 'valibot';
import { FlatType } from "../types/flat.ts";

/**
 * Represents an element of a key path that can be either a string or a predicate function
 */
export type KeyFullPathElement = string | ((v: string) => boolean);

/**
 * Represents a full path of keys in dot notation
 */
export type KeyFullPath = KeyFullPathElement[];

/**
 * Entry used internally to store path and schema information
 */
export type SchemaPathEntry = [KeyFullPath, v.BaseSchema<any, any, any>];

/**
 * Checks if a path string matches a key path pattern
 * 
 * @param fullPath - The pattern to match against
 * @param p - The path string to check
 * @returns True if the path matches the pattern, false otherwise
 */
export function checkPath(fullPath: KeyFullPath, p: unknown): boolean {
    if (typeof p !== 'string') return false;
    const split = p.split('.');
    if (split.length !== fullPath.length) return false;
    
    for (let i = 0; i < split.length; i++) {
        const k = fullPath[i];
        if (typeof k === 'string') {
            if (k !== split[i]) return false;
        } else if (typeof k === 'function') {
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
 * @param schema - The valibot schema to extract paths from
 * @returns Array of path entries containing path pattern and schema
 */
export function extractSchemaPaths(schema: v.BaseSchema<any, any, any>): SchemaPathEntry[] {
    const entries: SchemaPathEntry[] = [];
    const toProcess: Array<{ key: KeyFullPath; value: v.BaseSchema<any, any, any> }> = [
        { key: [], value: schema }
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
 * @param schema - The original schema to transform
 * @returns A custom valibot schema that supports dot notation
 */
export function createDotNotationSchema<T extends v.BaseSchema<any, any, any>>(schema: T): v.CustomSchema<any, any> {
    const entries = extractSchemaPaths(schema);
    
    return v.custom((input) => {
        if (typeof input !== 'object' || input === null) return false;
        
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
 */
export type DotNotationSchemaOutput<T extends v.BaseSchema<any, any, any>> = 
    Partial<FlatType<v.InferOutput<T>>>;

/**
 * Type helper for dot notation schema input type
 */
export type DotNotationSchemaInput<T extends v.BaseSchema<any, any, any>> = 
    Partial<FlatType<v.InferInput<T>>>;
