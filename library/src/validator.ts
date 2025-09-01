import type * as v from './schema.ts';

type UnknownSchema = v.BaseSchema<any, any, any>;
type UnknownValidation = v.BaseValidation<any, any, any>;

function buildPipelineResult<T>(pipe: any) {
    return pipe.filter((v: any) => v.kind == "validation" || v.type == "literal")
        .map(constructorToValidator)
        .filter(Boolean)
        .reduce((acc: any, value: any) => {
            for (const [key, val] of Object.entries(value)) {
                if (key in acc) {
                    if (key == "pattern") {
                        // Combine regex patterns with lookahead
                        acc[key] = `(?=${acc[key]})(?=${val})`;
                    } else if (key == "enum") {
                        // For enum values, we keep them as they are
                        acc[key] = val;
                    } else if (key == "minLength" || key == "minItems" || key == "minimum") {
                        // For minimum constraints, take the maximum value (most restrictive)
                        acc[key] = Math.max(acc[key], val as number);
                    } else if (key == "maxLength" || key == "maxItems" || key == "maximum") {
                        // For maximum constraints, take the minimum value (most restrictive)
                        acc[key] = Math.min(acc[key], val as number);
                    } else {
                        // For other properties, last value wins
                        acc[key] = val;
                    }
                } else {
                    acc[key] = val;
                }
            }

            return acc;
        }, {});
}

function constructorToValidator(schema: UnknownSchema | UnknownValidation) {
    const { kind, type } = schema;

    if (kind == "schema") {
        switch (type) {
            case "object": {
                const s = schema as v.ObjectSchema<any, any>;

                // Required fields
                const required: string[] = [];
                const properties: Record<string, any> = {};

                for (const [key, value] of Object.entries(s.entries) as [string, UnknownSchema][]) {
                    const { type } = value;

                    // Optional check
                    {
                        let isRequired = true;
                        // Not required if optional and no default value
                        if (type == "optional" && !(value as v.OptionalSchema<any, any>).default) {
                            isRequired = false;
                        }
                        if (type == "union") {
                            const s = value as v.UnionSchema<any, any>;
                            isRequired = s.options.some((v: UnknownSchema) => v.type == "undefined") == false;
                        }

                        if (isRequired) {
                            required.push(key);
                        }
                    }

                    const validator = constructorToValidator(value);
                    if(validator) {
                        properties[key] = validator;
                    }
                }
                
                if(required.length == 0) {
                    return {
                        bsonType: "object",
                        properties,
                    }
                }

                return {
                    bsonType: "object",
                    required,
                    properties,
                }
            }
            // DEPRECATED: Use object instead
            // https://www.mongodb.com/docs/manual/reference/operator/query/type
            case "undefined": {
                return {
                    bsonType: "undefined",
                    description: `must be undefined`,
                }
            }
            case "string": {
                const s = schema as v.StringSchema<undefined>;
                const pipes: Record<string, any> = buildPipelineResult((s as any).pipe ?? []);

                return {
                    ...pipes,
                    bsonType: "string",
                    description: s.message ?? `must be a string`,
                }
            }
            case "number": {
                const s = schema as v.NumberSchema<undefined>;
                const pipes: Record<string, any> = buildPipelineResult((s as any).pipe ?? []);
                return {
                    ...pipes,
                    bsonType: "number",
                    description: s.message ?? `must be a number`,
                }
            }
            case "boolean": {
                const s = schema as v.BooleanSchema<undefined>;
                return {
                    bsonType: "bool",
                    description: s.message ?? `must be a boolean`,
                }
            }
            case "date": {
                const s = schema as v.DateSchema<undefined>;
                return {
                    bsonType: "date",
                    description: s.message ?? `must be a date`,
                }
            }
            case "null": {
                const s = schema as v.NullSchema<undefined>;
                return {
                    bsonType: "null",
                    description: s.message ?? `must be null`,
                }
            }
            case "array": {
                const s = schema as v.ArraySchema<any, undefined>;
                const items = constructorToValidator(s.item) as any;
                const pipes: Record<string, any> = buildPipelineResult((s as any).pipe ?? []);

                // Special case for array of objects
                if ("minLength" in pipes) {
                    pipes.minItems = pipes.minLength;
                    delete pipes.minLength;
                }

                if ("maxLength" in pipes) {
                    pipes.maxItems = pipes.maxLength;
                    delete pipes.maxLength;
                }

                return {
                    ...pipes,
                    bsonType: "array",
                    items,
                    description: s.message ?? `must be an array`,
                }
            }
            case "optional": {
                const s = schema as v.OptionalSchema<any, any>;
                return constructorToValidator(s.wrapped);
            }
            case "nullable": {
                const s = schema as v.OptionalSchema<any, any>;
                const wrappedValidator = constructorToValidator(s.wrapped) as any;
                
                if (!wrappedValidator) {
                    return {
                        anyOf: [
                            { bsonType: "null" }
                        ]
                    };
                }
                
                return {
                    anyOf: [
                        wrappedValidator,
                        { bsonType: "null" }
                    ]
                };
            }
            case "union": {
                const s = schema as v.UnionSchema<any, any>;
                const anyOf: any[] = [];

                for (const value of s.options) {
                    const element = constructorToValidator(value);
                    if (element) {
                        anyOf.push(element);
                    }
                }

                return {
                    anyOf,
                }
            }
            case "intersect": {
                const s = schema as v.IntersectSchema<any, any>;
                const allOf: any[] = [];

                for (const value of s.options) {
                    const element = constructorToValidator(value);
                    if (element) {
                        allOf.push(element);
                    }
                }

                return {
                    allOf,
                }
            }
            case "literal": {
                const s = schema as v.LiteralSchema<any, any>;
                let bsonType: string = typeof s.literal;

                if (bsonType == "string") {
                    bsonType = "string";
                } else if (bsonType == "number") {
                    bsonType = "number";
                } else if (bsonType == "boolean") {
                    bsonType = "bool";
                } else if (bsonType == "object") {
                    bsonType = "object";
                } else {
                    bsonType = "string";
                }
                
                return {
                    bsonType: bsonType,
                    enum: [s.literal],
                    description: s.message ?? `must be ${s.literal}`,
                }
            }
            case "enum": {
                const s = schema as v.EnumSchema<any, any>;
                
                // Get all the enum values (filtering out the keys in numeric enums)
                const enumValues = s.options.filter(value => 
                    typeof value === "string" || typeof value === "number"
                );
                
                // Determine bsonType based on the actual values
                const firstValue = enumValues[0];
                let bsonType: string;
                
                switch (typeof firstValue) {
                    case "string":
                        bsonType = "string";
                        break;
                    case "number":
                        bsonType = "number";
                        break;
                    default:
                        bsonType = "string";
                }
                
                return {
                    bsonType,
                    enum: enumValues,
                    description: s.message ?? `must be one of the allowed values`,
                }
            }
            case "picklist": {
                const s = schema as v.PicklistSchema<any, any>;
                
                // Get all the picklist values
                const picklistValues = s.options;
                
                // Determine bsonType based on the actual values
                const firstValue = picklistValues[0];
                let bsonType: string;
                
                switch (typeof firstValue) {
                    case "string":
                        bsonType = "string";
                        break;
                    case "number":
                        bsonType = "number";
                        break;
                    default:
                        bsonType = "string";
                }
                
                return {
                    bsonType,
                    enum: picklistValues,
                    description: s.message ?? `must be one of the allowed values`,
                }
            }
            case "record": {
                // Record: arbitrary keys validated by `key` schema and values by `value` schema
                const s = schema as v.RecordSchema<any, UnknownSchema, any>;

                // Build validators for key and value
                const keyValidator = constructorToValidator(s.key as any) as any;
                const valueValidator = constructorToValidator(s.value as any) as any;

                const result: any = {
                    bsonType: "object",
                    description: s.message ?? `must be a record`,
                };

                // MongoDB doesn't support propertyNames, so we use patternProperties instead
                // If we have a key pattern (regex), use it with patternProperties
                if (keyValidator && keyValidator.pattern) {
                    // Use patternProperties with the key pattern to validate both key pattern and value
                    result.patternProperties = {
                        [keyValidator.pattern]: valueValidator || {}
                    };
                    // Don't allow additional properties that don't match the pattern
                    result.additionalProperties = false;
                } else {
                    // If no key pattern specified, just validate values with additionalProperties
                    if (valueValidator) {
                        result.additionalProperties = valueValidator;
                    } else {
                        // If no specific value validator, allow any type
                        result.additionalProperties = {};
                    }
                }

                return result;
            }
            case "any" : {
                return;
            }
            default: {
                throw new Error(`Unsupported schema type: ${type}`);
            }
        }
    } else if (kind == "validation") {
        switch (type) {
            case "length": {
                const s = schema as v.LengthAction<any, any, any>;
                return {
                    minLength: s.requirement,
                    maxLength: s.requirement,
                }
            }
            case "min_value": {
                const s = schema as v.MinValueAction<any, any, any>;
                return {
                    minimum: s.requirement,
                }
            }
            case "max_value": {
                const s = schema as v.MaxValueAction<any, any, any>;
                return {
                    maximum: s.requirement,
                }
            }
            case "min_length": {
                const s = schema as v.MinLengthAction<any, any, any>;
                return {
                    minLength: s.requirement,
                }
            }
            case "max_length": {
                const s = schema as v.MaxLengthAction<any, any, any>;
                return {
                    maxLength: s.requirement,
                }
            }
            case "non_empty": {
                // For arrays, use minItems, for strings use minLength
                // We can't distinguish here, so we provide both and let MongoDB pick the right one
                return {
                    minLength: 1,
                    minItems: 1,
                }
            }
            default: {
                // Check if requirement is a regex
                if ((schema as v.RegexAction<any, any>).requirement instanceof RegExp) {
                    const s = schema as v.RegexAction<any, any>;
                    const regexString = s.requirement.toString();
                    const firstSlashIndex = regexString.indexOf('/');
                    const lastSlashIndex = regexString.lastIndexOf('/');
                    const santizeRegex = regexString.substring(firstSlashIndex + 1, lastSlashIndex);
                    const flags = regexString.substring(lastSlashIndex + 1).replace('u', '');
                    if (flags.length > 0) {
                        console.warn(`[WARN] Unsupported regex flags: ${flags} for "${schema.type}" schema`);
                        // Tips:
                        if (flags.includes('i')) {
                            console.warn(`[WARN] - Tips: Use toLowerCase modifier in your application code`);
                        }
                    }
                    return {
                        pattern: `${santizeRegex}`,
                    }
                }

                console.warn(`[WARN] Unsupported schema type: ${type}`);
                console.log({ kind, type });
                return;
            }
        }
    } else {
        // Not handled yet
        return {};
    }
}

/**
 * Converts a Valibot schema to a MongoDB JSON Schema validator
 * https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema
 * 
 * This function transforms a Valibot schema into a MongoDB-compatible 
 * JSON Schema validator that can be used with MongoDB's schema validation.
 * 
 * @param schema - The Valibot schema to convert
 * @returns A MongoDB validator object with the $jsonSchema property
 * 
 * @example
 * ```ts
 * import * as v from 'valibot';
 * import { toMongoValidator } from './validator.ts';
 * 
 * const userSchema = v.object({
 *   name: v.string(),
 *   age: v.number([v.minValue(0)]),
 * });
 * 
 * const validator = toMongoValidator(userSchema);
 * // Use with db.createCollection or db.command({ collMod: ... })
 * ```
 */
/**
 * Converts a Valibot schema to a MongoDB JSON Schema validator
 * 
 * This function transforms a Valibot schema into a MongoDB-compatible 
 * JSON Schema validator that can be used with MongoDB's schema validation.
 * 
 * @param schema - The Valibot schema to convert
 * @returns A MongoDB validator object with the $jsonSchema property
 * 
 * @example
 * ```typescript
 * import * as v from 'valibot';
 * import { toMongoValidator } from './validator.ts';
 * 
 * const userSchema = v.object({
 *   name: v.string(),
 *   age: v.number([v.minValue(0)]),
 * });
 * 
 * const validator = toMongoValidator(userSchema);
 * await db.createCollection("users", { validator });
 * ```
 * @internal
 */
export function toMongoValidator(schema: v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>) {
    return {
        $jsonSchema: constructorToValidator(schema),
    }
}