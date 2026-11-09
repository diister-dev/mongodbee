import type * as v from "./schema.ts";
import { createLogger } from "./utils/logger.ts";

const log = createLogger("validator");

type UnknownSchema = v.BaseSchema<any, any, any>;
type UnknownValidation = v.BaseValidation<any, any, any>;

interface Bound {
  value: number;
  exclusive: boolean;
}

type BoundSide = "lower" | "upper";

const BOUND_KEYS: Record<BoundSide, { value: string; flag: string }> = {
  lower: { value: "minimum", flag: "exclusiveMinimum" },
  upper: { value: "maximum", flag: "exclusiveMaximum" },
};

function tighterBound(
  side: BoundSide,
  current: Bound | undefined,
  next: Bound,
): Bound {
  if (current === undefined) return next;
  if (next.value === current.value) {
    return {
      value: current.value,
      exclusive: current.exclusive || next.exclusive,
    };
  }
  const nextIsTighter =
    side === "lower" ? next.value > current.value : next.value < current.value;
  return nextIsTighter ? next : current;
}

function writeBound(
  acc: Record<string, unknown>,
  side: BoundSide,
  bound: Bound,
) {
  const keys = BOUND_KEYS[side];
  acc[keys.value] = bound.value;
  if (bound.exclusive) {
    acc[keys.flag] = true;
  } else {
    delete acc[keys.flag];
  }
}

const ASTRAL_CHARACTER = "[\\x{10000}-\\x{10FFFF}]";

// $jsonSchema counts code points, valibot UTF-16 units: an astral code point is two units.
function minLengthInCodePoints(
  constraints: Record<string, any>,
): Record<string, any> {
  const { minLength, ...rest } = constraints;
  if (typeof minLength !== "number" || minLength < 2) return constraints;
  return {
    ...rest,
    anyOf: [
      { minLength },
      { minLength: Math.ceil(minLength / 2), pattern: ASTRAL_CHARACTER },
    ],
  };
}

const VALUE_PRESERVING_TRANSFORMATIONS = new Set([
  "brand",
  "flavor",
  "readonly",
]);

function actionsCheckingStoredValue(pipe: any[]): any[] {
  const lastReshaping = pipe.findLastIndex(
    (item) =>
      item.kind == "transformation" &&
      !VALUE_PRESERVING_TRANSFORMATIONS.has(item.type),
  );
  return pipe.slice(lastReshaping + 1);
}

function allPatterns(patterns: readonly string[]): string {
  return `^${patterns.map((pattern) => `(?=[\\s\\S]*?(?:${pattern}))`).join("")}`;
}

function buildPipelineResult(pipe: any) {
  const bounds: Partial<Record<BoundSide, Bound>> = {};
  const patterns: string[] = [];
  return actionsCheckingStoredValue(pipe)
    .filter((v: any) => v.kind == "validation" || v.type == "literal")
    .map((action) => constructorToValidator(action))
    .filter(Boolean)
    .reduce((acc: any, value: any) => {
      for (const [key, val] of Object.entries(value)) {
        if (key == "exclusiveMinimum" || key == "exclusiveMaximum") {
          continue;
        }
        if (key == "minimum" || key == "maximum") {
          const side: BoundSide = key == "minimum" ? "lower" : "upper";
          const bound = tighterBound(side, bounds[side], {
            value: val as number,
            exclusive: value[BOUND_KEYS[side].flag] === true,
          });
          bounds[side] = bound;
          writeBound(acc, side, bound);
          continue;
        }
        if (key in acc) {
          if (key == "pattern") {
            patterns.push(val as string);
            acc[key] = allPatterns(patterns);
          } else if (key == "enum") {
            // For enum values, we keep them as they are
            acc[key] = val;
          } else if (key == "minLength" || key == "minItems") {
            acc[key] = Math.max(acc[key], val as number);
          } else if (key == "maxLength" || key == "maxItems") {
            acc[key] = Math.min(acc[key], val as number);
          } else {
            // For other properties, last value wins
            acc[key] = val;
          }
        } else {
          if (key == "pattern") patterns.push(val as string);
          acc[key] = val;
        }
      }

      return acc;
    }, {});
}

function constructorToValidator(
  schema: UnknownSchema | UnknownValidation,
  ctx: { lazyStack: Set<v.LazySchema<any>> } = { lazyStack: new Set() },
) {
  const { kind, type } = schema;

  if (kind == "schema") {
    switch (type) {
      // Loose/strict objects share the object translation: MongoDB's
      // $jsonSchema allows additional properties by DEFAULT, which is exactly
      // valibot's looseObject semantics (known entries validated, unknown
      // keys pass through). strictObject adds additionalProperties: false.
      case "object":
      case "loose_object":
      case "strict_object": {
        const s = schema as v.ObjectSchema<any, any>;

        // Required fields
        const required: string[] = [];
        const properties: Record<string, any> = {};

        for (const [key, value] of Object.entries(s.entries) as [
          string,
          UnknownSchema,
        ][]) {
          if (value === undefined) {
            // Field is removed
            continue;
          }
          const { type } = value;

          // Optional check
          {
            let isRequired = true;
            // Not required if optional and no default value
            if (
              type == "optional" &&
              !(value as v.OptionalSchema<any, any>).default
            ) {
              isRequired = false;
            }
            // Not required if nullish and no default value
            if (
              type == "nullish" &&
              !(value as v.NullishSchema<any, any>).default
            ) {
              isRequired = false;
            }
            if (type == "union") {
              const s = value as v.UnionSchema<any, any>;
              isRequired =
                s.options.some((v: UnknownSchema) => v.type == "undefined") ==
                false;
            }

            if (isRequired) {
              required.push(key);
            }
          }

          const validator = constructorToValidator(value, ctx);
          if (validator) {
            properties[key] = validator;
          }
        }

        const strict =
          type === "strict_object" ? { additionalProperties: false } : {};
        if (required.length == 0) {
          return {
            bsonType: "object",
            properties,
            ...strict,
          };
        }

        return {
          bsonType: "object",
          required,
          properties,
          ...strict,
        };
      }
      // DEPRECATED: Use object instead
      // https://www.mongodb.com/docs/manual/reference/operator/query/type
      case "undefined": {
        return {
          bsonType: "undefined",
          description: `must be undefined`,
        };
      }
      case "string": {
        const s = schema as v.StringSchema<undefined>;
        const pipes: Record<string, any> = buildPipelineResult(
          (s as any).pipe ?? [],
        );

        return {
          ...minLengthInCodePoints(pipes),
          bsonType: "string",
          description: s.message ?? `must be a string`,
        };
      }
      case "number": {
        const s = schema as v.NumberSchema<undefined>;
        const pipes: Record<string, any> = buildPipelineResult(
          (s as any).pipe ?? [],
        );
        return {
          ...pipes,
          bsonType: "number",
          description: s.message ?? `must be a number`,
        };
      }
      case "boolean": {
        const s = schema as v.BooleanSchema<undefined>;
        return {
          bsonType: "bool",
          description: s.message ?? `must be a boolean`,
        };
      }
      case "date": {
        const s = schema as v.DateSchema<undefined>;
        return {
          bsonType: "date",
          description: s.message ?? `must be a date`,
        };
      }
      case "null": {
        const s = schema as v.NullSchema<undefined>;
        return {
          bsonType: "null",
          description: s.message ?? `must be null`,
        };
      }
      case "array": {
        const s = schema as v.ArraySchema<any, undefined>;
        const items = constructorToValidator(s.item, ctx) as any;
        const pipes: Record<string, any> = buildPipelineResult(
          (s as any).pipe ?? [],
        );

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
        };
      }
      case "optional": {
        const s = schema as v.OptionalSchema<any, any>;
        return constructorToValidator(s.wrapped, ctx);
      }
      case "nullable": {
        const s = schema as v.OptionalSchema<any, any>;
        const wrappedValidator = constructorToValidator(s.wrapped, ctx) as any;

        if (!wrappedValidator) {
          return {
            anyOf: [{ bsonType: "null" }],
          };
        }

        return {
          anyOf: [wrappedValidator, { bsonType: "null" }],
        };
      }
      case "nullish": {
        const s = schema as v.NullishSchema<any, any>;
        const wrappedValidator = constructorToValidator(s.wrapped, ctx) as any;

        if (!wrappedValidator) {
          return {
            anyOf: [{ bsonType: "null" }],
          };
        }

        return {
          anyOf: [wrappedValidator, { bsonType: "null" }],
        };
      }
      case "union": {
        const s = schema as v.UnionSchema<any, any>;
        const anyOf: any[] = [];

        for (const value of s.options) {
          const element = constructorToValidator(value, ctx);
          if (element) {
            anyOf.push(element);
          }
        }

        return {
          anyOf,
        };
      }
      case "variant": {
        const s = schema as v.VariantSchema<any, any, any>;
        const anyOf: any[] = [];

        for (const value of s.options) {
          const element = constructorToValidator(value, ctx);
          if (element) {
            anyOf.push(element);
          }
        }

        return {
          anyOf,
        };
      }
      case "intersect": {
        const s = schema as v.IntersectSchema<any, any>;
        const allOf: any[] = [];

        for (const value of s.options) {
          const element = constructorToValidator(value, ctx);
          if (element) {
            allOf.push(element);
          }
        }

        return {
          allOf,
        };
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
        };
      }
      case "enum": {
        const s = schema as v.EnumSchema<any, any>;

        // Get all the enum values (filtering out the keys in numeric enums)
        const enumValues = s.options.filter(
          (value) => typeof value === "string" || typeof value === "number",
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
        };
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
        };
      }
      case "record": {
        // Record: arbitrary keys validated by `key` schema and values by `value` schema
        const s = schema as v.RecordSchema<any, UnknownSchema, any>;

        // Build validators for key and value
        const keyValidator = constructorToValidator(s.key as any, ctx) as any;
        const valueValidator = constructorToValidator(
          s.value as any,
          ctx,
        ) as any;

        const result: any = {
          bsonType: "object",
          description: s.message ?? `must be a record`,
        };

        // MongoDB doesn't support propertyNames, so we use patternProperties instead
        // If we have a key pattern (regex), use it with patternProperties
        if (keyValidator && keyValidator.pattern) {
          // Use patternProperties with the key pattern to validate both key pattern and value
          result.patternProperties = {
            [keyValidator.pattern]: valueValidator || {},
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
      case "any":
      case "unknown": {
        return;
      }
      case "lazy": {
        const s = schema as v.LazySchema<any>;
        if (ctx.lazyStack.has(s)) {
          return {};
        }
        ctx.lazyStack.add(s);
        try {
          const wrapped = s.getter(undefined);
          return constructorToValidator(wrapped, ctx);
        } finally {
          ctx.lazyStack.delete(s);
        }
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
        };
      }
      case "min_value": {
        const s = schema as v.MinValueAction<any, any, any>;
        // $jsonSchema refuses a non-numeric minimum or maximum at collection creation.
        if (typeof s.requirement !== "number") return;
        return {
          minimum: s.requirement,
        };
      }
      case "max_value": {
        const s = schema as v.MaxValueAction<any, any, any>;
        if (typeof s.requirement !== "number") return;
        return {
          maximum: s.requirement,
        };
      }
      case "gt_value": {
        // MongoDB $jsonSchema follows JSON Schema draft-4, where
        // exclusiveMinimum is a BOOLEAN modifier of `minimum` (not a number).
        const s = schema as v.GtValueAction<any, any, any>;
        if (typeof s.requirement !== "number") return;
        return {
          minimum: s.requirement,
          exclusiveMinimum: true,
        };
      }
      case "lt_value": {
        // Draft-4: exclusiveMaximum is a boolean modifier of `maximum`.
        const s = schema as v.LtValueAction<any, any, any>;
        if (typeof s.requirement !== "number") return;
        return {
          maximum: s.requirement,
          exclusiveMaximum: true,
        };
      }
      case "min_length": {
        const s = schema as v.MinLengthAction<any, any, any>;
        return {
          minLength: s.requirement,
        };
      }
      case "max_length": {
        const s = schema as v.MaxLengthAction<any, any, any>;
        return {
          maxLength: s.requirement,
        };
      }
      case "non_empty": {
        return {
          minLength: 1,
        };
      }
      case "integer": {
        return { multipleOf: 1 };
      }
      case "finite":
      case "check":
      case "url": {
        // Not expressible in MongoDB JSON Schema — Valibot still enforces it at the app layer.
        return;
      }
      default: {
        // Check if requirement is a regex
        if ((schema as v.RegexAction<any, any>).requirement instanceof RegExp) {
          const { source, flags } = (schema as v.RegexAction<any, any>)
            .requirement;
          const untranslatable = flags.replace(/[dgimsu]/g, "");
          if (untranslatable.length > 0) {
            log.warn(
              `Unsupported regex flags: ${untranslatable} for "${schema.type}" schema; the pattern is left to valibot`,
            );
            return;
          }
          const inlineFlags = flags.replace(/[dgu]/g, "");
          return {
            pattern:
              inlineFlags.length > 0 ? `(?${inlineFlags}:${source})` : source,
          };
        }

        log.warn(`Unsupported schema type: ${type} (kind ${kind})`);
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
export function toMongoValidator(
  schema: v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>,
) {
  return {
    $jsonSchema: constructorToValidator(schema),
  };
}
