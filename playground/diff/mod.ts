import * as v from "valibot"

const cleanupProperties = [
    "~standard",
    "async",
    "expects",
    "message",
    "default",
]

const cleanupKind = [
    "transformation",
    "metadata",
]

const simplifyHandlers = {
    "map": (schema: v.MapSchema<any, any, any>) => {
        return {
            ...schema,
            key: simplifySchema(schema.key),
            value: simplifySchema(schema.value),
        }
    },
    "record": (schema: v.RecordSchema<any, any, any>) => {
        return {
            ...schema,
            key: simplifySchema(schema.key),
            value: simplifySchema(schema.value),
        }
    },
    "set": (schema: v.SetSchema<any, any>) => {
        return {
            ...schema,
            value: simplifySchema(schema.value),
        }
    },
    "object": (schema: v.ObjectSchema<any, any>) => {
        return {
            ...schema,
            entries: Object.fromEntries(
                Object.entries(schema.entries).map(
                    ([key, value]) => [key, simplifySchema(value)]
                )
            )
        }
    },
    "loose_object": (schema: v.LooseObjectSchema<any, any>) => {
        return simplifyHandlers["object"](schema as unknown as v.ObjectSchema<any, any>);
    },
    "object_with_rest": (schema: v.ObjectWithRestSchema<any, any, any>) => {
        return {
            ...simplifyHandlers["object"](schema as unknown as v.ObjectSchema<any, any>),
            rest: simplifySchema(schema.rest),
        };
    },
    "strict_object": (schema: v.StrictObjectSchema<any, any>) => {
        return simplifyHandlers["object"](schema as unknown as v.ObjectSchema<any, any>);
    },
    "array": (schema: v.ArraySchema<any, any>) => {
        return {
            ...schema,
            item: simplifySchema(schema.item),
        }
    },
    "tuple": (schema: v.TupleSchema<any, any>) => {
        return {
            ...schema,
            items: schema.items.map((s: any) => simplifySchema(s)),
        }
    },
    "loose_tuple": (schema: v.LooseTupleSchema<any, any>) => {
        return simplifyHandlers["tuple"](schema as unknown as v.TupleSchema<any, any>);
    },
    "strict_tuple": (schema: v.StrictTupleSchema<any, any>) => {
        return simplifyHandlers["tuple"](schema as unknown as v.TupleSchema<any, any>);
    },
    "tuple_with_rest": (schema: v.TupleWithRestSchema<any, any, any>) => {
        return {
            ...simplifyHandlers["tuple"](schema as unknown as v.TupleSchema<any, any>),
            rest: simplifySchema(schema.rest),
        };
    },
    "union": (schema: v.UnionSchema<any, any>) => {
        return schema.options.map((s: any) => simplifySchema(s));
    },
    "intersect": (schema: v.IntersectSchema<any, any>) => {
        return {
            ...schema,
            options: schema.options.map((s: any) => simplifySchema(s)),
        }
    },
    "variant": (schema: v.VariantSchema<any, any, any>) => {
        return {
            ...schema,
            options: schema.options.map((s: any) => simplifySchema(s)),
        }
    },
    "#wrapped": (schema: any) => {
        return {
            ...schema,
            wrapped: simplifySchema(schema.wrapped),
        }
    },
    "optional": (schema: v.OptionalSchema<any, any>) => {
        return simplifyHandlers["#wrapped"](schema);
    },
    "non_optional": (schema: v.NonOptionalSchema<any, any>) => {
        return simplifyHandlers["#wrapped"](schema);
    },
    "undefinedable": (schema: v.UndefinedableSchema<any, any>) => {
        return simplifyHandlers["#wrapped"](schema);
    },
    "nullable": (schema: v.NullableSchema<any, any>) => {
        return simplifyHandlers["#wrapped"](schema);
    },
    "non_nullable": (schema: v.NonNullableSchema<any, any>) => {
        return simplifyHandlers["#wrapped"](schema);
    },
    "nullish": (schema: v.NullishSchema<any, any>) => {
        return simplifyHandlers["#wrapped"](schema);
    },
    "non_nullish": (schema: v.NonNullishSchema<any, any>) => {
        return simplifyHandlers["#wrapped"](schema);
    },
    "exact_optional": (schema: v.ExactOptionalSchema<any, any>) => {
        return simplifyHandlers["#wrapped"](schema);
    }
}

function simplifySchema(schema: any): any {
    if (cleanupKind.includes(schema.kind)) {
        return undefined;
    }

    for(const prop of cleanupProperties) {
        if(prop in schema) {
            delete schema[prop];
        }
    }

    if("pipe" in schema && Array.isArray(schema.pipe)) {
        schema.pipe = schema.pipe
            .map((s: any) => simplifySchema(s))
            .filter((s: any) => !!s);
    }

    const handler = simplifyHandlers[schema.type as keyof typeof simplifyHandlers];
    if(handler) {
        return handler(schema);
    }

    return schema;
}

function flatten(schema: any): any {
    const obj: any = {};
    const keys = Object.keys(schema);
    for(const key of keys) {
        const value = schema[key];
        if (Array.isArray(value)) {
            for (const [index, subValue] of value.entries()) {
                if (typeof subValue === "object") {
                    for (const [subKey, subSubValue] of Object.entries(flatten(subValue))) {
                        obj[`${key}[${index}].${subKey}`] = subSubValue;
                    }
                    continue;
                }
                obj[`${key}[${index}]`] = subValue;
            }
            continue;
        }
        else if(typeof value === "object") {
            for(const [subKey, subValue] of Object.entries(flatten(value))) {
                obj[`${key}.${subKey}`] = subValue;
            }
            continue;
        }

        obj[key] = value;
    }
    return obj;
}

function diffSchemas(schemaA: any, schemaB: any): any {
    const diffs: any = [];
    const keys = new Set([...Object.keys(schemaA), ...Object.keys(schemaB)]);
    for(const key of keys) {
        if (key.endsWith("kind")) {
            continue;
        }
        const valA = schemaA[key];
        const valB = schemaB[key];
        if (JSON.stringify(valA) !== JSON.stringify(valB)) {
            diffs.push({
                key,
                ...(key in schemaA ? { before: valA } : {}),
                ...(key in schemaB ? { after: valB } : {}),
            });
        }
    }
    return diffs;
}

function printSchemaDiff(diffResults: any) {
    if (diffResults.length === 0) {
        console.log('%c✓ No differences', 'color: green; font-weight: bold');
        return;
    }

    for (const diff of diffResults) {
        const hasBefore = 'before' in diff;
        const hasAfter = 'after' in diff;

        if (!hasBefore && hasAfter) {
            // Ajout
            console.log('%c+ %s%c = %s', 'color: green; font-weight: bold', diff.key, 'color: green', JSON.stringify(diff.after));
        } else if (hasBefore && !hasAfter) {
            // Suppression
            console.log('%c- %s%c = %s', 'color: red; font-weight: bold', diff.key, 'color: red', JSON.stringify(diff.before));
        } else {
            // Modification
            console.log('%c~ %s', 'color: orange; font-weight: bold', diff.key);
            console.log('%c  - %s', 'color: red', JSON.stringify(diff.before));
            console.log('%c  + %s', 'color: green', JSON.stringify(diff.after));
        }
    }
}

const schemaA = v.object({
    constraints: v.array(v.union([
        v.object({
            type: v.literal("min"),
            value: v.pipe(v.number(), v.minValue(0)),
        }),
        v.object({
            type: v.literal("max_value"),
            value: v.number(),
        }),
    ]))
})

const simplified = simplifySchema(schemaA);
const flattened = flatten(simplified);

const schemaB = v.object({
    constraints: v.array(v.union([
        v.object({
            type: v.literal("min"),
            value: v.number(),
        }),
        v.object({
            type: v.literal("max"),
            value: v.number(),
        }),
    ]))
})

console.log(JSON.stringify(flattened, null, 2));

const simplifiedB = simplifySchema(schemaB);
const flattenedB = flatten(simplifiedB);
console.log(JSON.stringify(flattenedB, null, 2));

console.log("----------------------");
const diffs = diffSchemas(flattened, flattenedB);
console.log(JSON.stringify(diffs, null, 2));

printSchemaDiff(diffs);