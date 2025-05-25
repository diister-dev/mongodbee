import type * as v from './schema.ts';

/**
 * Generic types to represent any Valibot schema or validation
 */
type UnknownSchema = v.BaseSchema<any, any, any>;
type UnknownValidation = v.BaseValidation<any, any, any>;

/**
 * Navigation context containing information about the traversed path
 */
export interface NavigationContext {
    /** Complete path from root (e.g., ["user", "address", "street"]) */
    path: string[];
    /** Current depth in the tree */
    depth: number;
    /** Parent schema (undefined for root) */
    parent?: UnknownSchema | UnknownValidation;
    /** Key in the parent (undefined for root) */
    key?: string | number;
}

/**
 * Result of a node visit
 */
export interface VisitResult {
    /** If true, continue navigation into children */
    continue: boolean;
    /** Optional data to attach to the node */
    data?: any;
}

/**
 * Visitor interface for traversing schemas
 */
export interface SchemaVisitor {
    /**
     * Called for each schema node encountered
     * @param schema - The current schema or validation
     * @param context - Navigation context
     * @returns Visit result
     */
    visitNode(schema: UnknownSchema | UnknownValidation, context: NavigationContext): VisitResult;

    /**
     * Called before entering a container node (object, array, union, etc.)
     * @param schema - The container schema
     * @param context - Navigation context
     * @returns Visit result
     */
    enterContainer?(schema: UnknownSchema, context: NavigationContext): VisitResult;    /**
     * Called after traversing all children of a container
     * @param schema - The container schema
     * @param context - Navigation context
     */
    exitContainer?(schema: UnknownSchema, context: NavigationContext): void;

    /**
     * Called for each validation in a pipe
     * @param validation - The validation
     * @param context - Navigation context
     * @returns Visit result
     */
    visitValidation?(validation: UnknownValidation, context: NavigationContext): VisitResult;
}

/**
 * Recursive navigator for Valibot schemas
 * 
 * This class provides an AST-like interface for recursively navigating
 * through Valibot schemas, visiting each node in a controlled manner.
 * 
 * @example
 * ```typescript
 * const navigator = new SchemaNavigator();
 * 
 * // Visitor that collects all schema types
 * const typeCollector: SchemaVisitor = {
 *   visitNode(schema, context) {
 *     console.log(`${' '.repeat(context.depth)}${schema.type} at ${context.path.join('.')}`);
 *     return { continue: true };
 *   }
 * };
 * 
 * const userSchema = v.object({
 *   name: v.string(),
 *   age: v.pipe(v.number(), v.minValue(0)),
 *   address: v.object({
 *     street: v.string(),
 *     city: v.string()
 *   })
 * });
 * 
 * navigator.navigate(userSchema, typeCollector);
 * ```
 */
export class SchemaNavigator {
    /**
     * Navigate recursively through a Valibot schema
     * 
     * @param schema - The root schema to traverse
     * @param visitor - The visitor to use for each node
     * @param initialContext - Optional initial context
     */
    navigate(
        schema: UnknownSchema | UnknownValidation,
        visitor: SchemaVisitor,
        initialContext?: Partial<NavigationContext>
    ): void {
        const context: NavigationContext = {
            path: [],
            depth: 0,
            parent: undefined,
            key: undefined,
            ...initialContext
        };

        this.visitNodeRecursive(schema, visitor, context);
    }

    /**
     * Internal recursive method for visiting nodes
     */
    private visitNodeRecursive(
        schema: UnknownSchema | UnknownValidation,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        // Visiter le nœud actuel
        const result = visitor.visitNode(schema, context);

        if (!result.continue) {
            return;
        }

        const { kind, type } = schema;

        // Traiter selon le type de schéma
        if (kind === "schema") {
            this.navigateSchema(schema as UnknownSchema, visitor, context);
        } else if (kind === "validation") {
            this.navigateValidation(schema as UnknownValidation, visitor, context);
        }
    }

    /**
     * Navigate in a schema (kind === "schema")
     */
    private navigateSchema(
        schema: UnknownSchema,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        const { type } = schema;

        // Call enterContainer if it's a container
        if (this.isContainerSchema(type)) {
            const enterResult = visitor.enterContainer?.(schema, context) ?? { continue: true };
            if (!enterResult.continue) {
                return;
            }
        }

        switch (type) {
            case "object": {
                this.navigateObjectSchema(schema as v.ObjectSchema<any, any>, visitor, context);
                break;
            }
            case "array": {
                this.navigateArraySchema(schema as v.ArraySchema<any, any>, visitor, context);
                break;
            }
            case "union": {
                this.navigateUnionSchema(schema as v.UnionSchema<any, any>, visitor, context);
                break;
            }
            case "intersect": {
                this.navigateIntersectSchema(schema as v.IntersectSchema<any, any>, visitor, context);
                break;
            }
            case "optional": {
                this.navigateOptionalSchema(schema as v.OptionalSchema<any, any>, visitor, context);
                break;
            }
            case "nullable": {
                this.navigateNullableSchema(schema as v.NullableSchema<any, any>, visitor, context);
                break;
            }
            case "nullish": {
                this.navigateNullishSchema(schema as v.NullishSchema<any, any>, visitor, context);
                break;
            }
            case "tuple": {
                this.navigateTupleSchema(schema as v.TupleSchema<any, any>, visitor, context);
                break;
            }
            case "record": {
                this.navigateRecordSchema(schema as v.RecordSchema<any, any, any>, visitor, context);
                break;
            }
            case "map": {
                this.navigateMapSchema(schema as v.MapSchema<any, any, any>, visitor, context);
                break;
            }
            case "set": {
                this.navigateSetSchema(schema as v.SetSchema<any, any>, visitor, context);
                break;
            } default: {
                // For schemas with pipes (string, number, etc.)
                this.navigatePipedSchema(schema, visitor, context);
                break;
            }
        }

        // Call exitContainer if it's a container
        if (this.isContainerSchema(type)) {
            visitor.exitContainer?.(schema, context);
        }
    }

    /**
     * Navigate in a validation (kind === "validation")
     */
    private navigateValidation(
        validation: UnknownValidation,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        visitor.visitValidation?.(validation, context);
    }

    /**
     * Navigate in an object schema
     */
    private navigateObjectSchema(
        schema: v.ObjectSchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        for (const [key, childSchema] of Object.entries(schema.entries)) {
            const childContext: NavigationContext = {
                path: [...context.path, key],
                depth: context.depth + 1,
                parent: schema,
                key: key
            };

            this.visitNodeRecursive(childSchema as UnknownSchema, visitor, childContext);
        }
    }

    /**
     * Navigate in an array schema
     */
    private navigateArraySchema(
        schema: v.ArraySchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        const childContext: NavigationContext = {
            path: [...context.path, "$[]"],
            depth: context.depth + 1,
            parent: schema,
            key: "$[]"
        };

        this.visitNodeRecursive(schema.item, visitor, childContext);

        // Process array schema pipes if present
        this.navigatePipes(schema, visitor, context);
    }

    /**
     * Navigate in a union schema
     */
    private navigateUnionSchema(
        schema: v.UnionSchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        schema.options.forEach((option: UnknownSchema, index: number) => {
            const childContext: NavigationContext = {
                path: [...context.path, `$union[${index}]`],
                depth: context.depth + 1,
                parent: schema,
                key: index
            };

            this.visitNodeRecursive(option, visitor, childContext);
        });
    }

    /**
     * Navigate in an intersect schema
     */
    private navigateIntersectSchema(
        schema: v.IntersectSchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        schema.options.forEach((option: UnknownSchema, index: number) => {
            const childContext: NavigationContext = {
                path: [...context.path, `$intersect[${index}]`],
                depth: context.depth + 1,
                parent: schema,
                key: index
            };

            this.visitNodeRecursive(option, visitor, childContext);
        });
    }

    /**
     * Navigate in an optional schema
     */
    private navigateOptionalSchema(
        schema: v.OptionalSchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        const childContext: NavigationContext = {
            path: context.path, // Same path, optional is transparent
            depth: context.depth,
            parent: schema,
            key: context.key
        };

        this.visitNodeRecursive(schema.wrapped, visitor, childContext);
    }

    /**
     * Navigate in a nullable schema
     */
    private navigateNullableSchema(
        schema: v.NullableSchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        const childContext: NavigationContext = {
            path: context.path,
            depth: context.depth,
            parent: schema,
            key: context.key
        };

        this.visitNodeRecursive(schema.wrapped, visitor, childContext);
    }

    /**
     * Navigate in a nullish schema
     */
    private navigateNullishSchema(
        schema: v.NullishSchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        const childContext: NavigationContext = {
            path: context.path,
            depth: context.depth,
            parent: schema,
            key: context.key
        };

        this.visitNodeRecursive(schema.wrapped, visitor, childContext);
    }

    /**
     * Navigate in a tuple schema
     */
    private navigateTupleSchema(
        schema: v.TupleSchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        schema.items.forEach((item: UnknownSchema, index: number) => {
            const childContext: NavigationContext = {
                path: [...context.path, index.toString()],
                depth: context.depth + 1,
                parent: schema,
                key: index
            };

            this.visitNodeRecursive(item, visitor, childContext);
        });
    }

    /**
     * Navigate in a record schema
     */
    private navigateRecordSchema(
        schema: v.RecordSchema<any, any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        // Navigate in the key schema
        const keyContext: NavigationContext = {
            path: [...context.path, "$key"],
            depth: context.depth + 1,
            parent: schema,
            key: "$key"
        };
        this.visitNodeRecursive(schema.key, visitor, keyContext);

        // Navigate in the value schema
        const valueContext: NavigationContext = {
            path: [...context.path, "$value"],
            depth: context.depth + 1,
            parent: schema,
            key: "$value"
        };
        this.visitNodeRecursive(schema.value, visitor, valueContext);
    }

    /**
     * Navigate in a map schema
     */
    private navigateMapSchema(
        schema: v.MapSchema<any, any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        // Navigate in the key schema
        const keyContext: NavigationContext = {
            path: [...context.path, "$key"],
            depth: context.depth + 1,
            parent: schema,
            key: "$key"
        };
        this.visitNodeRecursive(schema.key, visitor, keyContext);

        // Navigate in the value schema
        const valueContext: NavigationContext = {
            path: [...context.path, "$value"],
            depth: context.depth + 1,
            parent: schema,
            key: "$value"
        };
        this.visitNodeRecursive(schema.value, visitor, valueContext);
    }

    /**
     * Navigate in a set schema
     */
    private navigateSetSchema(
        schema: v.SetSchema<any, any>,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        const childContext: NavigationContext = {
            path: [...context.path, "$item"],
            depth: context.depth + 1,
            parent: schema,
            key: "$item"
        };

        this.visitNodeRecursive(schema.value, visitor, childContext);
    }

    /**
     * Navigate in the pipes of a schema (validations)
     */
    private navigatePipedSchema(
        schema: UnknownSchema,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        this.navigatePipes(schema, visitor, context);
    }

    /**
     * Navigate in the pipes (validations) of a schema
     */
    private navigatePipes(
        schema: UnknownSchema,
        visitor: SchemaVisitor,
        context: NavigationContext
    ): void {
        const pipes = (schema as any).pipe;
        if (pipes && Array.isArray(pipes)) {
            pipes.forEach((pipe: UnknownValidation, index: number) => {
                const pipeContext: NavigationContext = {
                    path: [...context.path, `$pipe[${index}]`],
                    depth: context.depth + 1,
                    parent: schema,
                    key: `$pipe[${index}]`
                };

                this.visitNodeRecursive(pipe, visitor, pipeContext);
            });
        }
    }

    /**
     * Check if a schema type is a container (can have children)
     */
    private isContainerSchema(type: string): boolean {
        return [
            "object",
            "array",
            "union",
            "intersect",
            "tuple",
            "record",
            "map",
            "set",
            "optional",
            "nullable",
            "nullish"
        ].includes(type);
    }
}

/**
 * Utility function to create simple visitors
 * 
 * @example
 * ```typescript
 * const visitor = createSimpleVisitor({
 *   onNode: (schema, context) => {
 *     console.log(`Visiting ${schema.type} at ${context.path.join('.')}`);
 *   },
 *   onValidation: (validation, context) => {
 *     console.log(`Validation ${validation.type} at ${context.path.join('.')}`);
 *   }
 * });
 * ```
 */
export function createSimpleVisitor(handlers: {
    onNode?: (schema: UnknownSchema | UnknownValidation, context: NavigationContext) => boolean | void;
    onContainer?: (schema: UnknownSchema, context: NavigationContext) => boolean | void;
    onValidation?: (validation: UnknownValidation, context: NavigationContext) => boolean | void;
}): SchemaVisitor {
    return {
        visitNode(schema, context) {
            const result = handlers.onNode?.(schema, context);
            return { continue: result !== false };
        },

        enterContainer(schema, context) {
            const result = handlers.onContainer?.(schema, context);
            return { continue: result !== false };
        },

        visitValidation(validation, context) {
            const result = handlers.onValidation?.(validation, context);
            return { continue: result !== false };
        }
    };
}

/**
 * Utility function to extract all paths from a schema
 * 
 * @param schema - The schema to analyze
 * @returns Array of found paths with their corresponding schemas
 * 
 * @example
 * ```typescript
 * const userSchema = v.object({
 *   name: v.string(),
 *   address: v.object({
 *     street: v.string(),
 *     city: v.string()
 *   })
 * });
 * 
 * const paths = extractSchemaPaths(userSchema);
 * // Returns:
 * // [
 * //   { path: ["name"], schema: StringSchema },
 * //   { path: ["address"], schema: ObjectSchema },
 * //   { path: ["address", "street"], schema: StringSchema },
 * //   { path: ["address", "city"], schema: StringSchema }
 * // ]
 * ```
 */
export function extractSchemaPaths(schema: UnknownSchema): Array<{ path: string[], schema: UnknownSchema | UnknownValidation }> {
    const paths: Array<{ path: string[], schema: UnknownSchema | UnknownValidation }> = [];
    const navigator = new SchemaNavigator();

    const visitor = createSimpleVisitor({
        onNode: (schema, context) => {
            if (context.path.length > 0 && !context.path.some(p => p.startsWith('$'))) {
                paths.push({ path: [...context.path], schema });
            }
        }
    });

    navigator.navigate(schema, visitor);
    return paths;
}

/**
 * Utility function to find a schema at a given path
 * 
 * @param schema - The root schema
 * @param targetPath - The path to search for
 * @returns The found schema or undefined
 * 
 * @example
 * ```typescript
 * const userSchema = v.object({
 *   address: v.object({
 *     street: v.string()
 *   })
 * });
 * 
 * const streetSchema = findSchemaAtPath(userSchema, ["address", "street"]);
 * // Returns the StringSchema for "street"
 * ```
 */
export function findSchemaAtPath(
    schema: UnknownSchema,
    targetPath: string[]
): UnknownSchema | UnknownValidation | undefined {
    let found: UnknownSchema | UnknownValidation | undefined;
    const navigator = new SchemaNavigator();

    const visitor = createSimpleVisitor({
        onNode: (schema, context) => {
            if (context.path.length === targetPath.length &&
                context.path.every((p, i) => p === targetPath[i])) {
                found = schema;
                return false; // Stop navigation
            }
        }
    });

    navigator.navigate(schema, visitor);
    return found;
}
