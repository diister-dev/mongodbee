import type * as v from "./schema.ts";

/**
 * Generic types to represent any Valibot schema or validation
 */
type UnknownSchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;
type UnknownValidation = v.BaseValidation<
  unknown,
  unknown,
  v.BaseIssue<unknown>
>;

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
 * Navigation node that combines schema and context information
 */
export interface NavigationNode {
  /** The schema or validation at this node */
  schema: UnknownSchema | UnknownValidation;
  /** Navigation context for this node */
  context: NavigationContext;
  /** Complete path from root (convenience accessor) */
  get path(): string[];
  /** Current depth in the tree (convenience accessor) */
  get depth(): number;
  /** Parent schema (convenience accessor) */
  get parent(): UnknownSchema | UnknownValidation | undefined;
  /** Key in the parent (convenience accessor) */
  get key(): string | number | undefined;
}

/**
 * Result of a node visit
 */
export interface VisitResult {
  /** If true, continue navigation into children */
  continue: boolean;
  /** Optional data to attach to the node */
  data?: unknown;
}

/**
 * Visitor interface for traversing schemas
 */
export interface SchemaVisitor {
  /**
   * Called for each schema node encountered
   * @param node - The navigation node containing schema and context
   * @returns Visit result
   */
  visitNode(node: NavigationNode): VisitResult;

  /**
   * Called before entering a container node (object, array, union, etc.)
   * @param node - The navigation node for the container
   * @returns Visit result
   */
  enterContainer?(node: NavigationNode): VisitResult;

  /**
   * Called after traversing all children of a container
   * @param node - The navigation node for the container
   */
  exitContainer?(node: NavigationNode): void; /**
   * Called for each validation in a pipe
   * @param node - The navigation node for the validation
   * @returns Visit result
   */

  visitValidation?(node: NavigationNode): VisitResult;
}

/**
 * Recursive navigator for Valibot schemas
 *
 * This class provides an AST-like interface for recursively navigating
 * through Valibot schemas, visiting each node in a controlled manner.
 *  * @example
 * ```typescript
 * const navigator = new SchemaNavigator();
 *
 * // Visitor that collects all schema types
 * const typeCollector: SchemaVisitor = {
 *   visitNode(node) {
 *     console.log(`${' '.repeat(node.depth)}${node.schema.type} at ${node.path.join('.')}`);
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
    initialContext?: Partial<NavigationContext>,
  ): void {
    const context: NavigationContext = {
      path: [],
      depth: 0,
      parent: undefined,
      key: undefined,
      ...initialContext,
    };

    this.visitNodeRecursive(schema, visitor, context);
  } /**
   * Create a navigation node from schema and context
   */

  private createNode(
    schema: UnknownSchema | UnknownValidation,
    context: NavigationContext,
  ): NavigationNode {
    return {
      schema,
      context,
      get path() {
        return context.path;
      },
      get depth() {
        return context.depth;
      },
      get parent() {
        return context.parent;
      },
      get key() {
        return context.key;
      },
    };
  }

  /**
   * Internal recursive method for visiting nodes
   */
  private visitNodeRecursive(
    schema: UnknownSchema | UnknownValidation,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    // Create navigation node
    const node = this.createNode(schema, context);
    // Visit the current node
    const result = visitor.visitNode(node);

    if (!result.continue) {
      return;
    }

    // Process according to schema type
    if (schema.kind === "schema") {
      this.navigateSchema(schema as UnknownSchema, visitor, context);
    } else if (schema.kind === "validation") {
      this.navigateValidation(schema as UnknownValidation, visitor, context);
    }
  } /**
   * Navigate in a schema (kind === "schema")
   */

  private navigateSchema(
    schema: UnknownSchema,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    const { type } = schema;

    // Call enterContainer if it's a container
    if (this.isContainerSchema(type)) {
      const node = this.createNode(schema, context);
      const enterResult = visitor.enterContainer?.(node) ?? { continue: true };
      if (!enterResult.continue) {
        return;
      }
    }

    switch (type) {
      case "object": {
        this.navigateObjectSchema(
          schema as v.ObjectSchema<
            v.ObjectEntries,
            v.ErrorMessage<v.ObjectIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "array": {
        this.navigateArraySchema(
          schema as v.ArraySchema<
            UnknownSchema,
            v.ErrorMessage<v.ArrayIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "union": {
        this.navigateUnionSchema(
          schema as v.UnionSchema<
            v.UnionOptions,
            v.ErrorMessage<v.UnionIssue<v.BaseIssue<unknown>>> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "intersect": {
        this.navigateIntersectSchema(
          schema as v.IntersectSchema<
            v.IntersectOptions,
            v.ErrorMessage<v.IntersectIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "optional": {
        this.navigateOptionalSchema(
          schema as v.OptionalSchema<UnknownSchema, never>,
          visitor,
          context,
        );
        break;
      }
      case "nullable": {
        this.navigateNullableSchema(
          schema as v.NullableSchema<
            UnknownSchema,
            v.ErrorMessage<v.NonNullableIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "nullish": {
        this.navigateNullishSchema(
          schema as v.NullishSchema<
            UnknownSchema,
            v.ErrorMessage<v.NonNullishIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "tuple": {
        this.navigateTupleSchema(
          schema as v.TupleSchema<
            v.TupleItems,
            v.ErrorMessage<v.TupleIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "record": {
        this.navigateRecordSchema(
          schema as v.RecordSchema<
            v.BaseSchema<
              string,
              string | number | symbol,
              v.BaseIssue<unknown>
            >,
            UnknownSchema,
            v.ErrorMessage<v.RecordIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "map": {
        this.navigateMapSchema(
          schema as v.MapSchema<
            UnknownSchema,
            UnknownSchema,
            v.ErrorMessage<v.MapIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      case "set": {
        this.navigateSetSchema(
          schema as v.SetSchema<
            UnknownSchema,
            v.ErrorMessage<v.SetIssue> | undefined
          >,
          visitor,
          context,
        );
        break;
      }
      default: {
        // For schemas with pipes (string, number, etc.)
        this.navigatePipedSchema(schema, visitor, context);
        break;
      }
    }

    // Call exitContainer if it's a container
    if (this.isContainerSchema(type)) {
      const node = this.createNode(schema, context);
      visitor.exitContainer?.(node);
    }
  }

  /**
   * Navigate in a validation (kind === "validation")
   */
  private navigateValidation(
    validation: UnknownValidation,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    const node = this.createNode(validation, context);
    visitor.visitValidation?.(node);
  } /**
   * Navigate in an object schema
   */

  private navigateObjectSchema(
    schema: v.ObjectSchema<
      v.ObjectEntries,
      v.ErrorMessage<v.ObjectIssue> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    for (const [key, childSchema] of Object.entries(schema.entries)) {
      const childContext: NavigationContext = {
        path: [...context.path, key],
        depth: context.depth + 1,
        parent: schema,
        key: key,
      };

      this.visitNodeRecursive(
        childSchema as UnknownSchema,
        visitor,
        childContext,
      );
    }
  }

  /**
   * Navigate in an array schema
   */
  private navigateArraySchema(
    schema: v.ArraySchema<
      UnknownSchema,
      v.ErrorMessage<v.ArrayIssue> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    const childContext: NavigationContext = {
      path: [...context.path, "$[]"],
      depth: context.depth + 1,
      parent: schema,
      key: "$[]",
    };

    this.visitNodeRecursive(schema.item, visitor, childContext);

    // Process array schema pipes if present
    this.navigatePipes(schema, visitor, context);
  }

  /**
   * Navigate in a union schema
   */
  private navigateUnionSchema(
    schema: v.UnionSchema<
      v.UnionOptions,
      v.ErrorMessage<v.UnionIssue<v.BaseIssue<unknown>>> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    schema.options.forEach((option: UnknownSchema, index: number) => {
      const childContext: NavigationContext = {
        path: [...context.path, `$union[${index}]`],
        depth: context.depth + 1,
        parent: schema,
        key: index,
      };

      this.visitNodeRecursive(option, visitor, childContext);
    });
  } /**
   * Navigate in an intersect schema
   */

  private navigateIntersectSchema(
    schema: v.IntersectSchema<
      v.IntersectOptions,
      v.ErrorMessage<v.IntersectIssue> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    schema.options.forEach((option: UnknownSchema, index: number) => {
      const childContext: NavigationContext = {
        path: [...context.path, `$intersect[${index}]`],
        depth: context.depth + 1,
        parent: schema,
        key: index,
      };

      this.visitNodeRecursive(option, visitor, childContext);
    });
  }

  /**
   * Navigate in an optional schema
   */
  private navigateOptionalSchema(
    schema: v.OptionalSchema<UnknownSchema, never>,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    const childContext: NavigationContext = {
      path: context.path, // Same path, optional is transparent
      depth: context.depth,
      parent: schema,
      key: context.key,
    };

    this.visitNodeRecursive(schema.wrapped, visitor, childContext);
  }

  /**
   * Navigate in a nullable schema
   */
  private navigateNullableSchema(
    schema: v.NullableSchema<
      UnknownSchema,
      v.ErrorMessage<v.NonNullableIssue> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    const childContext: NavigationContext = {
      path: context.path,
      depth: context.depth,
      parent: schema,
      key: context.key,
    };

    this.visitNodeRecursive(schema.wrapped, visitor, childContext);
  }

  /**
   * Navigate in a nullish schema
   */
  private navigateNullishSchema(
    schema: v.NullishSchema<
      UnknownSchema,
      v.ErrorMessage<v.NonNullishIssue> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    const childContext: NavigationContext = {
      path: context.path,
      depth: context.depth,
      parent: schema,
      key: context.key,
    };

    this.visitNodeRecursive(schema.wrapped, visitor, childContext);
  }

  /**
   * Navigate in a tuple schema
   */
  private navigateTupleSchema(
    schema: v.TupleSchema<
      v.TupleItems,
      v.ErrorMessage<v.TupleIssue> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    schema.items.forEach((item: UnknownSchema, index: number) => {
      const childContext: NavigationContext = {
        path: [...context.path, index.toString()],
        depth: context.depth + 1,
        parent: schema,
        key: index,
      };

      this.visitNodeRecursive(item, visitor, childContext);
    });
  }

  /**
   * Navigate in a record schema
   */
  private navigateRecordSchema(
    schema: v.RecordSchema<
      v.BaseSchema<string, string | number | symbol, v.BaseIssue<unknown>>,
      UnknownSchema,
      v.ErrorMessage<v.RecordIssue> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    // Navigate in the key schema
    const keyContext: NavigationContext = {
      path: [...context.path, "$key"],
      depth: context.depth + 1,
      parent: schema,
      key: "$key",
    };
    this.visitNodeRecursive(schema.key, visitor, keyContext);

    // Navigate in the value schema
    const valueContext: NavigationContext = {
      path: [...context.path, "$value"],
      depth: context.depth + 1,
      parent: schema,
      key: "$value",
    };
    this.visitNodeRecursive(schema.value, visitor, valueContext);
  }

  /**
   * Navigate in a map schema
   */
  private navigateMapSchema(
    schema: v.MapSchema<
      UnknownSchema,
      UnknownSchema,
      v.ErrorMessage<v.MapIssue> | undefined
    >,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    // Navigate in the key schema
    const keyContext: NavigationContext = {
      path: [...context.path, "$key"],
      depth: context.depth + 1,
      parent: schema,
      key: "$key",
    };
    this.visitNodeRecursive(schema.key, visitor, keyContext);

    // Navigate in the value schema
    const valueContext: NavigationContext = {
      path: [...context.path, "$value"],
      depth: context.depth + 1,
      parent: schema,
      key: "$value",
    };
    this.visitNodeRecursive(schema.value, visitor, valueContext);
  }

  /**
   * Navigate in a set schema
   */
  private navigateSetSchema(
    schema: v.SetSchema<UnknownSchema, v.ErrorMessage<v.SetIssue> | undefined>,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    const childContext: NavigationContext = {
      path: [...context.path, "$item"],
      depth: context.depth + 1,
      parent: schema,
      key: "$item",
    };

    this.visitNodeRecursive(schema.value, visitor, childContext);
  }

  /**
   * Navigate in the pipes of a schema (validations)
   */
  private navigatePipedSchema(
    schema: UnknownSchema,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    this.navigatePipes(schema, visitor, context);
  } /**
   * Navigate in the pipes (validations) of a schema
   */

  private navigatePipes(
    schema: UnknownSchema,
    visitor: SchemaVisitor,
    context: NavigationContext,
  ): void {
    const pipes =
      (schema as UnknownSchema & { pipe?: UnknownValidation[] }).pipe;
    if (pipes && Array.isArray(pipes)) {
      pipes.forEach((pipe: UnknownValidation, index: number) => {
        const pipeContext: NavigationContext = {
          path: [...context.path, `$pipe[${index}]`],
          depth: context.depth + 1,
          parent: schema,
          key: `$pipe[${index}]`,
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
      "nullish",
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
  onNode?: (node: NavigationNode) => boolean | void;
  onContainer?: (node: NavigationNode) => boolean | void;
  onValidation?: (node: NavigationNode) => boolean | void;
}): SchemaVisitor {
  return {
    visitNode(node) {
      const result = handlers.onNode?.(node);
      return { continue: result !== false };
    },

    enterContainer(node) {
      const result = handlers.onContainer?.(node);
      return { continue: result !== false };
    },

    visitValidation(node) {
      const result = handlers.onValidation?.(node);
      return { continue: result !== false };
    },
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
export function extractSchemaPaths(
  schema: UnknownSchema,
): Array<{ path: string[]; schema: UnknownSchema | UnknownValidation }> {
  const paths: Array<
    { path: string[]; schema: UnknownSchema | UnknownValidation }
  > = [];
  const navigator = new SchemaNavigator();

  const visitor = createSimpleVisitor({
    onNode: (node) => {
      if (node.path.length > 0 && !node.path.some((p) => p.startsWith("$"))) {
        paths.push({ path: [...node.path], schema: node.schema });
      }
    },
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
  targetPath: string[],
): UnknownSchema | UnknownValidation | undefined {
  let found: UnknownSchema | UnknownValidation | undefined;
  const navigator = new SchemaNavigator();

  const visitor = createSimpleVisitor({
    onNode: (node) => {
      if (
        node.path.length === targetPath.length &&
        node.path.every((p, i) => p === targetPath[i])
      ) {
        found = node.schema;
        return false; // Stop navigation
      }
    },
  });

  navigator.navigate(schema, visitor);
  return found;
}

/**
 * Path element processor result types
 */
type PathProcessorResult = boolean | string;

/**
 * Function to process each path element during path computation
 * @param pathElement - The current path element (string or number)
 * @param schema - The schema/validation at this path element
 * @param context - The navigation context at this element
 * @returns
 *   - true: accept the element as-is
 *   - false: discard this element
 *   - string: override with this value
 */
type PathProcessor = (
  pathElement: string | number,
  schema: UnknownSchema | UnknownValidation,
  context: NavigationContext,
) => PathProcessorResult;

/**
 * Custom dot notation utility that builds paths by traversing up the parent chain
 *
 * This function takes a NavigationContext and rebuilds the path by calling a processor
 * function for each element, allowing filtering and transformation of path components.
 *
 * @param context - The navigation context to start from
 * @param processor - Function to process each path element
 * @returns Array of processed path elements
 *
 * @example
 * ```typescript
 * // Simple path building
 * const path = computePath(context, () => true).join('.');
 *
 * // Filter out array notations
 * const cleanPath = computePath(context, (element) => {
 *   if (element.toString().startsWith('$')) return false;
 *   return true;
 * }).join('.');
 *
 * // Transform pipe validations
 * const transformedPath = computePath(context, (element, schema) => {
 *   if (element.toString().startsWith('$pipe[')) {
 *     return `validation:${schema.type}`;
 *   }
 *   return true;
 * }).join('.');
 *
 * // Complex filtering and renaming
 * const customPath = computePath(context, (element, schema, ctx) => {
 *   // Skip internal notations
 *   if (element.toString().startsWith('$')) return false;
 *
 *   // Rename based on schema type
 *   if (schema.type === 'object' && element === 'data') return 'payload';
 *
 *   return true;
 * }).join('.');
 * ```
 */
export function computePath(
  context: NavigationContext,
  processor: PathProcessor,
): string[] {
  const result: string[] = [];

  // Process each element in the path
  for (let i = 0; i < context.path.length; i++) {
    const pathElement = context.path[i];

    // We need to reconstruct context info for this path element
    // For now, we'll create a minimal context - this could be enhanced
    // to traverse back through the actual schema tree if needed
    const elementContext: NavigationContext = {
      path: context.path.slice(0, i + 1),
      depth: i,
      parent: undefined, // Could be enhanced to get actual parent
      key: pathElement,
    };

    // For the processor, we need the schema at this path element
    // Since we don't have direct access, we'll pass the context's schema info
    // This is a limitation - in a real implementation, we might want to store
    // schema references in the context during navigation
    const currentSchema = context.parent; // This is approximate

    if (currentSchema) {
      const processorResult = processor(
        pathElement,
        currentSchema,
        elementContext,
      );

      if (processorResult === false) {
        // Discard this element
        continue;
      } else if (typeof processorResult === "string") {
        // Override with new value
        result.push(processorResult);
      } else {
        // Accept as-is (true)
        result.push(pathElement.toString());
      }
    } else {
      // Fallback: just process the path element without schema info
      const processorResult = processor(
        pathElement,
        {} as UnknownSchema,
        elementContext,
      );

      if (processorResult === false) {
        continue;
      } else if (typeof processorResult === "string") {
        result.push(processorResult);
      } else {
        result.push(pathElement.toString());
      }
    }
  }

  return result;
}

/**
 * Enhanced version of computePath that reconstructs schema information
 * by re-navigating from the root. This provides accurate schema context
 * for each path element but is less performant.
 *
 * @param rootSchema - The root schema to start navigation from
 * @param targetContext - The target context to build path for
 * @param processor - Function to process each path element
 * @returns Array of processed path elements
 *
 * @example
 * ```typescript
 * const userSchema = v.object({
 *   name: v.string(),
 *   address: v.object({
 *     street: v.pipe(v.string(), v.minLength(1))
 *   })
 * });
 *
 * // During navigation, when you reach a deep context:
 * const path = computePathWithSchema(userSchema, context, (element, schema) => {
 *   // Now you have accurate schema information
 *   if (schema.type === 'string' && element === 'street') {
 *     return 'road'; // rename street to road
 *   }
 *   return true;
 * }).join('.');
 * ```
 */
export function computePathWithSchema(
  rootSchema: UnknownSchema,
  targetContext: NavigationContext,
  processor: PathProcessor,
): string[] {
  const result: string[] = [];
  const navigator = new SchemaNavigator();

  // Navigate to build accurate schema context for each path element
  const pathSchemas: Array<
    {
      element: string | number;
      schema: UnknownSchema | UnknownValidation;
      context: NavigationContext;
    }
  > = [];
  const visitor = createSimpleVisitor({
    onNode: (node) => {
      // Check if this context path is a prefix of our target path
      if (node.path.length <= targetContext.path.length) {
        const isPrefix = node.path.every((
          element: string | number,
          index: number,
        ) => element === targetContext.path[index]);
        if (isPrefix && node.path.length > 0) {
          pathSchemas.push({
            element: node.path[node.path.length - 1],
            schema: node.schema,
            context: { ...node.context },
          });
        }
      }
    },
  });

  navigator.navigate(rootSchema, visitor);

  // Process each path element with its accurate schema
  for (const { element, schema, context } of pathSchemas) {
    const processorResult = processor(element, schema, context);

    if (processorResult === false) {
      continue;
    } else if (typeof processorResult === "string") {
      result.push(processorResult);
    } else {
      result.push(element.toString());
    }
  }

  return result;
}

/**
 * Predefined path processors for common use cases
 */
export const PathProcessors = {
  /**
   * Accept all path elements as-is
   */
  identity: (): PathProcessor => () => true,

  /**
   * Filter out internal schema notations (elements starting with $)
   */
  cleanPath: (): PathProcessor => (element) => {
    return !element.toString().startsWith("$");
  },

  /**
   * Keep only object property paths (filter arrays, unions, etc.)
   */
  propertiesOnly: (): PathProcessor => (element) => {
    const str = element.toString();
    return !str.startsWith("$") && !str.match(/^\d+$/);
  },

  /**
   * Transform array notations to bracket syntax
   */
  arrayBrackets: (): PathProcessor => (element) => {
    if (element === "$[]") return "[]";
    return true;
  },
  /**
   * Custom processor with options
   */
  custom: (options: {
    skipInternal?: boolean;
    transformArrays?: boolean;
    renameMap?: Record<string, string>;
  }): PathProcessor =>
  (element, _schema, _context) => {
    const str = element.toString();

    // Skip internal notations
    if (options.skipInternal && str.startsWith("$")) {
      return false;
    }

    // Transform arrays
    if (options.transformArrays && element === "$[]") {
      return "[]";
    }

    // Apply rename map
    if (options.renameMap && str in options.renameMap) {
      return options.renameMap[str];
    }

    return true;
  },
};

/**
 * Utility function to sanitize path names
 * This function removes leading/trailing whitespace, replaces spaces with underscores,
 * and removes any invalid characters, leaving only alphanumeric characters, underscores, and dots.
 * @param path - The path name to sanitize
 * @return Sanitized path name
 */
export function sanitizePathName(path: string): string {
  // Remove any leading/trailing whitespace
  path = path.trim();

  // Replace spaces with underscores
  path = path.replace(/\s+/g, "_");

  // Remove any invalid characters (keep alphanumeric, underscores, and dots)
  path = path.replace(/[^a-zA-Z0-9_.]/g, "");

  return path;
}
