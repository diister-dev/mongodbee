/**
 * @fileoverview Migration file generators for scaffolding new migrations
 * 
 * This module provides utilities for generating migration files with templates,
 * proper naming conventions, and boilerplate code. Generators use a functional
 * approach and integrate with the migration system configuration.
 * 
 * @example
 * ```typescript
 * import { generateMigration, createCollectionTemplate } from "@diister/mongodbee/migration/generators";
 * 
 * // Generate a new migration file
 * const migrationFile = await generateMigration({
 *   name: "create-users-collection",
 *   description: "Creates users collection with initial data",
 *   template: createCollectionTemplate("users", userSchema)
 * });
 * 
 * console.log(`Generated: ${migrationFile.path}`);
 * ```
 * 
 * @module
 */

import * as v from '../../schema.ts';
import type { MigrationSystemConfig } from '../config/types.ts';
import type { MigrationDefinition } from '../types.ts';
import { ulid } from "@std/ulid";

/**
 * Finds the latest migration file in a directory to determine the parent
 */
async function findLatestMigration(migrationsDir: string): Promise<{
  fileName: string;
  importPath: string;
  migrationId: string;
} | null> {
  try {
    const entries = [];
    for await (const entry of Deno.readDir(migrationsDir)) {
      if (entry.isFile && entry.name.endsWith('.ts') && entry.name !== 'mod.ts') {
        entries.push(entry.name);
      }
    }
    
    if (entries.length === 0) {
      return null;
    }
    
    // Sort migration files by name (YYYY-MM-dd-ULID-name format will sort correctly)
    const migrationFiles = entries
      .filter(name => /^\d{4}-\d{2}-\d{2}-[A-Z0-9]+-.*\.ts$/.test(name))
      .sort()
      .reverse(); // Latest first
    
    if (migrationFiles.length === 0) {
      // Fallback: look for any .ts files and sort them
      const fallbackFiles = entries.filter(name => name.endsWith('.ts')).sort().reverse();
      if (fallbackFiles.length === 0) return null;
      
      const fileName = fallbackFiles[0];
      return {
        fileName,
        importPath: `./${fileName}`,
        migrationId: fileName.replace('.ts', ''),
      };
    }
    
    const latestFile = migrationFiles[0];
    // Extract migration ID from filename (remove .ts extension)
    const migrationId = latestFile.replace('.ts', '');
    
    return {
      fileName: latestFile,
      importPath: `./${latestFile}`,
      migrationId,
    };
  } catch {
    return null;
  }
}


/**
 * Configuration for migration generation
 */
export const MigrationGeneratorOptionsSchema = v.object({
  /** Base name for the migration (will be sanitized and timestamped) */
  name: v.string(),
  
  /** Optional description of what the migration does */
  description: v.optional(v.string()),
  
  /** Template to use for generation */
  template: v.optional(v.string()),
  
  /** Parent migration ID (if this migration depends on another) */
  parent: v.optional(v.string()),
  
  /** Custom variables to inject into the template */
  variables: v.optional(v.record(v.string(), v.any())),
  
  /** Whether to create a reversible migration (default: true) */
  reversible: v.optional(v.boolean()),
  
  /** Author information */
  author: v.optional(v.object({
    name: v.optional(v.string()),
    email: v.optional(v.string()),
  })),
});

/**
 * Options for generating a migration
 */
export type MigrationGeneratorOptions = v.InferInput<typeof MigrationGeneratorOptionsSchema>;

/**
 * Template variable context for migration generation
 */
export type TemplateContext = {
  /** Migration metadata */
  migration: {
    id: string;
    name: string;
    description?: string;
    timestamp: string;
    parent?: string;
    author?: {
      name?: string;
      email?: string;
    };
  };
  
  /** Custom variables from options */
  variables: Record<string, unknown>;
  
  /** Helper functions available in templates */
  helpers: {
    /** Current timestamp in ISO format */
    now: () => string;
    
    /** Generate a UUID */
    uuid: () => string;
    
    /** Sanitize a string for use as identifier */
    sanitize: (str: string) => string;
    
    /** Convert string to camelCase */
    camelCase: (str: string) => string;
    
    /** Convert string to PascalCase */
    pascalCase: (str: string) => string;
    
    /** Convert string to snake_case */
    snakeCase: (str: string) => string;
    
    /** Convert string to kebab-case */
    kebabCase: (str: string) => string;
  };

  /** Parent migration information for automatic linking */
  parentInfo?: {
    fileName: string;
    importPath: string;
    migrationId: string;
  } | null;
};

/**
 * Result of migration generation
 */
export type MigrationGenerationResult = {
  /** Generated migration definition */
  definition: MigrationDefinition;
  
  /** Generated file content */
  content: string;
  
  /** Migration metadata */
  metadata: {
    id: string;
    name: string;
    timestamp: string;
    template: string;
    size: number;
  };
};

/**
 * Template information
 */
export type MigrationTemplate = {
  /** Template name/identifier */
  name: string;
  
  /** Human-readable description */
  description: string;
  
  /** Template content with placeholders */
  content: string;
  
  /** Required variables for this template */
  requiredVariables?: string[];
  
  /** Optional variables with defaults */
  optionalVariables?: Record<string, unknown>;
  
  /** Tags for template categorization */
  tags?: string[];
};

/**
 * Built-in template types
 */
export enum BuiltInTemplates {
  EMPTY = 'empty',
  CREATE_COLLECTION = 'create-collection',
  SEED_DATA = 'seed-data',
  TRANSFORM_DATA = 'transform-data',
  ADD_INDEX = 'add-index',
  CUSTOM = 'custom',
}

/**
 * Helper functions for template processing
 */
const templateHelpers = {
  now: () => new Date().toISOString(),
  uuid: () => crypto.randomUUID(),
  sanitize: (str: string) => str.replace(/[^a-zA-Z0-9_]/g, '_').replace(/_+/g, '_'),
  camelCase: (str: string) => str.replace(/[-_\s](.)/g, (_, char) => char.toUpperCase()),
  pascalCase: (str: string) => {
    const camel = templateHelpers.camelCase(str);
    return camel.charAt(0).toUpperCase() + camel.slice(1);
  },
  snakeCase: (str: string) => str.replace(/[-\s]/g, '_').replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase(),
  kebabCase: (str: string) => str.replace(/[_\s]/g, '-').replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase(),
};

/**
 * Built-in migration templates
 */
export const BUILTIN_TEMPLATES: Record<BuiltInTemplates, MigrationTemplate> = {
  [BuiltInTemplates.EMPTY]: {
    name: 'empty',
    description: 'Empty migration template for custom operations',
    content: `/**
 * Migration: {{migration.name}}
 * {{#if migration.description}}Description: {{migration.description}}{{/if}}
 * Generated: {{migration.timestamp}}
 * {{#if migration.author}}Author: {{migration.author.name}}{{#if migration.author.email}} <{{migration.author.email}}>{{/if}}{{/if}}
 */

import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";
{{#if parentInfo}}import parent from "{{parentInfo.importPath}}";{{/if}}

{{#if parentInfo}}const schemas = {
  collections: {
    ...parent.schemas.collections,
    // Add or modify your collection schemas here
    // example: {
    //   ...parent.schemas.collections.example,
    //   newField: v.string(),
    // },
  }
};{{else}}const schemas = {
  collections: {
    // Define your collection schemas here
    // example: v.object({
    //   _id: v.string(),
    //   name: v.string(),
    //   createdAt: v.date(),
    // }),
  }
};{{/if}}

export default migrationDefinition("{{migration.id}}", "{{migration.name}}", {
  {{#if parentInfo}}parent,{{else}}parent: null,{{/if}}
  schemas,
  migrate: (builder) => {
    return builder
      // Add your migration operations here
      // .createCollection("example")
      //   .seed([...])
      //   .done()
      .compile();
  },
});
`,
    tags: ['basic', 'template'],
  },

  [BuiltInTemplates.CREATE_COLLECTION]: {
    name: 'create-collection',
    description: 'Template for creating a new collection with schema',
    content: `/**
 * Migration: {{migration.name}}
 * {{#if migration.description}}Description: {{migration.description}}{{/if}}
 * Generated: {{migration.timestamp}}
 * Creates collection: {{variables.collectionName}}
 */

import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";

// Schema definition for {{variables.collectionName}}
const {{helpers.camelCase variables.collectionName}}Schema = v.object({
  _id: v.string(),
  {{#if variables.schema}}{{variables.schema}}{{else}}// Add your schema fields here
  // name: v.string(),
  // email: v.pipe(v.string(), v.email()),
  // createdAt: v.date(),{{/if}}
});

export default migrationDefinition("{{migration.id}}", "{{migration.name}}", {
  {{#if migration.parent}}parent: {{migration.parent}},{{else}}parent: null,{{/if}}
  schemas: {
    collections: {
      {{variables.collectionName}}: {{helpers.camelCase variables.collectionName}}Schema,
    },
  },
  migrate: (builder) => {
    return builder
      .createCollection("{{variables.collectionName}}")
      {{#if variables.seedData}}.seed({{variables.seedData}}){{/if}}
      .done()
      .compile();
  },
});
`,
    requiredVariables: ['collectionName'],
    optionalVariables: {
      schema: undefined,
      seedData: undefined,
    },
    tags: ['collection', 'create', 'schema'],
  },

  [BuiltInTemplates.SEED_DATA]: {
    name: 'seed-data',
    description: 'Template for seeding data into an existing collection',
    content: `/**
 * Migration: {{migration.name}}
 * {{#if migration.description}}Description: {{migration.description}}{{/if}}
 * Generated: {{migration.timestamp}}
 * Seeds data into: {{variables.collectionName}}
 */

import { migrationBuilder } from "@diister/mongodbee/migration";
import type { MigrationDefinition } from "@diister/mongodbee/migration";

// Seed data for {{variables.collectionName}}
const seedData = {{#if variables.data}}{{variables.data}}{{else}}[
  // Add your seed data here
  // { name: "Example", value: "data" },
]{{/if}};

export const migration: MigrationDefinition = {
  id: "{{migration.id}}",
  name: "{{migration.name}}",
  {{#if migration.parent}}parent: "{{migration.parent}}",{{/if}}
  schemas: {},
  
  migrate: (builder) => {
    return builder
      .collection("{{variables.collectionName}}")
        .seed(seedData)
        .done()
      .compile();
  },
};

export default migration;
`,
    requiredVariables: ['collectionName'],
    optionalVariables: {
      data: undefined,
    },
    tags: ['data', 'seed', 'populate'],
  },

  [BuiltInTemplates.TRANSFORM_DATA]: {
    name: 'transform-data',
    description: 'Template for transforming existing data in a collection',
    content: `/**
 * Migration: {{migration.name}}
 * {{#if migration.description}}Description: {{migration.description}}{{/if}}
 * Generated: {{migration.timestamp}}
 * Transforms data in: {{variables.collectionName}}
 */

import { migrationBuilder } from "@diister/mongodbee/migration";
import type { MigrationDefinition } from "@diister/mongodbee/migration";

export const migration: MigrationDefinition = {
  id: "{{migration.id}}",
  name: "{{migration.name}}",
  {{#if migration.parent}}parent: "{{migration.parent}}",{{/if}}
  schemas: {},
  
  migrate: (builder) => {
    return builder
      .collection("{{variables.collectionName}}")
        .transform({
          up: (document) => {
            // Transform document for forward migration
            {{#if variables.upTransform}}{{variables.upTransform}}{{else}}// Example: return { ...document, newField: "value" };
            return document;{{/if}}
          },
          down: (document) => {
            // Transform document for rollback migration
            {{#if variables.downTransform}}{{variables.downTransform}}{{else}}// Example: const { newField, ...rest } = document; return rest;
            return document;{{/if}}
          },
        })
        .done()
      .compile();
  },
};

export default migration;
`,
    requiredVariables: ['collectionName'],
    optionalVariables: {
      upTransform: undefined,
      downTransform: undefined,
    },
    tags: ['transform', 'data', 'update'],
  },

  [BuiltInTemplates.ADD_INDEX]: {
    name: 'add-index',
    description: 'Template for adding indexes to a collection',
    content: `/**
 * Migration: {{migration.name}}
 * {{#if migration.description}}Description: {{migration.description}}{{/if}}
 * Generated: {{migration.timestamp}}
 * Adds indexes to: {{variables.collectionName}}
 */

import { migrationBuilder } from "@diister/mongodbee/migration";
import type { MigrationDefinition } from "@diister/mongodbee/migration";

export const migration: MigrationDefinition = {
  id: "{{migration.id}}",
  name: "{{migration.name}}",
  {{#if migration.parent}}parent: "{{migration.parent}}",{{/if}}
  schemas: {},
  
  migrate: (builder) => {
    return builder
      .collection("{{variables.collectionName}}")
        // Note: Index operations would be implemented as custom operations
        // This template is a placeholder for index management functionality
        .done()
      .compile();
  },
};

export default migration;
`,
    requiredVariables: ['collectionName'],
    optionalVariables: {
      indexes: undefined,
    },
    tags: ['index', 'performance', 'database'],
  },

  [BuiltInTemplates.CUSTOM]: {
    name: 'custom',
    description: 'Custom template with user-defined content',
    content: `/**
 * Migration: {{migration.name}}
 * {{#if migration.description}}Description: {{migration.description}}{{/if}}
 * Generated: {{migration.timestamp}}
 */

import { migrationBuilder } from "@diister/mongodbee/migration";
import type { MigrationDefinition } from "@diister/mongodbee/migration";

export const migration: MigrationDefinition = {
  id: "{{migration.id}}",
  name: "{{migration.name}}",
  {{#if migration.parent}}parent: "{{migration.parent}}",{{/if}}
  schemas: {},
  
  migrate: (builder) => {
    {{#if variables.content}}{{variables.content}}{{else}}return builder
      // Add your custom migration logic here
      .compile();{{/if}}
  },
};

export default migration;
`,
    optionalVariables: {
      content: undefined,
    },
    tags: ['custom', 'flexible'],
  },
};

/**
 * Simple template engine for processing migration templates
 * 
 * Supports basic variable interpolation with {{variable}} syntax
 * and conditional blocks with {{#if condition}} syntax.
 */
function processTemplate(template: string, context: TemplateContext): string {
  let result = template;
  
  // Process conditional blocks
  result = result.replace(/\{\{#if\s+([^}]+)\}\}([\s\S]*?)\{\{\/if\}\}/g, (_, condition, content) => {
    const value = getNestedValue(context, condition.trim());
    return value ? content : '';
  });
  
  // Process variable interpolations
  result = result.replace(/\{\{([^}]+)\}\}/g, (_, variable) => {
    const trimmed = variable.trim();
    const value = getNestedValue(context, trimmed);
    return value !== undefined ? String(value) : '';
  });
  
  return result;
}

/**
 * Gets a nested value from an object using dot notation
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  return path.split('.').reduce((current: unknown, key: string) => {
    if (current && typeof current === 'object' && key in current) {
      return (current as Record<string, unknown>)[key];
    }
    return undefined;
  }, obj);
}

/**
 * Generates a unique migration ID in format: YYYY-MM-dd-<ULID>-<NAME>
 * This format ensures chronological sorting and uniqueness
 */
function generateMigrationId(name: string): string {
  const date = new Date().toISOString().split('T')[0]; // YYYY-MM-dd
  const migrationUlid = ulid(); // Generate ULID
  const sanitizedName = templateHelpers.kebabCase(name);
  return `${date}-${migrationUlid}-${sanitizedName}`;
}

/**
 * Creates a template context for migration generation
 */
function createTemplateContext(
  options: MigrationGeneratorOptions,
  migrationId: string,
  parentInfo?: {
    fileName: string;
    importPath: string;
    migrationId: string;
  } | null
): TemplateContext {
  return {
    migration: {
      id: migrationId,
      name: options.name,
      description: options.description,
      timestamp: templateHelpers.now(),
      parent: options.parent || (parentInfo ? parentInfo.importPath : undefined),
      author: options.author,
    },
    variables: options.variables || {},
    helpers: templateHelpers,
    // Add parent info to context for template use
    parentInfo: parentInfo ? {
      fileName: parentInfo.fileName,
      importPath: parentInfo.importPath,
      migrationId: parentInfo.migrationId,
    } : null,
  };
}

/**
 * Validates migration generator options
 */
function validateGeneratorOptions(options: MigrationGeneratorOptions): string[] {
  const parseResult = v.safeParse(MigrationGeneratorOptionsSchema, options);
  
  if (!parseResult.success) {
    return parseResult.issues.map(issue => 
      `${issue.path?.map(p => String(p)).join('.') || 'root'}: ${issue.message}`
    );
  }
  
  const errors: string[] = [];
  
  // Validate name format
  if (!/^[a-zA-Z][a-zA-Z0-9-_]*$/.test(options.name)) {
    errors.push('Migration name must start with a letter and contain only letters, numbers, hyphens, and underscores');
  }
  
  // Validate template requirements
  const template = options.template ? BUILTIN_TEMPLATES[options.template as BuiltInTemplates] : null;
  if (template?.requiredVariables) {
    const missing = template.requiredVariables.filter(
      variable => !options.variables || !(variable in options.variables)
    );
    
    if (missing.length > 0) {
      errors.push(`Template requires missing variables: ${missing.join(', ')}`);
    }
  }
  
  return errors;
}

/**
 * Generates a migration file from a template
 * 
 * @param options - Generation options including name, template, and variables
 * @param config - Optional system configuration for file paths
 * @returns Promise resolving to generation result
 * 
 * @example
 * ```typescript
 * const result = await generateMigration({
 *   name: "create-users",
 *   description: "Create users collection",
 *   template: "create-collection",
 *   variables: {
 *     collectionName: "users",
 *     seedData: [{ name: "admin", email: "admin@example.com" }]
 *   }
 * });
 * 
 * console.log(`Generated migration: ${result.filePath}`);
 * ```
 */
/**
 * Generate a migration file with automatic parent detection
 * 
 * This function creates a new migration file with proper parent linking
 * by automatically detecting the latest migration in the migrations directory.
 */
export async function generateMigration(
  options: MigrationGeneratorOptions,
  config?: MigrationSystemConfig
): Promise<MigrationGenerationResult> {
  // Validate options
  const validationErrors = validateGeneratorOptions(options);
  if (validationErrors.length > 0) {
    throw new Error(`Migration generation validation failed:\n${validationErrors.join('\n')}`);
  }
  
  // Generate unique migration ID
  const migrationId = generateMigrationId(options.name);
  
  // Try to find the latest migration automatically for parent detection
  let parentInfo = null;
  if (config?.paths?.migrations) {
    try {
      parentInfo = await findLatestMigration(config.paths.migrations);
    } catch {
      // Ignore errors in parent detection - will default to null
    }
  }
  
  // Select template
  const templateName = (options.template || BuiltInTemplates.EMPTY) as BuiltInTemplates;
  const template = BUILTIN_TEMPLATES[templateName];
  
  if (!template) {
    throw new Error(`Unknown template: ${templateName}`);
  }
  
  // Create template context with parent info
  const context = createTemplateContext(options, migrationId, parentInfo);
  
  // Merge template variables with user variables
  const mergedVariables = {
    ...template.optionalVariables,
    ...options.variables,
  };
  context.variables = mergedVariables;
  
  // Process template
  const content = processTemplate(template.content, context);
  
  // Create migration definition (this would be dynamically imported in practice)
  const definition: MigrationDefinition = {
    id: migrationId,
    name: options.name,
    parent: null, // Would be resolved from parent ID in practice
    schemas: {
      collections: {},
    },
    migrate: () => {
      throw new Error('Generated migration must be imported and executed properly');
    },
  };
  
  return {
    definition,
    content,
    metadata: {
      id: migrationId,
      name: options.name,
      timestamp: context.migration.timestamp,
      template: templateName,
      size: content.length,
    },
  };
}

/**
 * Lists available templates
 * 
 * @returns Array of available template information
 */
export function listTemplates(): MigrationTemplate[] {
  return Object.values(BUILTIN_TEMPLATES);
}

/**
 * Gets a specific template by name
 * 
 * @param name - Template name
 * @returns Template information or undefined if not found
 */
export function getTemplate(name: string): MigrationTemplate | undefined {
  return BUILTIN_TEMPLATES[name as BuiltInTemplates];
}

/**
 * Creates a custom template for collection creation
 * 
 * @param collectionName - Name of the collection to create
 * @param schema - Optional schema definition
 * @param seedData - Optional initial data
 * @returns Custom template string
 */
export function createCollectionTemplate(
  collectionName: string,
  schema?: string,
  seedData?: unknown[]
): string {
  const template = BUILTIN_TEMPLATES[BuiltInTemplates.CREATE_COLLECTION];
  const context = createTemplateContext({
    name: `create-${collectionName}`,
    variables: {
      collectionName,
      schema,
      seedData: seedData ? JSON.stringify(seedData, null, 2) : undefined,
    },
  }, `temp_${Date.now()}`, null);
  
  return processTemplate(template.content, context);
}

/**
 * Creates a custom template for data transformation
 * 
 * @param collectionName - Name of the collection to transform
 * @param upTransform - Forward transformation code
 * @param downTransform - Reverse transformation code
 * @returns Custom template string
 */
export function createTransformTemplate(
  collectionName: string,
  upTransform: string,
  downTransform: string
): string {
  const template = BUILTIN_TEMPLATES[BuiltInTemplates.TRANSFORM_DATA];
  const context = createTemplateContext({
    name: `transform-${collectionName}`,
    variables: {
      collectionName,
      upTransform,
      downTransform,
    },
  }, `temp_${Date.now()}`, null);
  
  return processTemplate(template.content, context);
}