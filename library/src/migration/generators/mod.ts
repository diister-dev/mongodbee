/**
 * @fileoverview Migration generators system exports
 * 
 * This module provides the main exports for the migration generators system,
 * including templates, generation utilities, and helper functions.
 * 
 * @example
 * ```typescript
 * import { 
 *   generateMigration, 
 *   BuiltInTemplates,
 *   createCollectionTemplate 
 * } from "@diister/mongodbee/migration/generators";
 * 
 * // Generate a migration from a built-in template
 * const result = await generateMigration({
 *   name: "create-users",
 *   template: BuiltInTemplates.CREATE_COLLECTION,
 *   variables: {
 *     collectionName: "users",
 *     seedData: [{ name: "admin", email: "admin@example.com" }]
 *   }
 * });
 * 
 * console.log(`Generated: ${result.filePath}`);
 * ```
 * 
 * @module
 */

// Re-export all types
export type {
  MigrationGeneratorOptions,
  TemplateContext,
  MigrationGenerationResult,
  MigrationTemplate,
} from './templates.ts';

// Re-export enums and constants
export {
  BuiltInTemplates,
  BUILTIN_TEMPLATES,
} from './templates.ts';

// Re-export schemas for validation
export {
  MigrationGeneratorOptionsSchema,
} from './templates.ts';

// Re-export all utility functions
export {
  generateMigration,
  listTemplates,
  getTemplate,
  createCollectionTemplate,
  createTransformTemplate,
} from './templates.ts';

/**
 * Quick migration generator for common use cases
 * 
 * @param name - Migration name
 * @param type - Type of migration to generate
 * @param options - Additional options
 * @returns Promise resolving to generation result
 * 
 * @example
 * ```typescript
 * import { quickGenerate } from "@diister/mongodbee/migration/generators";
 * 
 * // Create a collection migration
 * await quickGenerate("create-posts", "collection", {
 *   collectionName: "posts",
 *   description: "Create posts collection with initial schema"
 * });
 * 
 * // Seed data migration
 * await quickGenerate("seed-admin-user", "seed", {
 *   collectionName: "users",
 *   data: [{ name: "admin", role: "administrator" }]
 * });
 * ```
 */
export async function quickGenerate(
  name: string,
  type: 'collection' | 'seed' | 'transform' | 'empty' | 'custom',
  options: Record<string, unknown> = {}
) {
  const { generateMigration, BuiltInTemplates } = await import('./templates.ts');
  
  const templateMap = {
    collection: BuiltInTemplates.CREATE_COLLECTION,
    seed: BuiltInTemplates.SEED_DATA,
    transform: BuiltInTemplates.TRANSFORM_DATA,
    empty: BuiltInTemplates.EMPTY,
    custom: BuiltInTemplates.CUSTOM,
  };
  
  return await generateMigration({
    name,
    template: templateMap[type],
    variables: options,
  });
}

/**
 * Interactive migration generator (for CLI use)
 * 
 * This function would typically be used by CLI tools to guide users
 * through the migration generation process with prompts.
 * 
 * @param prompts - Function to handle user prompts
 * @returns Promise resolving to generation result
 */
export async function interactiveGenerate(
  prompts: {
    text: (message: string, initial?: string) => Promise<string>;
    select: (message: string, choices: Array<{ title: string; value: string; description?: string }>) => Promise<string>;
    confirm: (message: string, initial?: boolean) => Promise<boolean>;
    multitext: (message: string) => Promise<Record<string, string>>;
  }
) {
  const { generateMigration, listTemplates } = await import('./templates.ts');
  
  // Get migration name
  const name = await prompts.text('Migration name:', '');
  if (!name) throw new Error('Migration name is required');
  
  // Get description
  const description = await prompts.text('Description (optional):', '');
  
  // Select template
  const templates = listTemplates();
  const templateChoices = templates.map(template => ({
    title: template.name,
    value: template.name,
    description: template.description,
  }));
  
  const selectedTemplate = await prompts.select('Select template:', templateChoices);
  const template = templates.find(t => t.name === selectedTemplate);
  
  if (!template) throw new Error('Invalid template selection');
  
  // Get template variables
  const variables: Record<string, unknown> = {};
  
  if (template.requiredVariables) {
    for (const variable of template.requiredVariables) {
      const value = await prompts.text(`${variable} (required):`, '');
      if (!value) throw new Error(`${variable} is required for this template`);
      variables[variable] = value;
    }
  }
  
  if (template.optionalVariables) {
    const wantsOptional = await prompts.confirm('Configure optional variables?', false);
    
    if (wantsOptional) {
      const optionalValues = await prompts.multitext('Optional variables:');
      Object.assign(variables, optionalValues);
    }
  }
  
  // Generate migration
  return await generateMigration({
    name,
    description: description || undefined,
    template: selectedTemplate,
    variables,
  });
}