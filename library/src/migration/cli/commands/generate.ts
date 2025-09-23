/**
 * Generate command for MongoDBee Migration CLI
 * 
 * Generates new migration files
 * 
 * @module
 */

import { green, yellow, red, dim, blue } from "@std/fmt/colors";
import * as path from "@std/path";
import { existsSync } from "@std/fs";

import { loadConfig } from "../../config/loader.ts";
import { generateMigration } from "../../generators/templates.ts";

export interface GenerateCommandOptions {
  configPath: string;
  environment: string;
  name: string;
  template: string;
}

/**
 * Generate a new migration file
 */
export async function generateCommand(options: GenerateCommandOptions): Promise<void> {
  const { configPath, environment, name, template } = options;

  if (!existsSync(configPath)) {
    console.error(red("Error: Configuration file not found"));
    console.log(dim("Run 'mongodbee-migrate init' to initialize configuration"));
    Deno.exit(1);
  }

  try {
    // Load configuration
    const configResult = await loadConfig({ 
      configPath,
      environment 
    });
    const config = configResult.config;
    
    console.log(yellow(`Generating migration: ${blue(name)}`));
    console.log(dim(`Template: ${template}`));
    console.log(dim(`Environment: ${environment}`));

    // Resolve migrations directory
    const migrationsDir = path.isAbsolute(config.paths.migrations)
      ? config.paths.migrations
      : path.resolve(path.dirname(configPath), config.paths.migrations);

    // Ensure migrations directory exists
    await Deno.mkdir(migrationsDir, { recursive: true });

    // Generate migration file
    const result = await generateMigration({
      name,
      template,
      variables: {
        database: config.database.name,
        collectionName: name.includes('collection') ? name.replace(/^(create-|add-|remove-)?(.+?)(-collection)?$/, '$2') : name,
        timestamp: new Date().toISOString(),
        author: "CLI User",
      },
    }, config);

    // Write the migration to the output directory using the generated ID
    const fileName = `${result.metadata.id}.ts`;
    const filePath = path.join(migrationsDir, fileName);
    
    await Deno.writeTextFile(filePath, result.content);

    console.log(green("✓ Migration generated successfully"));
    console.log(dim(`  File: ${filePath}`));
    console.log(dim(`  Template: ${result.metadata.template}`));

    console.log("\n" + yellow("Next steps:"));
    console.log(dim("1. Edit the migration file to implement your changes"));
    console.log(dim("2. Test the migration:"));
    console.log(dim("   mongodbee-migrate apply --dry-run"));
    console.log(dim("3. Apply the migration:"));
    console.log(dim("   mongodbee-migrate apply"));

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red("Error generating migration:"), message);
    
    if (message.includes("Unknown template")) {
      console.log("\n" + yellow("Available templates:"));
      console.log(dim("• empty - Empty migration"));
      console.log(dim("• create-collection - Create a new collection"));
      console.log(dim("• seed-data - Add initial data"));
      console.log(dim("• transform-data - Transform existing data"));
      console.log(dim("• add-index - Add database indexes"));
    }
    
    Deno.exit(1);
  }
}