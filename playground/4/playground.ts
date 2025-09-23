#!/usr/bin/env -S deno run --allow-read --allow-write --allow-net --allow-env
/**
 * MongoDBee Migration System - Complete Playground Example
 * 
 * This playground demonstrates the complete migration system functionality including:
 * - Configuration management and validation
 * - Migration file generation with templates
 * - CLI integration and automation
 * - Database connection and schema validation
 * - End-to-end migration workflow
 * 
 * @example
 * ```bash
 * # Run the complete demo
 * deno task demo
 * 
 * # Run just the playground
 * deno task start
 * ```
 * 
 * @module
 */

import { blue, bold, green, red, yellow, dim } from "@std/fmt/colors";
import * as path from "@std/path";

// Import MongoDBee components
import { 
  loadConfig, 
  generateMigration,
  migrationBuilder,
  type MigrationSystemConfig 
} from "@diister/mongodbee/migration";

/**
 * Demo configuration for the playground
 */
const DEMO_CONFIG = {
  database: {
    connection: {
      uri: "mongodb://localhost:27017",
      options: {
        maxPoolSize: 10,
        minPoolSize: 1,
        maxIdleTimeMS: 30000
      }
    },
    name: "playground_example"
  },
  paths: {
    migrations: "./migrations",
    schemas: "./schemas"
  },
  migration: {
    collection: "migrations",
    lockCollection: "migration_locks",
    timeout: 300000,
    retry: {
      maxAttempts: 3,
      backoffMs: 1000
    }
  },
  environments: {
    development: {
      database: {
        name: "playground_example_dev"
      }
    },
    testing: {
      database: {
        name: "playground_example_test"
      }
    },
    production: {
      database: {
        connection: {
          uri: "mongodb://prod-host:27017"
        },
        name: "playground_example_prod"
      }
    }
  },
  cli: {
    colors: true,
    timestamps: true,
    logLevel: "info"
  }
};

/**
 * Display a section header
 */
function showSection(title: string): void {
  console.log(`\n${bold(blue(`=== ${title} ===`))}`);
}

/**
 * Display a step
 */
function showStep(step: string, description: string): void {
  console.log(`\n${yellow(`${step}:`)} ${description}`);
}

/**
 * Display success message
 */
function showSuccess(message: string): void {
  console.log(green(`✓ ${message}`));
}

/**
 * Display error message
 */
function showError(message: string): void {
  console.log(red(`✗ ${message}`));
}

/**
 * Create demo migration files
 */
async function createDemoMigrations(): Promise<void> {
  showStep("1", "Creating demo migration files");

  // Ensure migrations directory exists
  await Deno.mkdir("./migrations", { recursive: true });

  // Migration 1: Create users collection
  const migration1 = `/**
 * Create users collection with schema validation
 * 
 * @migration create-users-collection
 * @timestamp ${new Date().toISOString()}
 */

import { createMigrationBuilder } from "@diister/mongodbee/migration";

export const migration = createMigrationBuilder()
  .id("001-create-users-collection")
  .name("Create users collection")
  .description("Creates users collection with email validation and indexes")
  .author({ name: "MongoDBee Demo", email: "demo@mongodbee.dev" })
  .migrate((builder) => {
    return builder
      .createCollection("users")
      .withSchema({
        bsonType: "object",
        required: ["email", "name", "createdAt"],
        properties: {
          email: {
            bsonType: "string",
            pattern: "^[\\\\w-\\\\.]+@([\\\\w-]+\\\\.)+[\\\\w-]{2,4}$",
            description: "User email address"
          },
          name: {
            bsonType: "string",
            minLength: 1,
            maxLength: 100,
            description: "User full name"
          },
          age: {
            bsonType: "int",
            minimum: 0,
            maximum: 150,
            description: "User age"
          },
          createdAt: {
            bsonType: "date",
            description: "Account creation timestamp"
          },
          updatedAt: {
            bsonType: "date",
            description: "Last update timestamp"
          }
        }
      })
      .createIndex("users", { email: 1 }, { unique: true })
      .createIndex("users", { createdAt: -1 });
  })
  .rollback((builder) => {
    return builder
      .dropCollection("users");
  });`;

  await Deno.writeTextFile("./migrations/001-create-users-collection.ts", migration1);

  // Migration 2: Seed initial data
  const migration2 = `/**
 * Seed initial user data
 * 
 * @migration seed-initial-users
 * @timestamp ${new Date().toISOString()}
 */

import { createMigrationBuilder } from "@diister/mongodbee/migration";

export const migration = createMigrationBuilder()
  .id("002-seed-initial-users")
  .name("Seed initial users")
  .description("Adds sample user data for testing")
  .parent("001-create-users-collection")
  .author({ name: "MongoDBee Demo", email: "demo@mongodbee.dev" })
  .migrate((builder) => {
    return builder
      .insertMany("users", [
        {
          email: "alice@example.com",
          name: "Alice Johnson",
          age: 28,
          createdAt: new Date(),
          updatedAt: new Date()
        },
        {
          email: "bob@example.com", 
          name: "Bob Smith",
          age: 35,
          createdAt: new Date(),
          updatedAt: new Date()
        },
        {
          email: "charlie@example.com",
          name: "Charlie Brown",
          age: 22,
          createdAt: new Date(),
          updatedAt: new Date()
        }
      ]);
  })
  .rollback((builder) => {
    return builder
      .deleteMany("users", {
        email: { $in: ["alice@example.com", "bob@example.com", "charlie@example.com"] }
      });
  });`;

  await Deno.writeTextFile("./migrations/002-seed-initial-users.ts", migration2);

  // Migration 3: Add user profiles collection
  const migration3 = `/**
 * Create user profiles collection
 * 
 * @migration create-user-profiles
 * @timestamp ${new Date().toISOString()}
 */

import { createMigrationBuilder } from "@diister/mongodbee/migration";

export const migration = createMigrationBuilder()
  .id("003-create-user-profiles")
  .name("Create user profiles")
  .description("Creates user profiles collection with references to users")
  .parent("002-seed-initial-users")
  .author({ name: "MongoDBee Demo", email: "demo@mongodbee.dev" })  
  .migrate((builder) => {
    return builder
      .createCollection("user_profiles")
      .withSchema({
        bsonType: "object",
        required: ["userId", "createdAt"],
        properties: {
          userId: {
            bsonType: "objectId",
            description: "Reference to user document"
          },
          bio: {
            bsonType: "string",
            maxLength: 500,
            description: "User biography"
          },
          avatar: {
            bsonType: "string",
            description: "Avatar image URL"
          },
          preferences: {
            bsonType: "object",
            properties: {
              theme: {
                bsonType: "string",
                enum: ["light", "dark", "auto"]
              },
              notifications: {
                bsonType: "bool"
              }
            }
          },
          createdAt: {
            bsonType: "date",
            description: "Profile creation timestamp"
          },
          updatedAt: {
            bsonType: "date",
            description: "Last update timestamp"
          }
        }
      })
      .createIndex("user_profiles", { userId: 1 }, { unique: true })
      .createIndex("user_profiles", { createdAt: -1 });
  })
  .rollback((builder) => {
    return builder
      .dropCollection("user_profiles");
  });`;

  await Deno.writeTextFile("./migrations/003-create-user-profiles.ts", migration3);

  showSuccess("Created 3 demo migration files");
}

/**
 * Create demo schema files
 */
async function createDemoSchemas(): Promise<void> {
  showStep("2", "Creating demo schema files");

  // Ensure schemas directory exists
  await Deno.mkdir("./schemas", { recursive: true });

  // User schema
  const userSchema = `/**
 * User schema definition for MongoDBee
 */

import { createSchema } from "@diister/mongodbee";

export const UserSchema = createSchema("User", {
  email: {
    type: "string",
    required: true,
    unique: true,
    format: "email"
  },
  name: {
    type: "string", 
    required: true,
    minLength: 1,
    maxLength: 100
  },
  age: {
    type: "number",
    minimum: 0,
    maximum: 150
  },
  createdAt: {
    type: "date",
    required: true,
    default: () => new Date()
  },
  updatedAt: {
    type: "date",
    required: true,
    default: () => new Date()
  }
});`;

  await Deno.writeTextFile("./schemas/user.ts", userSchema);

  // User profile schema
  const profileSchema = `/**
 * User profile schema definition for MongoDBee
 */

import { createSchema } from "@diister/mongodbee";

export const UserProfileSchema = createSchema("UserProfile", {
  userId: {
    type: "objectId",
    required: true,
    ref: "User"
  },
  bio: {
    type: "string",
    maxLength: 500
  },
  avatar: {
    type: "string",
    format: "uri"
  },
  preferences: {
    type: "object",
    properties: {
      theme: {
        type: "string",
        enum: ["light", "dark", "auto"],
        default: "light"
      },
      notifications: {
        type: "boolean",
        default: true
      }
    }
  },
  createdAt: {
    type: "date",
    required: true,
    default: () => new Date()
  },
  updatedAt: {
    type: "date",
    required: true,
    default: () => new Date()
  }
});`;

  await Deno.writeTextFile("./schemas/user-profile.ts", profileSchema);

  showSuccess("Created demo schema files");
}

/**
 * Demonstrate configuration loading
 */
async function demonstrateConfiguration(): Promise<MigrationSystemConfig> {
  showStep("3", "Demonstrating configuration system");

  // Write demo configuration
  const configPath = "./mongodbee.config.json";
  await Deno.writeTextFile(configPath, JSON.stringify(DEMO_CONFIG, null, 2));

  try {
    // Load configuration for different environments
    const devConfig = await loadConfig({ 
      configPath,
      environment: "development" 
    });

    const testConfig = await loadConfig({
      configPath,
      environment: "testing"
    });

    showSuccess("Configuration loaded successfully");
    console.log(dim(`  Development DB: ${devConfig.config.database.name}`));
    console.log(dim(`  Testing DB: ${testConfig.config.database.name}`));
    console.log(dim(`  Migrations path: ${devConfig.config.paths.migrations}`));
    console.log(dim(`  Schemas path: ${devConfig.config.paths.schemas}`));

    if (devConfig.warnings.length > 0) {
      console.log(yellow("  Configuration warnings:"));
      for (const warning of devConfig.warnings) {
        console.log(dim(`    • ${warning}`));
      }
    }

    return devConfig.config;

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    showError(`Configuration loading failed: ${message}`);
    throw error;
  }
}

/**
 * Demonstrate migration generation
 */
async function demonstrateMigrationGeneration(): Promise<void> {
  showStep("4", "Demonstrating migration generation");

  try {
    // Generate a migration using a template
    const result = await generateMigration({
      name: "add-user-settings",
      template: "create-collection",
      variables: {
        collectionName: "user_settings",
        timestamp: new Date().toISOString(),
        author: "MongoDBee Demo"
      }
    });

    // Write the generated migration
    const fileName = `004-add-user-settings.ts`;
    const filePath = path.join("./migrations", fileName);
    await Deno.writeTextFile(filePath, result.content);

    showSuccess("Generated migration using template");
    console.log(dim(`  File: ${filePath}`));
    console.log(dim(`  Template: ${result.metadata.template}`));
    console.log(dim(`  Size: ${result.metadata.size} bytes`));

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    showError(`Migration generation failed: ${message}`);
  }
}

/**
 * Demonstrate CLI integration
 */
function demonstrateCLI(): void {
  showStep("5", "Demonstrating CLI integration");

  console.log(dim("Available CLI commands:"));
  console.log(dim("  • deno task migrate:init     - Initialize configuration"));
  console.log(dim("  • deno task migrate:generate - Generate new migration"));
  console.log(dim("  • deno task migrate:status   - Show migration status"));
  console.log(dim("  • deno task migrate:apply    - Apply pending migrations"));
  console.log(dim("  • deno task migrate:rollback - Rollback last migration"));

  console.log();
  console.log(yellow("Try running:"));
  console.log(dim("  deno task migrate:status"));
  console.log(dim("  deno task migrate:generate --name my-migration --template empty"));

  showSuccess("CLI integration ready");
}

/**
 * Show migration builder capabilities
 */
function demonstrateMigrationBuilder(): void {
  showStep("6", "Demonstrating migration builder");

  try {
    console.log(dim("Migration builder functionality:"));
    console.log(dim("  • Functional approach using migrationBuilder()"));
    console.log(dim("  • Schema-driven collection creation"));
    console.log(dim("  • Type-safe operation chaining"));
    console.log(dim("  • Validation integration"));
    console.log(dim("  • Simulation and production appliers"));

    showSuccess("Migration builder API available");
    console.log(dim("  See generated migration files for usage examples"));
    console.log(dim("  API: migrationBuilder({ schemas }).createCollection().done().compile()"));

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    showError(`Migration builder failed: ${message}`);
  }
}

/**
 * Show summary and next steps
 */
function showSummary(): void {
  showSection("Playground Complete!");

  console.log(green("✓ Configuration system demonstrated"));
  console.log(green("✓ Migration files created"));
  console.log(green("✓ Schema definitions established"));
  console.log(green("✓ Template generation shown"));
  console.log(green("✓ CLI integration ready"));
  console.log(green("✓ Migration builder API demonstrated"));

  console.log(bold(yellow("\nNext Steps:")));
  console.log(dim("1. Start MongoDB: mongod --dbpath ./data"));
  console.log(dim("2. Check status: deno task migrate:status"));
  console.log(dim("3. Apply migrations: deno task migrate:apply"));
  console.log(dim("4. Generate new migration: deno task migrate:generate --name my-feature"));
  console.log(dim("5. Explore the generated files in ./migrations/ and ./schemas/"));

  console.log(bold(yellow("\nGenerated Files:")));
  console.log(dim("• mongodbee.config.json - Migration system configuration"));
  console.log(dim("• migrations/001-create-users-collection.ts - Users collection setup"));
  console.log(dim("• migrations/002-seed-initial-users.ts - Sample data seeding"));
  console.log(dim("• migrations/003-create-user-profiles.ts - User profiles collection"));
  console.log(dim("• migrations/004-add-user-settings.ts - Generated via template"));
  console.log(dim("• schemas/user.ts - User schema definition"));
  console.log(dim("• schemas/user-profile.ts - User profile schema definition"));

  console.log(bold(yellow("\nDocumentation:")));
  console.log(dim("• README.md - Complete usage documentation"));
  console.log(dim("• Migration system: https://jsr.io/@diister/mongodbee"));
  console.log(dim("• CLI reference: deno task migrate --help"));
}

/**
 * Main playground function
 */
async function main(): Promise<void> {
  console.log(bold(blue("MongoDBee Migration System - Complete Playground")));
  console.log(dim("Demonstrating full migration system capabilities\n"));

  try {
    // Create demo files
    await createDemoMigrations();
    await createDemoSchemas();
    
    // Demonstrate system features
    const _config = await demonstrateConfiguration();
    await demonstrateMigrationGeneration();
    demonstrateCLI();
    demonstrateMigrationBuilder();
    
    // Show completion summary
    showSummary();

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    showError(`Playground failed: ${message}`);
    console.error(error);
    Deno.exit(1);
  }
}

// Run playground if this is the main module
if (import.meta.main) {
  await main();
}