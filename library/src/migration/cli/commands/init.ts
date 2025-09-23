/**
 * Init command for MongoDBee Migration CLI
 * 
 * Initializes migration configuration
 * 
 * @module
 */

import { green, yellow, red, dim } from "@std/fmt/colors";
import * as path from "@std/path";
import { existsSync } from "@std/fs";

export interface InitCommandOptions {
  configPath: string;
  force?: boolean;
}

/**
 * Default configuration template
 */
const DEFAULT_CONFIG = {
  database: {
    connection: {
      uri: "mongodb://localhost:27017",
      options: {
        maxPoolSize: 10,
        minPoolSize: 1,
        maxIdleTimeMS: 30000
      }
    },
    name: "myapp"
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
        name: "myapp_dev"
      }
    },
    staging: {
      database: {
        connection: {
          uri: "mongodb://staging-host:27017"
        },
        name: "myapp_staging"
      }
    },
    production: {
      database: {
        connection: {
          uri: "mongodb://prod-host:27017",
          options: {
            maxPoolSize: 50,
            minPoolSize: 5
          }
        },
        name: "myapp_prod"
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
 * Initialize migration configuration
 */
export async function initCommand(options: InitCommandOptions): Promise<void> {
  const { configPath, force = false } = options;

  console.log(yellow("Initializing MongoDBee migration configuration..."));
  console.log(dim(`Config file: ${configPath}`));

  // Check if config already exists
  if (existsSync(configPath) && !force) {
    console.log(red("Error: Configuration file already exists"));
    console.log(dim("Use --force to overwrite the existing configuration"));
    Deno.exit(1);
  }

  try {
    // Ensure directory exists
    const configDir = path.dirname(configPath);
    await Deno.mkdir(configDir, { recursive: true });

    // Write configuration file
    const configContent = JSON.stringify(DEFAULT_CONFIG, null, 2);
    await Deno.writeTextFile(configPath, configContent);

    // Create migrations directory
    const migrationsDir = path.resolve(configDir, DEFAULT_CONFIG.paths.migrations);
    await Deno.mkdir(migrationsDir, { recursive: true });

    // Create schemas directory
    const schemasDir = path.resolve(configDir, DEFAULT_CONFIG.paths.schemas);
    await Deno.mkdir(schemasDir, { recursive: true });

    console.log(green("✓ Configuration initialized successfully"));
    console.log(dim(`  • Configuration file: ${configPath}`));
    console.log(dim(`  • Migrations directory: ${migrationsDir}`));
    console.log(dim(`  • Schemas directory: ${schemasDir}`));
    
    console.log("\n" + yellow("Next steps:"));
    console.log(dim("1. Edit the configuration file to match your environment"));
    console.log(dim("2. Generate your first migration:"));
    console.log(dim("   mongodbee-migrate generate --name initial-setup"));
    console.log(dim("3. Apply migrations:"));
    console.log(dim("   mongodbee-migrate apply"));

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red("Error initializing configuration:"), message);
    Deno.exit(1);
  }
}