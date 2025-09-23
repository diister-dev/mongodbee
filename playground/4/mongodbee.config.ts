/**
 * MongoDBee Migration Configuration - Simple & User-friendly
 * 
 * This is all you need! Everything else has sensible defaults.
 * Much simpler than the previous 95-line configuration file.
 */

import type { MigrationSystemConfig } from "../../library/src/migration/config/types.ts";

const config: MigrationSystemConfig = {
  // Database connection (required)
  database: {
    connection: {
      uri: "mongodb://localhost:27017"
    },
    name: "mongodbee_playground_dev"
  },
  
  // File paths (required)  
  paths: {
    migrations: "./migrations",
    schemas: "./schemas"
  },

  // Environment-specific overrides (optional)
  environments: {
    development: {
      database: {
        connection: { uri: "mongodb://localhost:27017" },
        name: "mongodbee_playground_dev"
      }
    },
    
    production: {
      database: {
        connection: { uri: Deno.env.get("MONGODB_URI") || "mongodb://localhost:27017" },
        name: Deno.env.get("MONGODB_DATABASE") || "mongodbee_prod"
      }
    }
  }
};

export default config;