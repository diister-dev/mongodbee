import type { MongoClientOptions } from "mongodb";
import { MongoClient } from "../../../mongodb.ts";

/**
 * The client of a CLI command: the configured connection options, always on
 * the primary — a migration reads its state and history right before
 * writing them, so it must never see a lagging secondary.
 */
export function createMigrationClient(
  uri: string,
  config: {
    database?: { connection?: { options?: Record<string, unknown> } };
  },
): MongoClient {
  return new MongoClient(uri, {
    ...(config.database?.connection?.options as MongoClientOptions | undefined),
    readPreference: "primary",
  });
}
