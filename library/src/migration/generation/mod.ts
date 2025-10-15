/**
 * @module
 * Functions for generating migration IDs and files.
 */

import { ulid } from "@std/ulid/ulid";

/**
 * Generates a unique migration ID based on the current timestamp and an optional name.
 * The format is `YYYY_MM_DD_<ULID>_<name>`, where `<ID>` is a zero-padded number to ensure uniqueness.
 */
export function generateMigrationId(name?: string): string {
  const date = new Date();
  const datePart = date.toISOString().split("T")[0].replace(/-/g, "_");
  const uniquePart = ulid();
  const namePart = name ? `_${name.replace(/\s+/g, "_").toLowerCase()}` : "";
  return `${datePart}_${uniquePart}${namePart}`;
}
