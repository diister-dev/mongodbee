/**
 * Migration: create_users
 * 
 * Generated: 2025-09-23T23:06:38.865Z
 * 
 */

import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";

export default migrationDefinition("2025-09-23-01K5WD8EWHY0CGRJXSEAEQF9PV-create-users", "create_users", {
  parent: null,
  schemas: {
    collections: {}
  },
  migrate: (builder) => {
    return builder
      .compile();
  },
});
