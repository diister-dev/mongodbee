/**
 * @fileoverview Comprehensive rollback tests for migrations
 *
 * Tests rollback functionality for both collections and multi-collections,
 * including validator synchronization and successive rollbacks.
 *
 * @module
 */

import { assertEquals } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { MongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import * as v from "../../src/schema.ts";
import {
  markMigrationAsApplied,
  markMigrationAsReverted,
} from "../../src/migration/state.ts";

// ============================================================================
// Collection Transformation Rollback Tests
// ============================================================================

Deno.test("Rollback - simple collection transformation", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create initial migration
    const migration1 = migrationDefinition(
      "test_001@create_users",
      "create_users",
      {
        parent: null,
        schemas: {
          collections: {
            users: {
              name: v.string(),
              email: v.string(),
            },
          },
          multiCollections: {},
        },
        migrate(migration) {
          return migration
            .createCollection("users")
            .done()
            .collection("users")
            .seed([
              { name: "Alice", email: "alice@test.com" },
              { name: "Bob", email: "bob@test.com" },
            ])
            .done()
            .compile();
        },
      },
    );

    // Create transformation migration that adds a field
    const migration2 = migrationDefinition(
      "test_002@add_age",
      "add_age",
      {
        parent: migration1,
        schemas: {
          collections: {
            users: {
              name: v.string(),
              email: v.string(),
              age: v.number(),
            },
          },
          multiCollections: {},
        },
        migrate(migration) {
          return migration
            .collection("users")
            .transform({
              up: (doc) => ({ ...doc, age: 25 }),
              down: (doc) => {
                const { age, ...rest } = doc;
                return rest;
              },
            })
            .done()
            .compile();
        },
      },
    );

    // Apply both migrations
    const applier = new MongodbApplier(db);

    const state1 = migration1.migrate(migrationBuilder({ schemas: migration1.schemas }));
    for (const op of state1.operations) {
      await applier.applyOperation(op);
    }
    await applier.synchronizeSchemas(migration1.schemas);
    await markMigrationAsApplied(db, migration1.id, migration1.name);

    applier.setCurrentMigrationId(migration2.id);
    const state2 = migration2.migrate(migrationBuilder({ schemas: migration2.schemas }));
    for (const op of state2.operations) {
      await applier.applyOperation(op);
    }
    await applier.synchronizeSchemas(migration2.schemas);
    await markMigrationAsApplied(db, migration2.id, migration2.name);

    // Verify age field was added
    const usersAfterMigration = await db.collection("users").find({}).toArray();
    assertEquals(usersAfterMigration.length, 2);
    assertEquals(usersAfterMigration[0].age, 25);
    assertEquals(usersAfterMigration[1].age, 25);

    // Rollback migration2
    // CRITICAL: Synchronize with parent schema BEFORE rollback
    await applier.synchronizeSchemas(migration2.parent!.schemas);

    for (let i = state2.operations.length - 1; i >= 0; i--) {
      await applier.applyReverseOperation(state2.operations[i]);
    }
    await markMigrationAsReverted(db, migration2.id);

    // Verify age field was removed
    const usersAfterRollback = await db.collection("users").find({}).toArray();
    assertEquals(usersAfterRollback.length, 2);
    assertEquals("age" in usersAfterRollback[0], false, "age field should be removed after rollback");
    assertEquals("age" in usersAfterRollback[1], false, "age field should be removed after rollback");

    // Verify data integrity (name and email should be intact)
    assertEquals(usersAfterRollback[0].name, "Alice");
    assertEquals(usersAfterRollback[0].email, "alice@test.com");
    assertEquals(usersAfterRollback[1].name, "Bob");
    assertEquals(usersAfterRollback[1].email, "bob@test.com");
  });
});

Deno.test("Rollback - successive migrations", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Migration 1: Create collection
    const migration1 = migrationDefinition("test_001@create", "create", {
      parent: null,
      schemas: {
        collections: {
          products: {
            name: v.string(),
          },
        },
        multiCollections: {},
      },
      migrate(migration) {
        return migration
          .createCollection("products")
          .done()
          .collection("products")
          .seed([{ name: "Product A" }])
          .done()
          .compile();
      },
    });

    // Migration 2: Add price
    const migration2 = migrationDefinition("test_002@add_price", "add_price", {
      parent: migration1,
      schemas: {
        collections: {
          products: {
            name: v.string(),
            price: v.number(),
          },
        },
        multiCollections: {},
      },
      migrate(migration) {
        return migration
          .collection("products")
          .transform({
            up: (doc) => ({ ...doc, price: 100 }),
            down: (doc) => {
              const { price, ...rest } = doc;
              return rest;
            },
          })
          .done()
          .compile();
      },
    });

    // Migration 3: Add stock
    const migration3 = migrationDefinition("test_003@add_stock", "add_stock", {
      parent: migration2,
      schemas: {
        collections: {
          products: {
            name: v.string(),
            price: v.number(),
            stock: v.number(),
          },
        },
        multiCollections: {},
      },
      migrate(migration) {
        return migration
          .collection("products")
          .transform({
            up: (doc) => ({ ...doc, stock: 10 }),
            down: (doc) => {
              const { stock, ...rest } = doc;
              return rest;
            },
          })
          .done()
          .compile();
      },
    });

    const applier = new MongodbApplier(db);

    // Apply all three migrations
    const applyMigration = async (migration: any) => {
      applier.setCurrentMigrationId(migration.id);
      const state = migration.migrate(migrationBuilder({ schemas: migration.schemas }));
      for (const op of state.operations) {
        await applier.applyOperation(op);
      }
      await applier.synchronizeSchemas(migration.schemas);
      await markMigrationAsApplied(db, migration.id, migration.name);
    };

    await applyMigration(migration1);
    await applyMigration(migration2);
    await applyMigration(migration3);

    // Verify all fields exist
    let products = await db.collection("products").find({}).toArray();
    assertEquals(products[0].name, "Product A");
    assertEquals(products[0].price, 100);
    assertEquals(products[0].stock, 10);

    // Rollback migration 3
    await applier.synchronizeSchemas(migration3.parent!.schemas);
    const state3 = migration3.migrate(migrationBuilder({ schemas: migration3.schemas }));
    for (let i = state3.operations.length - 1; i >= 0; i--) {
      await applier.applyReverseOperation(state3.operations[i]);
    }
    await markMigrationAsReverted(db, migration3.id);

    products = await db.collection("products").find({}).toArray();
    assertEquals("stock" in products[0], false, "stock should be removed");
    assertEquals(products[0].price, 100, "price should still exist");

    // Rollback migration 2
    await applier.synchronizeSchemas(migration2.parent!.schemas);
    const state2 = migration2.migrate(migrationBuilder({ schemas: migration2.schemas }));
    for (let i = state2.operations.length - 1; i >= 0; i--) {
      await applier.applyReverseOperation(state2.operations[i]);
    }
    await markMigrationAsReverted(db, migration2.id);

    products = await db.collection("products").find({}).toArray();
    assertEquals("price" in products[0], false, "price should be removed");
    assertEquals(products[0].name, "Product A", "name should still exist");
  });
});

// ============================================================================
// Multi-Collection Transformation Rollback Tests
// ============================================================================

Deno.test("Rollback - multi-collection type transformation", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Migration 1: Create multi-collection
    const migration1 = migrationDefinition(
      "test_001@create_catalog",
      "create_catalog",
      {
        parent: null,
        schemas: {
          collections: {},
          multiCollections: {
            catalog: {
              book: {
                title: v.string(),
                author: v.string(),
              },
            },
          },
        },
        migrate(migration) {
          return migration
            .newMultiCollection("catalog_main", "catalog")
            .seedType("book", [
              { title: "Book 1", author: "Author 1" },
              { title: "Book 2", author: "Author 2" },
            ])
            .end()
            .compile();
        },
      },
    );

    // Migration 2: Add year field
    const migration2 = migrationDefinition(
      "test_002@add_year",
      "add_year",
      {
        parent: migration1,
        schemas: {
          collections: {},
          multiCollections: {
            catalog: {
              book: {
                title: v.string(),
                author: v.string(),
                year: v.number(),
              },
            },
          },
        },
        migrate(migration) {
          return migration
            .multiCollection("catalog")
            .type("book")
            .transform({
              up: (doc) => ({ ...doc, year: 2024 }),
              down: (doc) => {
                const { year, ...rest } = doc;
                return rest;
              },
            })
            .end()
            .end()
            .compile();
        },
      },
    );

    const applier = new MongodbApplier(db);

    // Apply migrations
    applier.setCurrentMigrationId(migration1.id);
    const state1 = migration1.migrate(migrationBuilder({ schemas: migration1.schemas }));
    for (const op of state1.operations) {
      await applier.applyOperation(op);
    }
    await applier.synchronizeSchemas(migration1.schemas);
    await markMigrationAsApplied(db, migration1.id, migration1.name);

    applier.setCurrentMigrationId(migration2.id);
    const state2 = migration2.migrate(migrationBuilder({ schemas: migration2.schemas }));
    for (const op of state2.operations) {
      await applier.applyOperation(op);
    }
    await applier.synchronizeSchemas(migration2.schemas);
    await markMigrationAsApplied(db, migration2.id, migration2.name);

    // Verify year field was added
    let books = await db.collection("catalog_main")
      .find({ _type: "book" }).toArray();
    assertEquals(books.length, 2);
    assertEquals(books[0].year, 2024);
    assertEquals(books[1].year, 2024);

    // Rollback migration 2
    // CRITICAL: Synchronize with parent schema BEFORE rollback
    await applier.synchronizeSchemas(migration2.parent!.schemas);

    for (let i = state2.operations.length - 1; i >= 0; i--) {
      await applier.applyReverseOperation(state2.operations[i]);
    }
    await markMigrationAsReverted(db, migration2.id);

    // Verify year field was removed
    books = await db.collection("catalog_main")
      .find({ _type: "book" }).toArray();
    assertEquals(books.length, 2);
    assertEquals("year" in books[0], false, "year field should be removed after rollback");
    assertEquals("year" in books[1], false, "year field should be removed after rollback");

    // Verify data integrity
    assertEquals(books[0].title, "Book 1");
    assertEquals(books[0].author, "Author 1");
    assertEquals(books[1].title, "Book 2");
    assertEquals(books[1].author, "Author 2");
  });
});

Deno.test("Rollback - multi-collection across multiple instances", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Migration 1: Create multi-collection with two instances
    const migration1 = migrationDefinition(
      "test_001@create_catalogs",
      "create_catalogs",
      {
        parent: null,
        schemas: {
          collections: {},
          multiCollections: {
            catalog: {
              book: {
                title: v.string(),
              },
            },
          },
        },
        migrate(migration) {
          return migration
            .newMultiCollection("catalog_library", "catalog")
            .seedType("book", [{ title: "Library Book 1" }])
            .end()
            .newMultiCollection("catalog_store", "catalog")
            .seedType("book", [{ title: "Store Book 1" }])
            .end()
            .compile();
        },
      },
    );

    // Migration 2: Add ISBN to all books
    const migration2 = migrationDefinition(
      "test_002@add_isbn",
      "add_isbn",
      {
        parent: migration1,
        schemas: {
          collections: {},
          multiCollections: {
            catalog: {
              book: {
                title: v.string(),
                isbn: v.string(),
              },
            },
          },
        },
        migrate(migration) {
          return migration
            .multiCollection("catalog")
            .type("book")
            .transform({
              up: (doc) => ({ ...doc, isbn: "000-0000000000" }),
              down: (doc) => {
                const { isbn, ...rest } = doc;
                return rest;
              },
            })
            .end()
            .end()
            .compile();
        },
      },
    );

    const applier = new MongodbApplier(db);

    // Apply migrations
    applier.setCurrentMigrationId(migration1.id);
    const state1 = migration1.migrate(migrationBuilder({ schemas: migration1.schemas }));
    for (const op of state1.operations) {
      await applier.applyOperation(op);
    }
    await applier.synchronizeSchemas(migration1.schemas);

    applier.setCurrentMigrationId(migration2.id);
    const state2 = migration2.migrate(migrationBuilder({ schemas: migration2.schemas }));
    for (const op of state2.operations) {
      await applier.applyOperation(op);
    }
    await applier.synchronizeSchemas(migration2.schemas);

    // Verify ISBN was added to both instances
    const libraryBooks = await db.collection("catalog_library")
      .find({ _type: "book" }).toArray();
    const storeBooks = await db.collection("catalog_store")
      .find({ _type: "book" }).toArray();

    assertEquals(libraryBooks[0].isbn, "000-0000000000");
    assertEquals(storeBooks[0].isbn, "000-0000000000");

    // Rollback
    await applier.synchronizeSchemas(migration2.parent!.schemas);
    for (let i = state2.operations.length - 1; i >= 0; i--) {
      await applier.applyReverseOperation(state2.operations[i]);
    }

    // Verify ISBN was removed from both instances
    const libraryBooksAfter = await db.collection("catalog_library")
      .find({ _type: "book" }).toArray();
    const storeBooksAfter = await db.collection("catalog_store")
      .find({ _type: "book" }).toArray();

    assertEquals("isbn" in libraryBooksAfter[0], false, "ISBN should be removed from library");
    assertEquals("isbn" in storeBooksAfter[0], false, "ISBN should be removed from store");

    // Verify titles are intact
    assertEquals(libraryBooksAfter[0].title, "Library Book 1");
    assertEquals(storeBooksAfter[0].title, "Store Book 1");
  });
});
