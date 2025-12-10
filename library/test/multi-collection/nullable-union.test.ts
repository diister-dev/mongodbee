import * as v from "../../src/schema.ts";
import { assertEquals } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import assert from "node:assert";
import { defineModel } from "../../src/multi-collection-model.ts";

/**
 * Tests for nullable and union field update operations in multi-collections.
 *
 * These tests verify that:
 * - Nullable fields can transition from null to object and vice versa
 * - Union fields can switch between variants without field contamination
 * - Nested nullable objects work correctly
 */

// =============================================================================
// NULLABLE FIELD TESTS
// =============================================================================

Deno.test("MultiCollection: Nullable object - null to object transition", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        job: {
          name: v.string(),
          status: v.string(),
          request: v.nullable(v.object({
            type: v.string(),
            requestedAt: v.date(),
            requestedBy: v.string(),
          })),
        },
      },
    });

    const coll = await multiCollection(db, "test", testModel);

    const jobId = await coll.insertOne("job", {
      name: "Test Job",
      status: "running",
      request: null,
    });

    const initialJob = await coll.findOne("job", { _id: jobId });
    assert(initialJob !== null);
    assertEquals(initialJob.request, null);

    const now = new Date();

    // Update null field to object - should work with full replacement
    await coll.updateOne("job", jobId, {
      request: {
        type: "cancel",
        requestedAt: now,
        requestedBy: "user:123",
      },
    });

    const updatedJob = await coll.findOne("job", { _id: jobId });
    assert(updatedJob !== null);
    assert(updatedJob.request !== null);
    assertEquals(updatedJob.request?.type, "cancel");
    assertEquals(updatedJob.request?.requestedBy, "user:123");
  });
});

Deno.test("MultiCollection: Nullable object - object to null transition", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        job: {
          name: v.string(),
          request: v.nullable(v.object({
            type: v.string(),
            requestedAt: v.date(),
          })),
        },
      },
    });

    const coll = await multiCollection(db, "test", testModel);

    const jobId = await coll.insertOne("job", {
      name: "Test Job",
      request: {
        type: "cancel",
        requestedAt: new Date(),
      },
    });

    const initialJob = await coll.findOne("job", { _id: jobId });
    assert(initialJob !== null);
    assert(initialJob.request !== null);

    await coll.updateOne("job", jobId, {
      request: null,
    });

    const updatedJob = await coll.findOne("job", { _id: jobId });
    assert(updatedJob !== null);
    assertEquals(updatedJob.request, null);
  });
});

Deno.test("MultiCollection: Nullable object - full replacement removes old fields", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        job: {
          name: v.string(),
          request: v.nullable(v.object({
            type: v.string(),
            requestedAt: v.date(),
            requestedBy: v.string(),
            reason: v.optional(v.string()),
          })),
        },
      },
    });

    const coll = await multiCollection(db, "test", testModel);

    // Insert with optional reason field
    const jobId = await coll.insertOne("job", {
      name: "Test Job",
      request: {
        type: "pause",
        requestedAt: new Date("2024-01-01"),
        requestedBy: "user:old",
        reason: "maintenance",
      },
    });

    const now = new Date();

    // Update without reason field - should remove it
    await coll.updateOne("job", jobId, {
      request: {
        type: "cancel",
        requestedAt: now,
        requestedBy: "user:new",
      },
    });

    const updatedJob = await coll.findOne("job", { _id: jobId });
    assert(updatedJob !== null);
    assertEquals(updatedJob.request?.type, "cancel");
    assertEquals(updatedJob.request?.requestedBy, "user:new");
    // Old field should be removed with full replacement
    assertEquals(updatedJob.request?.reason, undefined);
  });
});

Deno.test("MultiCollection: Deeply nested nullable object", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          profile: v.object({
            bio: v.string(),
            contact: v.nullable(v.object({
              email: v.string(),
              phone: v.optional(v.string()),
            })),
          }),
        },
      },
    });

    const coll = await multiCollection(db, "test", testModel);

    const userId = await coll.insertOne("user", {
      name: "John",
      profile: {
        bio: "Developer",
        contact: null,
      },
    });

    // Update nested null to object using dot notation path
    await coll.updateOne("user", userId, {
      "profile.contact": {
        email: "john@example.com",
        phone: "123-456",
      },
    });

    const updatedUser = await coll.findOne("user", { _id: userId });
    assert(updatedUser !== null);
    assertEquals(updatedUser.profile.bio, "Developer");
    assertEquals(updatedUser.profile.contact?.email, "john@example.com");
  });
});

// =============================================================================
// UNION FIELD TESTS
// =============================================================================

Deno.test("MultiCollection: Union - switch between variants", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        entity: {
          name: v.string(),
          data: v.union([
            v.object({ type: v.literal("typeA"), a: v.string(), commonField: v.string() }),
            v.object({ type: v.literal("typeB"), b: v.number(), commonField: v.string() }),
          ]),
        },
      },
    });

    const coll = await multiCollection(db, "test", testModel);

    const entityId = await coll.insertOne("entity", {
      name: "Test Entity",
      data: {
        type: "typeA",
        a: "hello",
        commonField: "shared",
      },
    });

    const initialEntity = await coll.findOne("entity", { _id: entityId });
    assert(initialEntity !== null);
    assertEquals(initialEntity.data.type, "typeA");
    assertEquals((initialEntity.data as { a: string }).a, "hello");

    // Switch to typeB - old fields should be removed
    await coll.updateOne("entity", entityId, {
      data: {
        type: "typeB",
        b: 42,
        commonField: "updated",
      },
    });

    const updatedEntity = await coll.findOne("entity", { _id: entityId });
    assert(updatedEntity !== null);
    assertEquals(updatedEntity.data.type, "typeB");
    assertEquals((updatedEntity.data as { b: number }).b, 42);
    // Field from typeA should NOT exist
    assertEquals((updatedEntity.data as { a?: string }).a, undefined);
  });
});

Deno.test("MultiCollection: Nullable union - null to variant", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        entity: {
          name: v.string(),
          data: v.nullable(v.union([
            v.object({ type: v.literal("typeA"), a: v.string() }),
            v.object({ type: v.literal("typeB"), b: v.number() }),
          ])),
        },
      },
    });

    const coll = await multiCollection(db, "test", testModel);

    const entityId = await coll.insertOne("entity", {
      name: "Test Entity",
      data: null,
    });

    const initialEntity = await coll.findOne("entity", { _id: entityId });
    assert(initialEntity !== null);
    assertEquals(initialEntity.data, null);

    // Update null to typeA variant
    await coll.updateOne("entity", entityId, {
      data: {
        type: "typeA",
        a: "hello",
      },
    });

    const updatedEntity = await coll.findOne("entity", { _id: entityId });
    assert(updatedEntity !== null);
    assert(updatedEntity.data !== null);
    assertEquals(updatedEntity.data?.type, "typeA");
  });
});

Deno.test("MultiCollection: Union with different structures - complete replacement", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        payment: {
          orderId: v.string(),
          method: v.union([
            v.object({
              type: v.literal("card"),
              cardNumber: v.string(),
              expiry: v.string(),
              cvv: v.string(),
            }),
            v.object({
              type: v.literal("bank_transfer"),
              iban: v.string(),
              bic: v.string(),
            }),
            v.object({
              type: v.literal("crypto"),
              walletAddress: v.string(),
              network: v.string(),
            }),
          ]),
        },
      },
    });

    const coll = await multiCollection(db, "test", testModel);

    const paymentId = await coll.insertOne("payment", {
      orderId: "order:123",
      method: {
        type: "card",
        cardNumber: "4111111111111111",
        expiry: "12/25",
        cvv: "123",
      },
    });

    // Switch from card to bank_transfer
    await coll.updateOne("payment", paymentId, {
      method: {
        type: "bank_transfer",
        iban: "FR7630001007941234567890185",
        bic: "BNPAFRPP",
      },
    });

    const updatedPayment = await coll.findOne("payment", { _id: paymentId });
    assert(updatedPayment !== null);
    assertEquals(updatedPayment.method.type, "bank_transfer");
    assertEquals((updatedPayment.method as { iban: string }).iban, "FR7630001007941234567890185");

    // Card fields should NOT exist
    assertEquals((updatedPayment.method as { cardNumber?: string }).cardNumber, undefined);
    assertEquals((updatedPayment.method as { expiry?: string }).expiry, undefined);
    assertEquals((updatedPayment.method as { cvv?: string }).cvv, undefined);
  });
});
