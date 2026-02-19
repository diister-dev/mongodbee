import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
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

test("MultiCollection: Nullable object - null to object transition", async () => {
  await withDatabase("MultiCollection: Nullable object - null to object transition", async (db) => {
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
    expect(initialJob).not.toBeNull();
    expect(initialJob.request).toEqual(null);

    const now = new Date();

    // Update null field to object - should work with full replacement
    await coll.updateById("job", jobId, {
      request: {
        type: "cancel",
        requestedAt: now,
        requestedBy: "user:123",
      },
    });

    const updatedJob = await coll.findOne("job", { _id: jobId });
    expect(updatedJob).not.toBeNull();
    expect(updatedJob.request).not.toBeNull();
    expect(updatedJob.request?.type).toEqual("cancel");
    expect(updatedJob.request?.requestedBy).toEqual("user:123");
  });
});

test("MultiCollection: Nullable object - object to null transition", async () => {
  await withDatabase("MultiCollection: Nullable object - object to null transition", async (db) => {
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
    expect(initialJob).not.toBeNull();
    expect(initialJob.request).not.toBeNull();

    await coll.updateById("job", jobId, {
      request: null,
    });

    const updatedJob = await coll.findOne("job", { _id: jobId });
    expect(updatedJob).not.toBeNull();
    expect(updatedJob.request).toEqual(null);
  });
});

test("MultiCollection: Nullable object - full replacement removes old fields", async () => {
  await withDatabase("MultiCollection: Nullable object - full replacement removes old fields", async (db) => {
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
    await coll.updateById("job", jobId, {
      request: {
        type: "cancel",
        requestedAt: now,
        requestedBy: "user:new",
      },
    });

    const updatedJob = await coll.findOne("job", { _id: jobId });
    expect(updatedJob).not.toBeNull();
    expect(updatedJob.request?.type).toEqual("cancel");
    expect(updatedJob.request?.requestedBy).toEqual("user:new");
    // Old field should be removed with full replacement
    expect(updatedJob.request?.reason).toEqual(undefined);
  });
});

test("MultiCollection: Deeply nested nullable object", async () => {
  await withDatabase("MultiCollection: Deeply nested nullable object", async (db) => {
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
    await coll.updateById("user", userId, {
      "profile.contact": {
        email: "john@example.com",
        phone: "123-456",
      },
    });

    const updatedUser = await coll.findOne("user", { _id: userId });
    expect(updatedUser).not.toBeNull();
    expect(updatedUser.profile.bio).toEqual("Developer");
    expect(updatedUser.profile.contact?.email).toEqual("john@example.com");
  });
});

// =============================================================================
// UNION FIELD TESTS
// =============================================================================

test("MultiCollection: Union - switch between variants", async () => {
  await withDatabase("MultiCollection: Union - switch between variants", async (db) => {
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
    expect(initialEntity).not.toBeNull();
    expect(initialEntity.data.type).toEqual("typeA");
    expect((initialEntity.data as { a: string }).a).toEqual("hello");

    // Switch to typeB - old fields should be removed
    await coll.updateById("entity", entityId, {
      data: {
        type: "typeB",
        b: 42,
        commonField: "updated",
      },
    });

    const updatedEntity = await coll.findOne("entity", { _id: entityId });
    expect(updatedEntity).not.toBeNull();
    expect(updatedEntity.data.type).toEqual("typeB");
    expect((updatedEntity.data as { b: number }).b).toEqual(42);
    // Field from typeA should NOT exist
    expect((updatedEntity.data as { a?: string }).a).toEqual(undefined);
  });
});

test("MultiCollection: Nullable union - null to variant", async () => {
  await withDatabase("MultiCollection: Nullable union - null to variant", async (db) => {
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
    expect(initialEntity).not.toBeNull();
    expect(initialEntity.data).toEqual(null);

    // Update null to typeA variant
    await coll.updateById("entity", entityId, {
      data: {
        type: "typeA",
        a: "hello",
      },
    });

    const updatedEntity = await coll.findOne("entity", { _id: entityId });
    expect(updatedEntity).not.toBeNull();
    expect(updatedEntity.data).not.toBeNull();
    expect(updatedEntity.data?.type).toEqual("typeA");
  });
});

test("MultiCollection: Union with different structures - complete replacement", async () => {
  await withDatabase("MultiCollection: Union with different structures - complete replacement", async (db) => {
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
    await coll.updateById("payment", paymentId, {
      method: {
        type: "bank_transfer",
        iban: "FR7630001007941234567890185",
        bic: "BNPAFRPP",
      },
    });

    const updatedPayment = await coll.findOne("payment", { _id: paymentId });
    expect(updatedPayment).not.toBeNull();
    expect(updatedPayment.method.type).toEqual("bank_transfer");
    expect((updatedPayment.method as { iban: string }).iban).toEqual("FR7630001007941234567890185");

    // Card fields should NOT exist
    expect((updatedPayment.method as { cardNumber?: string }).cardNumber).toEqual(undefined);
    expect((updatedPayment.method as { expiry?: string }).expiry).toEqual(undefined);
    expect((updatedPayment.method as { cvv?: string }).cvv).toEqual(undefined);
  });
});
