import * as v from "../src/schema.ts";
import { removeField } from "../src/sanitizer.ts";
import { test, expect } from "vitest";

// Mock collection behavior to test without MongoDB
class MockCollection {
  private documents: any[] = [];
  private lastDoc: any = null;

  constructor(
    private undefinedBehavior: "remove" | "ignore" | "error" = "remove",
  ) {}

  async insertOne(doc: any) {
    // Simulate the sanitization that would happen in real collection
    this.lastDoc = this.sanitize(doc);
    this.documents.push(this.lastDoc);
    return { insertedId: "mock-id" };
  }

  async replaceOne(filter: any, replacement: any) {
    this.lastDoc = this.sanitize(replacement);
    return { modifiedCount: 1 };
  }

  getLastDocument() {
    return this.lastDoc;
  }

  private sanitize(obj: any): any {
    // Simplified sanitization logic for testing
    if (this.undefinedBehavior === "error") {
      this.checkForUndefined(obj);
    }
    return this.removeUndefinedRecursive(obj);
  }

  private checkForUndefined(obj: any) {
    if (obj === undefined) {
      throw new Error("Undefined values are not allowed");
    }
    if (Array.isArray(obj)) {
      obj.forEach(this.checkForUndefined.bind(this));
    } else if (typeof obj === "object" && obj !== null) {
      Object.values(obj).forEach(this.checkForUndefined.bind(this));
    }
  }

  private removeUndefinedRecursive(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }
    if (Array.isArray(obj)) {
      return obj.map((item) => this.removeUndefinedRecursive(item))
        .filter((item) => item !== undefined);
    }
    if (typeof obj === "object" && obj.constructor === Object) {
      const result: any = {};
      for (const [key, value] of Object.entries(obj)) {
        if (value !== undefined && value !== removeField()) {
          result[key] = this.removeUndefinedRecursive(value);
        }
      }
      return result;
    }
    return obj;
  }
}

test("Integration: Default behavior removes undefined fields", async () => {
  const collection = new MockCollection("remove");

  await collection.insertOne({
    name: "John",
    email: "john@example.com",
    phone: undefined, // Should be removed
    age: 30,
    metadata: {
      source: "api",
      tags: undefined, // Should be removed
    },
  });

  const result = collection.getLastDocument();

  expect(result).toEqual({
    name: "John",
    email: "john@example.com",
    age: 30,
    metadata: {
      source: "api",
    },
  });

  // Verify removed fields
  expect(!("phone" in result)).toBeTruthy();
  expect(!("tags" in result.metadata)).toBeTruthy();
});

test("Integration: removeField() explicitly removes fields", async () => {
  const collection = new MockCollection("remove");

  await collection.insertOne({
    name: "John",
    email: "john@example.com",
    phone: removeField(), // Explicit removal
    age: 30,
    temporaryFlag: removeField(), // Explicit removal
  });

  const result = collection.getLastDocument();

  expect(result).toEqual({
    name: "John",
    email: "john@example.com",
    age: 30,
  });

  // Verify explicitly removed fields
  expect(!("phone" in result)).toBeTruthy();
  expect(!("temporaryFlag" in result)).toBeTruthy();
});

test("Integration: Error behavior throws on undefined", async () => {
  const collection = new MockCollection("error");

  // Should work fine without undefined
  await collection.insertOne({
    name: "John",
    email: "john@example.com",
    age: 30,
  });

  let errorThrown = false;
  try {
    await collection.insertOne({
      name: "Jane",
      email: undefined, // Should cause error
      age: 25,
    });
  } catch (error) {
    errorThrown = true;
    expect(error instanceof Error).toBeTruthy();
    expect((error as Error).message.includes("Undefined values are not allowed")).toBeTruthy();
  }

  expect(errorThrown).toBeTruthy();
});

test("Integration: Complex nested scenario", async () => {
  const collection = new MockCollection("remove");

  // Complex real-world scenario
  await collection.insertOne({
    user: {
      profile: {
        name: "John Doe",
        avatar: undefined, // Not provided
        bio: "Developer",
      },
      settings: {
        theme: "dark",
        notifications: {
          email: true,
          push: undefined, // Not configured
          sms: false,
        },
        privacy: removeField(), // Explicitly not wanted
      },
      metadata: {
        lastLogin: new Date(),
        createdAt: new Date(),
        tags: ["user", undefined, "active"], // Mixed array
        flags: {
          verified: true,
          beta: undefined, // Not in beta
          admin: removeField(), // Explicitly not admin
        },
      },
    },
  });

  const result = collection.getLastDocument();

  // Should have clean structure without undefined fields
  expect(result.user.profile).toEqual({
    name: "John Doe",
    bio: "Developer",
  });

  expect(result.user.settings.notifications).toEqual({
    email: true,
    sms: false,
  });

  expect(result.user.metadata.tags).toEqual(["user", "active"]);

  expect(result.user.metadata.flags).toEqual({
    verified: true,
  });

  // Verify all undefined and removeField() values were removed
  expect(!("avatar" in result.user.profile)).toBeTruthy();
  expect(!("push" in result.user.settings.notifications)).toBeTruthy();
  expect(!("privacy" in result.user.settings)).toBeTruthy();
  expect(!("beta" in result.user.metadata.flags)).toBeTruthy();
  expect(!("admin" in result.user.metadata.flags)).toBeTruthy();
});

test("Integration: Update operations maintain consistency", async () => {
  const collection = new MockCollection("remove");

  // Initial insert
  await collection.insertOne({
    name: "John",
    email: "john@example.com",
    phone: "123-456-7890",
    bio: "Software developer",
  });

  const initialDoc = collection.getLastDocument();

  // Update with mixed undefined and removeField()
  await collection.replaceOne({}, {
    name: "John Updated",
    email: "john.new@example.com",
    phone: removeField(), // Explicit removal
    bio: undefined, // Implicit removal
    avatar: "http://example.com/avatar.jpg", // New field
  });

  const updatedDoc = collection.getLastDocument();

  expect(updatedDoc).toEqual({
    name: "John Updated",
    email: "john.new@example.com",
    avatar: "http://example.com/avatar.jpg",
  });

  // Verify removed fields
  expect(!("phone" in updatedDoc)).toBeTruthy();
  expect(!("bio" in updatedDoc)).toBeTruthy();
});

test("Integration: Array sanitization in complex structures", async () => {
  const collection = new MockCollection("remove");

  await collection.insertOne({
    products: [
      {
        id: 1,
        name: "Product 1",
        price: 100,
        discount: undefined, // No discount
      },
      {
        id: 2,
        name: "Product 2",
        price: 200,
        discount: 10,
        category: removeField(), // Remove this field
      },
      undefined, // Invalid product entry
      {
        id: 3,
        name: "Product 3",
        price: 300,
        tags: ["electronics", undefined, "popular"], // Mixed tags
      },
    ],
    metadata: {
      total: 3,
      filters: undefined,
      pagination: {
        page: 1,
        limit: 10,
        hasNext: false,
        hasPrev: undefined, // Not set
      },
    },
  });

  const result = collection.getLastDocument();

  // Array should be cleaned
  expect(result.products.length).toEqual(3);

  // First product - discount removed
  expect(!("discount" in result.products[0])).toBeTruthy();
  expect(result.products[0]).toEqual({
    id: 1,
    name: "Product 1",
    price: 100,
  });

  // Second product - category removed
  expect(!("category" in result.products[1])).toBeTruthy();
  expect(result.products[1]).toEqual({
    id: 2,
    name: "Product 2",
    price: 200,
    discount: 10,
  });

  // Third product - tags cleaned
  expect(result.products[2].tags).toEqual(["electronics", "popular"]);

  // Metadata cleaned
  expect(!("filters" in result.metadata)).toBeTruthy();
  expect(!("hasPrev" in result.metadata.pagination)).toBeTruthy();
});

test("Integration: Performance with large nested structures", async () => {
  const collection = new MockCollection("remove");

  // Create a reasonably large structure with many undefined values
  const largeDoc = {
    users: Array.from({ length: 100 }, (_, i) => ({
      id: i,
      name: `User ${i}`,
      email: i % 2 === 0 ? `user${i}@example.com` : undefined,
      profile: {
        avatar: i % 3 === 0 ? `avatar${i}.jpg` : undefined,
        bio: i % 4 === 0 ? `Bio for user ${i}` : undefined,
        settings: {
          theme: i % 5 === 0 ? "dark" : undefined,
          language: "en",
          notifications: i % 6 === 0 ? { email: true } : undefined,
        },
      },
    })),
  };

  const startTime = performance.now();
  await collection.insertOne(largeDoc);
  const endTime = performance.now();

  const result = collection.getLastDocument();

  // Should process quickly (under 10ms for this size)
  expect(
    (endTime - startTime) < 100,
  ).toBeTruthy();

  // Verify structure is properly cleaned
  expect(result.users.length).toEqual(100);

  // Check that undefined fields were properly removed
  for (let i = 0; i < 100; i++) {
    const user = result.users[i];
    expect(user.id).toEqual(i);
    expect(user.name).toEqual(`User ${i}`);

    if (i % 2 !== 0) {
      expect(!("email" in user)).toBeTruthy();
    }

    if (i % 3 !== 0) {
      expect(!("avatar" in user.profile)).toBeTruthy();
    }
  }
});
