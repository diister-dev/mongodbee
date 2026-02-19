import { test, expect } from "vitest";
import { createQueueSystem } from "../src/utils/queue.ts";

test("Integration test - MongoDB queue system usage example", async () => {
  const mongoOperationQueue = createQueueSystem({ maxConcurrent: 2 });

  // Simulate multiple index creation operations
  const mockCollection = {
    async createIndex(
      _keySpec: Record<string, number>,
      options?: Record<string, unknown>,
    ) {
      // Simulate some delay for index creation
      await new Promise((resolve) => setTimeout(resolve, 10));
      return {
        acknowledged: true,
        name: options?.name || "test_index",
      };
    },

    async createIndexes(
      specs: Array<{ key: Record<string, number>; [key: string]: unknown }>,
    ) {
      // Simulate bulk index creation
      await new Promise((resolve) => setTimeout(resolve, 15));
      return {
        acknowledged: true,
        createdCollectionAutomatically: false,
        numIndexesBefore: 1,
        numIndexesAfter: 1 + specs.length,
      };
    },
  };

  // Test single index creation through queue
  const result1 = await mongoOperationQueue.add(() => {
    return mockCollection.createIndex(
      { name: 1 },
      { name: "name_index", unique: true },
    );
  });

  expect(result1.acknowledged).toEqual(true);
  expect(result1.name).toEqual("name_index");

  // Test bulk index creation through queue
  const result2 = await mongoOperationQueue.add(() => {
    return mockCollection.createIndexes([
      { key: { email: 1 }, name: "email_index" },
      { key: { age: 1 }, name: "age_index" },
    ]);
  });

  expect(result2.acknowledged).toEqual(true);
  expect(result2.numIndexesAfter).toEqual(3); // 1 initial + 2 created

  // Check queue stats
  const stats = mongoOperationQueue.getStats();
  expect(stats.completed).toEqual(2);
  expect(stats.failed).toEqual(0);
  expect(stats.pending).toEqual(0);
  expect(stats.running).toEqual(0);
});
