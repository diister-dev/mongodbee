import { test, expect } from "vitest";
import { createQueueSystem } from "../src/utils/queue.ts";

test("MongoOperationQueue - Basic functionality", async () => {
  const queue = createQueueSystem({ maxConcurrent: 2 });

  // Test basic operation
  const result = await queue.add(() => {
    return Promise.resolve("success");
  });

  expect(result).toEqual("success");
});

test("MongoOperationQueue - Concurrency limit", async () => {
  const queue = createQueueSystem({ maxConcurrent: 2 });
  const executionOrder: number[] = [];
  const startTimes: number[] = [];

  // Create operations that track when they start
  const createOperation = (id: number, delay: number) => () => {
    startTimes.push(Date.now());
    executionOrder.push(id);
    return new Promise((resolve) => setTimeout(() => resolve(id), delay));
  };

  // Start 4 operations simultaneously
  const promises = [
    queue.add(createOperation(1, 50)),
    queue.add(createOperation(2, 50)),
    queue.add(createOperation(3, 50)),
    queue.add(createOperation(4, 50)),
  ];

  const results = await Promise.all(promises);

  // All operations should complete
  expect(results.sort()).toEqual([1, 2, 3, 4]);

  // Check that only 2 operations started immediately (within 30ms of each other)
  const firstBatch = startTimes.filter((time, index) =>
    index === 0 || time - startTimes[0] < 30
  );
  expect(firstBatch.length).toEqual(2);
});

test("MongoOperationQueue - Priority ordering", async () => {
  const queue = createQueueSystem({ maxConcurrent: 1 });
  const executionOrder: number[] = [];

  // Create operations with different priorities
  const createOperation = (id: number) => () => {
    executionOrder.push(id);
    return Promise.resolve(id);
  };

  // Start first operation (will start immediately)
  const promise1 = queue.add(createOperation(1), { priority: 1 });

  // Add a small delay to ensure first operation is running
  await new Promise((resolve) => setTimeout(resolve, 10));

  // Add operations with priorities (higher number = higher priority)
  const promise2 = queue.add(createOperation(2), { priority: 3 });
  const promise3 = queue.add(createOperation(3), { priority: 2 });

  await Promise.all([promise1, promise2, promise3]);

  // Should execute in order: 1 (first started), 2 (priority 3), 3 (priority 2)
  expect(executionOrder).toEqual([1, 2, 3]);
});

test("MongoOperationQueue - Timeout handling", async () => {
  const queue = createQueueSystem({ defaultTimeout: 30 }); // Very short timeout

  try {
    await queue.add(() => {
      // Operation that just waits a bit longer than timeout
      return new Promise((resolve) =>
        setTimeout(() => resolve("should not complete"), 50)
      );
    });
    throw new Error("Should have timed out");
  } catch (error) {
    expect((error as Error).message).toEqual("Operation timeout after 30ms");
  }

  // Wait a bit to ensure all timers are cleaned up
  await new Promise((resolve) => setTimeout(resolve, 60));
});

test("MongoOperationQueue - Error handling", async () => {
  const queue = createQueueSystem();

  await expect(
    () =>
      queue.add(() => {
        throw new Error("Operation failed");
      }),
  ).rejects.toThrow("Operation failed");
});

test("MongoOperationQueue - Error isolation between operations", async () => {
  const queue = createQueueSystem({ maxConcurrent: 2 });
  const executionOrder: number[] = [];

  // Create operations where some succeed and some fail
  const createSuccessOperation = (id: number) => () => {
    executionOrder.push(id);
    return Promise.resolve(`success-${id}`);
  };

  const createFailOperation = (id: number) => () => {
    executionOrder.push(id);
    throw new Error(`failure-${id}`);
  };

  // Mix successful and failing operations
  const promises = [
    queue.add(createSuccessOperation(1)),
    queue.add(createFailOperation(2)).catch((error) => error), // Catch to prevent unhandled rejection
    queue.add(createSuccessOperation(3)),
    queue.add(createFailOperation(4)).catch((error) => error), // Catch to prevent unhandled rejection
    queue.add(createSuccessOperation(5)),
  ];

  const results = await Promise.all(promises);

  // All operations should have executed
  expect(executionOrder.sort()).toEqual([1, 2, 3, 4, 5]);

  // Check that successful operations return correct results
  expect(results[0]).toEqual("success-1");
  expect(results[2]).toEqual("success-3");
  expect(results[4]).toEqual("success-5");

  // Check that failed operations return errors
  expect((results[1] as Error).message).toEqual("failure-2");
  expect((results[3] as Error).message).toEqual("failure-4");
});

test("MongoOperationQueue - Retry functionality", async () => {
  const queue = createQueueSystem({
    retry: true,
    retryAttempts: 2,
    retryDelay: 1, // Very short delay
  });

  let attempts = 0;

  const result = await queue.add(() => {
    attempts++;
    if (attempts < 2) {
      throw new Error("Temporary failure");
    }
    return Promise.resolve("success after retry");
  });

  expect(result).toEqual("success after retry");
  expect(attempts).toEqual(2);
});

test("MongoOperationQueue - Stats tracking", async () => {
  const queue = createQueueSystem({ maxConcurrent: 1 });

  // Wait a bit to avoid timer conflicts from previous tests
  await new Promise((resolve) => setTimeout(resolve, 10));

  // Initial stats
  let stats = queue.getStats();
  expect(stats.pending).toEqual(0);
  expect(stats.running).toEqual(0);
  expect(stats.completed).toEqual(0);
  expect(stats.failed).toEqual(0);

  // Add operations
  const promise1 = queue.add(() => {
    return new Promise((resolve) => setTimeout(() => resolve("op1"), 30));
  });

  const promise2 = queue.add(() => {
    return Promise.resolve("op2");
  });

  // Check stats while operations are pending/running
  await new Promise((resolve) => setTimeout(resolve, 5));
  stats = queue.getStats();
  expect(stats.running).toEqual(1);
  expect(stats.pending).toEqual(1);

  // Wait for completion
  await Promise.all([promise1, promise2]);

  // Final stats
  stats = queue.getStats();
  expect(stats.pending).toEqual(0);
  expect(stats.running).toEqual(0);
  expect(stats.completed).toEqual(2);
  expect(stats.failed).toEqual(0);
});

test("MongoOperationQueue - Drain functionality", async () => {
  const queue = createQueueSystem({ maxConcurrent: 1 });

  // Wait a bit to avoid timer conflicts from previous tests
  await new Promise((resolve) => setTimeout(resolve, 5));

  // Add multiple operations
  const promises = [
    queue.add(() => {
      return new Promise((resolve) => setTimeout(() => resolve("op1"), 25));
    }),
    queue.add(() => {
      return new Promise((resolve) => setTimeout(() => resolve("op2"), 25));
    }),
  ];

  // Drain should wait for all to complete
  const startTime = Date.now();
  await queue.drain();
  const endTime = Date.now();

  // Should take at least 50ms (2 operations x 25ms each with concurrency 1)
  const duration = endTime - startTime;
  expect(duration >= 45).toEqual(true); // Small buffer for timing variations

  // Ensure all promises complete
  const results = await Promise.all(promises);
  expect(results).toEqual(["op1", "op2"]);

  // Queue should be empty
  const stats = queue.getStats();
  expect(stats.pending).toEqual(0);
  expect(stats.running).toEqual(0);
});

test("MongoOperationQueue - Clear functionality", async () => {
  const queue = createQueueSystem({ maxConcurrent: 1 });

  // Add operations
  const promise1 = queue.add(() => {
    return new Promise((resolve) => setTimeout(() => resolve("op1"), 100));
  });

  const promise2 = queue.add(() => {
    return Promise.resolve("op2");
  });

  // Setup promise2 error handler before clearing to prevent uncaught promise rejection
  const promise2Result = promise2.catch((error) => error);

  // Wait a bit then clear
  await new Promise((resolve) => setTimeout(resolve, 10));
  queue.clear();

  // First operation should complete (already running)
  const result1 = await promise1;
  expect(result1).toEqual("op1");

  // Second operation should be rejected
  const error = await promise2Result;
  expect(error.message).toEqual("Queue cleared");
});
