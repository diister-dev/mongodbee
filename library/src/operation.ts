import { createQueueSystem } from "./utils/queue.ts";

export const mongoOperationQueue = createQueueSystem({
    maxConcurrent: 2,
    defaultTimeout: 5000,
});