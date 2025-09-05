import { createQueueSystem } from "./utils/queue.ts";

export const mongoOperationQueue = createQueueSystem({
    maxConcurrent: 1,
});