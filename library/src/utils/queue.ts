/**
 * Generic queue system for managing MongoDB operations concurrency
 * 
 * This module provides a simple queue system to control the number of concurrent
 * MongoDB operations, particularly useful for index creation operations which
 * have built-in concurrency limits in MongoDB.
 */

export interface QueueOptions {
    /** Maximum number of concurrent operations (default: 3) */
    maxConcurrent?: number;
    /** Default timeout for operations in milliseconds (default: 30000) */
    defaultTimeout?: number;
    /** Whether to retry failed operations (default: false) */
    retry?: boolean;
    /** Number of retry attempts (default: 1) */
    retryAttempts?: number;
    /** Delay between retries in milliseconds (default: 1000) */
    retryDelay?: number;
}

export interface QueueTask<T = unknown> {
    id: string;
    operation: () => Promise<T>;
    priority?: number;
    timeout?: number;
    resolve: (value: T) => void;
    reject: (error: Error) => void;
    attempts?: number;
}

export interface QueueStats {
    pending: number;
    running: number;
    completed: number;
    failed: number;
}

/**
 * Queue system for managing concurrent MongoDB operations
 */
export class MongoOperationQueue {
    private readonly maxConcurrent: number;
    private readonly defaultTimeout: number;
    private readonly retry: boolean;
    private readonly retryAttempts: number;
    private readonly retryDelay: number;
    
    private readonly pendingTasks: QueueTask[] = [];
    private readonly runningTasks = new Set<QueueTask>();
    private completedCount = 0;
    private failedCount = 0;
    private taskIdCounter = 0;

    constructor(options: QueueOptions = {}) {
        this.maxConcurrent = options.maxConcurrent ?? 3;
        this.defaultTimeout = options.defaultTimeout ?? 30000;
        this.retry = options.retry ?? false;
        this.retryAttempts = options.retryAttempts ?? 1;
        this.retryDelay = options.retryDelay ?? 1000;
    }

    /**
     * Add an operation to the queue
     * 
     * @param operation - Async function to execute
     * @param options - Task-specific options
     * @returns Promise that resolves when the operation completes
     */
    add<T>(
        operation: () => Promise<T>,
        options: {
            priority?: number;
            timeout?: number;
        } = {}
    ): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            const task: QueueTask<T> = {
                id: `task_${++this.taskIdCounter}`,
                operation,
                priority: options.priority ?? 0,
                timeout: options.timeout ?? this.defaultTimeout,
                resolve,
                reject,
                attempts: 0,
            };

            // Insert task based on priority (higher priority first)
            const insertIndex = this.pendingTasks.findIndex(
                (t) => (t.priority ?? 0) < (task.priority ?? 0)
            );
            
            if (insertIndex === -1) {
                this.pendingTasks.push(task as QueueTask);
            } else {
                this.pendingTasks.splice(insertIndex, 0, task as QueueTask);
            }

            // Try to process the queue
            this.processQueue();
        });
    }

    /**
     * Get current queue statistics
     */
    getStats(): QueueStats {
        return {
            pending: this.pendingTasks.length,
            running: this.runningTasks.size,
            completed: this.completedCount,
            failed: this.failedCount,
        };
    }

    /**
     * Wait for all operations to complete
     */
    async drain(): Promise<void> {
        while (this.pendingTasks.length > 0 || this.runningTasks.size > 0) {
            await new Promise(resolve => setTimeout(resolve, 10));
        }
    }

    /**
     * Clear all pending tasks
     */
    clear(): void {
        const pendingTasks = [...this.pendingTasks];
        this.pendingTasks.length = 0;
        
        // Reject all pending tasks
        pendingTasks.forEach(task => {
            task.reject(new Error('Queue cleared'));
        });
    }

    private processQueue(): void {
        while (
            this.pendingTasks.length > 0 && 
            this.runningTasks.size < this.maxConcurrent
        ) {
            const task = this.pendingTasks.shift()!;
            this.runningTasks.add(task);
            
            // Execute task without awaiting to allow concurrent execution
            this.executeTask(task);
        }
    }

    private async executeTask<T>(task: QueueTask<T>): Promise<void> {
        task.attempts = (task.attempts ?? 0) + 1;

        let timeoutId: number | undefined;

        try {
            // Set up timeout
            const timeoutPromise = new Promise<never>((_, reject) => {
                timeoutId = setTimeout(() => {
                    reject(new Error(`Operation timeout after ${task.timeout}ms`));
                }, task.timeout);
            });

            // Race between operation and timeout
            const result = await Promise.race([
                task.operation(),
                timeoutPromise
            ]);

            // Clear timeout on success
            if (timeoutId !== undefined) {
                clearTimeout(timeoutId);
            }

            // Success
            this.runningTasks.delete(task as QueueTask);
            this.completedCount++;
            task.resolve(result);
            
        } catch (error) {
            // Clear timeout on error
            if (timeoutId !== undefined) {
                clearTimeout(timeoutId);
            }

            // Check if we should retry
            if (
                this.retry && 
                task.attempts! < this.retryAttempts &&
                error instanceof Error &&
                !error.message.includes('timeout')
            ) {
                // Retry after delay
                setTimeout(() => {
                    this.executeTask(task);
                }, this.retryDelay);
                return;
            }

            // Failed permanently
            this.runningTasks.delete(task as QueueTask);
            this.failedCount++;
            task.reject(error instanceof Error ? error : new Error(String(error)));
        }

        // Continue processing queue
        this.processQueue();
    }
}

/**
 * Create a new MongoDB operation queue
 * 
 * @param options - Queue configuration options
 * @returns A new MongoOperationQueue instance
 * 
 * @example
 * ```typescript
 * const mongoOperationQueue = createQueueSystem({ maxConcurrent: 2 });
 * 
 * await mongoOperationQueue.add(() => {
 *     return collection.createIndex({ name: 1 });
 * });
 * ```
 */
export function createQueueSystem(options: QueueOptions = {}): MongoOperationQueue {
    return new MongoOperationQueue(options);
}
