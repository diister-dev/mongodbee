/**
 * Error detection utilities for MongoDB write conflicts
 */

/**
 * Checks if an error is a MongoDB write conflict error
 *
 * Write conflicts occur when multiple transactions attempt to modify
 * the same document concurrently. MongoDB throws this error to maintain
 * transaction isolation.
 *
 * @param error - The error to check
 * @returns true if the error is a write conflict
 */
export function isWriteConflictError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const message = error.message.toLowerCase();

  // Don't retry on application-level errors
  if (message.includes("no element")) {
    return false;
  }

  // Check for various write conflict error messages
  return (
    message.includes("write conflict") ||
    (message.includes("plan execution") && message.includes("write conflict")) ||
    message.includes("writeconflict") ||
    message.includes("transaction") && message.includes("aborted") ||
    // MongoDB error code for write conflicts
    (error as any).code === 112
  );
}

/**
 * Options for retry behavior
 */
export interface RetryOptions {
  /**
   * Maximum number of retry attempts
   * @default 3
   */
  maxRetries?: number;

  /**
   * Initial delay in milliseconds before the first retry
   * @default 50
   */
  initialDelay?: number;

  /**
   * Maximum delay in milliseconds between retries
   * @default 1000
   */
  maxDelay?: number;

  /**
   * Enable exponential backoff for delays
   * @default true
   */
  exponentialBackoff?: boolean;

  /**
   * Add random jitter to delays to prevent thundering herd
   * @default true
   */
  jitter?: boolean;

  /**
   * Custom function to determine if an error should trigger a retry
   * @default isWriteConflictError
   */
  shouldRetry?: (error: unknown) => boolean;

  /**
   * Callback invoked before each retry attempt
   */
  onRetry?: (error: Error, attempt: number, delay: number) => void;
}

/**
 * Retries an operation when write conflicts occur
 *
 * This function automatically retries MongoDB operations that fail due to
 * write conflicts, using exponential backoff with jitter to avoid
 * overwhelming the database.
 *
 * @param operation - The async function to retry
 * @param options - Retry configuration options
 * @returns The result of the operation
 * @throws The last error if all retries are exhausted
 *
 * @example
 * ```typescript
 * const result = await retryOnWriteConflict(
 *   async () => {
 *     return await collection.updateOne({ _id: id }, { $set: data });
 *   },
 *   { maxRetries: 5 }
 * );
 * ```
 */
export async function retryOnWriteConflict<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const {
    maxRetries = 3,
    initialDelay = 50,
    maxDelay = 1000,
    exponentialBackoff = true,
    jitter = true,
    shouldRetry = isWriteConflictError,
    onRetry,
  } = options;

  let lastError: Error | undefined;
  let attempt = 0;

  while (attempt <= maxRetries) {
    try {
      // Try to execute the operation
      return await operation();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      // Check if we should retry this error
      if (!shouldRetry(error)) {
        throw error;
      }

      // If we've exhausted retries, throw the error
      if (attempt >= maxRetries) {
        throw error;
      }

      // Calculate delay for next retry
      let delayMs = initialDelay;

      if (exponentialBackoff) {
        // Exponential backoff: delay = initialDelay * 2^attempt
        delayMs = Math.min(initialDelay * Math.pow(2, attempt), maxDelay);
      }

      // Add jitter to prevent thundering herd problem
      if (jitter) {
        // Add random jitter up to 20% of the delay
        const jitterAmount = delayMs * 0.2 * Math.random();
        delayMs = delayMs + jitterAmount;
      }

      // Call retry callback if provided
      if (onRetry) {
        onRetry(lastError, attempt + 1, delayMs);
      }

      // Wait before retrying with proper cleanup
      await new Promise((resolve) => setTimeout(resolve, delayMs));

      attempt++;
    }
  }

  // This should never be reached, but TypeScript needs it
  throw lastError || new Error("Max retries reached");
}

/**
 * Creates a retry wrapper function with preset options
 *
 * This is useful when you want to apply the same retry logic
 * to multiple operations without repeating the options.
 *
 * @param options - Default retry options
 * @returns A function that wraps operations with retry logic
 *
 * @example
 * ```typescript
 * const retryDB = createRetryWrapper({ maxRetries: 5, initialDelay: 100 });
 *
 * const result1 = await retryDB(() => collection.updateOne(...));
 * const result2 = await retryDB(() => collection.insertOne(...));
 * ```
 */
export function createRetryWrapper(options: RetryOptions = {}) {
  return <T>(operation: () => Promise<T>): Promise<T> => {
    return retryOnWriteConflict(operation, options);
  };
}
