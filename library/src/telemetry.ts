import { trace, context, SpanStatusCode, SpanKind, Span } from "@opentelemetry/api";

/**
 * Configuration for telemetry
 */
export interface TelemetryConfig {
  enabled?: boolean;
  tracerName?: string;
  tracerVersion?: string;
}

/**
 * Global telemetry configuration
 */
let globalTelemetryConfig: TelemetryConfig = {
  enabled: true, // Default to true to check env variables
  tracerName: "mongodbee", 
  tracerVersion: "0.7.3",
};

/**
 * Check if telemetry is enabled via environment variable
 */
function isTelemetryEnabled(): boolean {
  // Check environment variable
  const otelEnabled = Deno.env.get("OTEL_DENO") === "true" || 
                     Deno.env.get("OTEL_SDK_DISABLED") === "false";
  return globalTelemetryConfig.enabled || otelEnabled;
}

/**
 * Set global telemetry configuration
 */
export function setTelemetryConfig(config: TelemetryConfig) {
  globalTelemetryConfig = { ...globalTelemetryConfig, ...config };
}

/**
 * Get the tracer instance
 */
function getTracer() {
  return trace.getTracer(
    globalTelemetryConfig.tracerName!,
    globalTelemetryConfig.tracerVersion
  );
}

/**
 * Create a span for a MongoDB operation
 */
export function createMongoSpan(
  operation: string,
  collection: string,
  dbName: string,
  attributes?: Record<string, any>
): Span | null {
  if (!isTelemetryEnabled()) {
    return null;
  }

  const tracer = getTracer();
  const span = tracer.startSpan(`mongodb.${operation}`, {
    kind: SpanKind.CLIENT,
    attributes: {
      "db.system": "mongodb",
      "db.operation": operation,
      "db.mongodb.collection": collection,
      "db.name": dbName,
      ...attributes,
    },
  });

  return span;
}

/**
 * Wrap a MongoDB operation with telemetry
 */
export async function withMongoSpan<T>(
  operation: string,
  collection: string,
  dbName: string,
  fn: () => Promise<T>,
  attributes?: Record<string, any>
): Promise<T> {
  if (!isTelemetryEnabled()) {
    return fn();
  }

  const span = createMongoSpan(operation, collection, dbName, attributes);
  if (!span) {
    return fn();
  }

  return context.with(trace.setSpan(context.active(), span), async () => {
    const startTime = performance.now();
    try {
      const result = await fn();
      const duration = performance.now() - startTime;

      span.setAttributes({
        "db.operation.duration_ms": duration,
        "db.operation.success": true,
      });

      // Add result metrics if available
      if (result && typeof result === "object") {
        const r = result as any;
        if (typeof r.matchedCount !== "undefined") {
          span.setAttribute("db.mongodb.matched_count", r.matchedCount);
        }
        if (typeof r.modifiedCount !== "undefined") {
          span.setAttribute("db.mongodb.modified_count", r.modifiedCount);
        }
        if (typeof r.deletedCount !== "undefined") {
          span.setAttribute("db.mongodb.deleted_count", r.deletedCount);
        }
        if (typeof r.insertedCount !== "undefined") {
          span.setAttribute("db.mongodb.inserted_count", r.insertedCount);
        }
        if (typeof r.acknowledged !== "undefined") {
          span.setAttribute("db.mongodb.acknowledged", r.acknowledged);
        }
        if (Array.isArray(result)) {
          span.setAttribute("db.mongodb.document_count", result.length);
        }
      }

      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (error) {
      span.recordException(error as Error);
      span.setAttributes({
        "db.operation.success": false,
        "db.operation.error": error instanceof Error ? error.message : "Unknown error",
      });
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: error instanceof Error ? error.message : "Database operation failed",
      });
      throw error;
    } finally {
      span.end();
    }
  });
}

/**
 * Wrap a cursor operation with telemetry
 */
export function wrapCursorWithTelemetry<T>(
  cursor: any,
  operation: string,
  collection: string,
  dbName: string,
  filter?: any
): any {
  if (!isTelemetryEnabled()) {
    return cursor;
  }

  const span = createMongoSpan(operation, collection, dbName, {
    "db.mongodb.filter": filter ? JSON.stringify(filter) : undefined,
  });

  if (!span) {
    return cursor;
  }

  const startTime = performance.now();

  // Wrap cursor methods
  const originalToArray = cursor.toArray;
  const originalNext = cursor.next;
  const originalForEach = cursor.forEach;

  // Helper to complete the span
  const completeSpan = (success: boolean, error?: Error, resultCount?: number) => {
    const duration = performance.now() - startTime;
    span.setAttributes({
      "db.operation.duration_ms": duration,
      "db.operation.success": success,
    });

    if (resultCount !== undefined) {
      span.setAttribute("db.mongodb.document_count", resultCount);
    }

    if (error) {
      span.recordException(error);
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: error.message,
      });
    } else {
      span.setStatus({ code: SpanStatusCode.OK });
    }

    span.end();
  };

  // Override toArray
  cursor.toArray = async function () {
    return context.with(trace.setSpan(context.active(), span), async () => {
      try {
        const results = await originalToArray.call(cursor);
        completeSpan(true, undefined, Array.isArray(results) ? results.length : 0);
        return results;
      } catch (error) {
        completeSpan(false, error as Error);
        throw error;
      }
    });
  };

  // Override next
  cursor.next = async function () {
    return context.with(trace.setSpan(context.active(), span), async () => {
      try {
        const result = await originalNext.call(cursor);
        // Don't complete span on next, as cursor might continue
        return result;
      } catch (error) {
        completeSpan(false, error as Error);
        throw error;
      }
    });
  };

  // Override forEach
  if (originalForEach) {
    cursor.forEach = async function (callback: any) {
      return context.with(trace.setSpan(context.active(), span), async () => {
        let count = 0;
        try {
          const result = await originalForEach.call(cursor, (doc: any) => {
            count++;
            return callback(doc);
          });
          completeSpan(true, undefined, count);
          return result;
        } catch (error) {
          completeSpan(false, error as Error);
          throw error;
        }
      });
    };
  }

  return cursor;
}

/**
 * Add filter details to span attributes
 */
export function addFilterAttributes(span: Span | null, filter: any) {
  if (!span || !filter) return;

  try {
    // Add filter details safely
    const filterStr = JSON.stringify(filter);
    if (filterStr.length <= 1000) {
      span.setAttribute("db.mongodb.filter", filterStr);
    } else {
      // For large filters, just indicate the keys
      span.setAttribute("db.mongodb.filter.keys", Object.keys(filter).join(","));
      span.setAttribute("db.mongodb.filter.truncated", true);
    }
  } catch {
    // Ignore serialization errors
  }
}

/**
 * Add update details to span attributes
 */
export function addUpdateAttributes(span: Span | null, update: any) {
  if (!span || !update) return;

  try {
    // Check if it's an update operator object
    const updateOps = Object.keys(update).filter(key => key.startsWith("$"));
    if (updateOps.length > 0) {
      span.setAttribute("db.mongodb.update.operators", updateOps.join(","));
    }

    // Add field count for $set operations
    if (update.$set) {
      span.setAttribute("db.mongodb.update.set_fields", Object.keys(update.$set).length);
    }
  } catch {
    // Ignore errors
  }
}