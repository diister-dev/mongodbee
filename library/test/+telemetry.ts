/**
 * Shared helpers for the telemetry test suites.
 *
 * The OpenTelemetry SDK packages below are devDependencies on purpose: they
 * must never reach the published dependency list, because the library itself
 * only ever depends on @opentelemetry/api.
 */
import { context } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  type ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import type { TelemetryOptions } from "../telemetry.ts";

// Span parenting relies on context propagation, and the bare @opentelemetry/api
// ships a NoopContextManager (context.active() is always the root context).
// Registering a real AsyncLocalStorage-based manager is what an application
// SDK would normally do. Each test file runs in its own worker, so this does
// not leak into unrelated test files; a duplicate registration in the same
// worker keeps the first (equivalent) manager.
context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());

/** Everything a telemetry test needs: an isolated provider and its exporter. */
export interface TestTelemetry {
  /** Collects finished spans synchronously (SimpleSpanProcessor). */
  exporter: InMemorySpanExporter;
  /** Provider wired to the exporter — never registered globally. */
  provider: BasicTracerProvider;
  /** Ready-to-use options for the collection factories. */
  telemetry: TelemetryOptions;
}

/**
 * Creates an isolated in-memory tracing setup. Call it inside each test so
 * spans never bleed between tests.
 */
export function makeTestTelemetry(): TestTelemetry {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  return {
    exporter,
    provider,
    telemetry: { enabled: true, tracerProvider: provider },
  };
}

/**
 * Serializes every recorded facet of the finished spans (name, attributes,
 * events including recorded exceptions, status) for PII scanning.
 */
export function dumpSpans(exporter: InMemorySpanExporter): string {
  return JSON.stringify(
    exporter.getFinishedSpans().map((span: ReadableSpan) => ({
      name: span.name,
      attributes: span.attributes,
      status: span.status,
      events: span.events.map((event) => ({
        name: event.name,
        attributes: event.attributes,
      })),
    })),
  );
}
