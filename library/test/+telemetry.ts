// deno-lint-ignore-file no-import-prefix -- test-only OpenTelemetry SDK deps, kept out of the published import map
/**
 * Shared helpers for the telemetry test suites.
 *
 * Test-only OpenTelemetry dependencies are imported through direct npm:
 * specifiers on purpose: they must never enter the published import map of
 * the package (the library itself only depends on @opentelemetry/api).
 */
import { context } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  type ReadableSpan,
  SimpleSpanProcessor,
} from "npm:@opentelemetry/sdk-trace-base@^2.1.0";
import { AsyncLocalStorageContextManager } from "npm:@opentelemetry/context-async-hooks@^2.1.0";
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
