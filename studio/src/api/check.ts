import { Worker } from "node:worker_threads";
import {
  type CheckReport,
  parseDocsPerCollection,
  parseRetention,
  runCheck,
} from "@diister/mongodbee/inspect";
import { digestWarnings, type WarningDigest } from "@diister/mongodbee/inspect";
import type { StudioContext } from "../context.ts";
import { jsonResponse, toJsonSafe } from "../http.ts";

export type CheckMode = "quick" | "normal" | "hard";

export interface CheckRunOptions {
  mode: CheckMode;
  last?: number;
  docs?: number;
  retention?: number;
  signal?: AbortSignal;
}

export type CheckEvent =
  | {
      type: "start";
      mode: CheckMode;
      last?: number;
      docs?: number;
      retention?: number;
      inMemory: true;
    }
  | { type: "schema"; valid: boolean; errors: string[]; warnings: string[] }
  | {
      type: "migration-start";
      index: number;
      total: number;
      id: string;
      name: string;
    }
  | {
      type: "migration-result";
      index: number;
      total: number;
      id: string;
      name: string;
      valid: boolean;
      errors: string[];
      warnings: string[];
      operationCount?: number;
      reversible?: boolean;
      durationMs: number;
    }
  | {
      type: "done";
      report: SerializedCheckReport;
      digest: WarningDigest;
      durationMs: number;
    }
  | { type: "error"; message: string };

export type SerializedCheckReport = Omit<CheckReport, "failure"> & {
  failure?: string;
};

let running: AbortController | null = null;

export function isCheckRunning(): boolean {
  return running !== null;
}

function serialize(report: CheckReport): SerializedCheckReport {
  const { failure, ...rest } = report;
  return failure ? { ...rest, failure: failure.message } : rest;
}

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

async function step(signal: AbortSignal | undefined): Promise<void> {
  await tick();
  signal?.throwIfAborted();
}

export type CheckTarget = Pick<StudioContext, "project">;

export async function runStudioCheck(
  context: CheckTarget,
  options: CheckRunOptions,
  emit: (event: CheckEvent) => void,
): Promise<SerializedCheckReport | undefined> {
  if (!context.project) {
    emit({
      type: "error",
      message: "The studio was started without a project to check",
    });
    return undefined;
  }
  const started = performance.now();
  let migrationStarted = started;
  emit({
    type: "start",
    mode: options.mode,
    last: options.last,
    docs: options.docs,
    retention: options.retention,
    inMemory: true,
  });
  await step(options.signal);
  try {
    const report = await runCheck(
      {
        cwd: context.project.cwd,
        configPath: context.project.configPath,
        mode: options.mode,
        last: options.last,
        docs: options.docs,
        retention: options.retention,
      },
      {
        log: () => {},
        write: () => {},
        tty: false,
        onSchemaResult: async (schema) => {
          emit({ type: "schema", ...schema });
          await step(options.signal);
        },
        onMigrationStart: async ({ index, total, migration }) => {
          migrationStarted = performance.now();
          emit({
            type: "migration-start",
            index,
            total,
            id: migration.id,
            name: migration.name,
          });
          await step(options.signal);
        },
        onMigrationResult: async ({ index, total, migration, result }) => {
          emit({
            type: "migration-result",
            index,
            total,
            id: migration.id,
            name: migration.name,
            valid: result.valid,
            errors: result.errors,
            warnings: result.warnings,
            operationCount: result.operationCount,
            reversible: result.reversible,
            durationMs: Math.round(performance.now() - migrationStarted),
          });
          await step(options.signal);
        },
      },
    );
    const serialized = serialize(report);
    const digest = digestWarnings(
      report.migrations.map((migration) => ({
        migrationId: migration.id,
        warnings: migration.warnings,
      })),
    );
    emit({
      type: "done",
      report: serialized,
      digest,
      durationMs: Math.round(performance.now() - started),
    });
    return serialized;
  } catch (error) {
    if (options.signal?.aborted) return undefined;
    emit({
      type: "error",
      message: error instanceof Error ? error.message : String(error),
    });
    return undefined;
  }
}

export function checkWorkerUrl(): URL {
  const extension = import.meta.url.endsWith(".ts") ? "ts" : "js";
  return new URL(`../check-worker.${extension}`, import.meta.url);
}

export function runCheckInWorker(
  context: CheckTarget,
  options: CheckRunOptions,
  emit: (event: CheckEvent) => void,
): Promise<void> {
  const { signal, ...rest } = options;
  if (!context.project) {
    return runStudioCheck(context, options, emit).then(() => undefined);
  }
  let worker: Worker;
  try {
    worker = new Worker(checkWorkerUrl(), {
      workerData: { project: { ...context.project }, options: rest },
    });
  } catch {
    return runStudioCheck(context, options, emit).then(() => undefined);
  }
  return new Promise<void>((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", stop);
      resolve();
    };
    const stop = () => {
      worker.terminate().finally(finish);
    };
    signal?.addEventListener("abort", stop, { once: true });
    worker.on("message", (event: CheckEvent) => emit(event));
    worker.on("error", (error: Error) => {
      emit({ type: "error", message: error.message });
      finish();
    });
    worker.on("exit", finish);
  });
}

export function streamCheck(context: StudioContext, url: URL): Response {
  const rawMode = url.searchParams.get("mode") ?? "normal";
  const mode: CheckMode =
    rawMode === "quick" || rawMode === "hard" ? rawMode : "normal";
  const lastRaw = Number.parseInt(url.searchParams.get("last") ?? "", 10);
  const last = Number.isFinite(lastRaw) && lastRaw > 0 ? lastRaw : undefined;
  let docs: number | undefined;
  let retention: number | undefined;
  try {
    docs = parseDocsPerCollection(url.searchParams.get("docs"));
    retention = parseRetention(url.searchParams.get("retention"));
  } catch (error) {
    return jsonResponse(
      { error: error instanceof Error ? error.message : String(error) },
      400,
    );
  }
  if (running) {
    return jsonResponse({ error: "A check is already running" }, 409);
  }
  const abort = new AbortController();
  running = abort;
  const release = () => {
    if (running === abort) running = null;
  };
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const emit = (event: CheckEvent) => {
        if (abort.signal.aborted) return;
        controller.enqueue(
          encoder.encode(
            `event: ${event.type}\ndata: ${JSON.stringify(toJsonSafe(event))}\n\n`,
          ),
        );
      };
      try {
        await runCheckInWorker(
          context,
          { mode, last, docs, retention, signal: abort.signal },
          emit,
        );
      } finally {
        release();
        if (!abort.signal.aborted) controller.close();
      }
    },
    cancel() {
      abort.abort();
      release();
    },
  });
  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-store",
      connection: "keep-alive",
    },
  });
}
