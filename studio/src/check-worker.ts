import { parentPort, workerData } from "node:worker_threads";
import { type CheckRunOptions, runStudioCheck } from "./api/check.ts";
import type { StudioContext } from "./context.ts";

interface CheckWorkerData {
  project: NonNullable<StudioContext["project"]>;
  options: Omit<CheckRunOptions, "signal">;
}

const data: CheckWorkerData = workerData;

await runStudioCheck({ project: data.project }, data.options, (event) =>
  parentPort?.postMessage(event),
);
