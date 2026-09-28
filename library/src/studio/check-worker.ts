import { parentPort, workerData } from "node:worker_threads";
import { type CheckRunOptions, runStudioCheck } from "./api/check.ts";
import type { StudioContext } from "./context.ts";

interface CheckWorkerData {
  project: NonNullable<StudioContext["project"]>;
  options: Omit<CheckRunOptions, "signal">;
}

const data = workerData as CheckWorkerData;

await runStudioCheck(
  { project: data.project } as StudioContext,
  data.options,
  (event) => parentPort?.postMessage(event),
);
