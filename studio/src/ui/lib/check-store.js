import { get, writable } from "svelte/store";

const initial = () => ({
  status: "idle",
  mode: "normal",
  last: 0,
  docs: "",
  retention: "",
  schema: null,
  rows: [],
  report: null,
  digest: null,
  error: null,
  durationMs: null,
  startedAt: null,
});

export const checkState = writable(initial());
export const checkRequest = writable(0);

let source = null;

export function requestCheck() {
  checkRequest.update((n) => n + 1);
}

export function stopCheck() {
  const state = get(checkState);
  if (state.status !== "running") return;
  source?.close();
  source = null;
  checkState.set({
    ...state,
    status: "stopped",
    durationMs: state.startedAt === null ? null : Math.round(performance.now() - state.startedAt),
    rows: state.rows.map((row) =>
      row.status === "waiting" || row.status === "running" ? { ...row, status: "skipped" } : row,
    ),
  });
}

export function startCheck({ mode, last, docs = "", retention = "", migrations }) {
  if (get(checkState).status === "running") return;
  source?.close();
  const window = last > 0 ? migrations.slice(-last) : migrations;
  checkState.set({
    ...initial(),
    status: "running",
    mode,
    last,
    docs,
    retention,
    startedAt: performance.now(),
    rows: window.map((migration) => ({
      id: migration.id,
      name: migration.name,
      fileName: migration.fileName,
      status: "waiting",
    })),
  });
  const params = new URLSearchParams({ mode });
  if (last > 0) params.set("last", String(last));
  if (docs !== "") params.set("docs", String(docs));
  if (retention !== "") params.set("retention", String(Number(retention) / 100));
  source = new EventSource(`/api/migrations/check?${params}`);

  const update = (fn) => checkState.update((state) => fn({ ...state }));
  const patchRow = (id, patch) =>
    update((state) => ({
      ...state,
      rows: state.rows.map((row) => (row.id === id ? { ...row, ...patch } : row)),
    }));

  source.addEventListener("schema", (event) => {
    const data = JSON.parse(event.data);
    update((state) => ({ ...state, schema: data }));
  });
  source.addEventListener("migration-start", (event) => {
    const data = JSON.parse(event.data);
    patchRow(data.id, { status: "running" });
  });
  source.addEventListener("migration-result", (event) => {
    const data = JSON.parse(event.data);
    patchRow(data.id, {
      status: data.valid ? "valid" : "failed",
      errors: data.errors,
      warnings: data.warnings,
      operationCount: data.operationCount,
      reversible: data.reversible,
      durationMs: data.durationMs,
      settledAt: Date.now(),
    });
  });
  source.addEventListener("done", (event) => {
    const data = JSON.parse(event.data);
    source?.close();
    source = null;
    update((state) => ({
      ...state,
      status: "done",
      report: data.report,
      digest: data.digest,
      durationMs: data.durationMs,
      rows: state.rows.map((row) =>
        row.status === "waiting" || row.status === "running" ? { ...row, status: "skipped" } : row,
      ),
    }));
  });
  source.addEventListener("error", (event) => {
    const message = event.data ? JSON.parse(event.data).message : "The check stream was interrupted";
    source?.close();
    source = null;
    update((state) =>
      state.status === "running" ? { ...state, status: "error", error: message } : state,
    );
  });
}
