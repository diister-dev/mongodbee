import { writable } from "svelte/store";
import { api } from "./api.js";

const BATCH = 100;
const DELAY_MS = 40;

export const labels = writable({});

const known = new Map();
let queue = new Set();
let timer = null;

async function flush() {
  timer = null;
  const ids = [...queue];
  queue = new Set();
  for (let start = 0; start < ids.length; start += BATCH) {
    const chunk = ids.slice(start, start + BATCH);
    try {
      const result = await api("/api/labels", { id: chunk });
      const found = new Map(result.labels.map((item) => [item.id, item]));
      for (const id of chunk) known.set(id, found.get(id) ?? null);
    } catch {
      for (const id of chunk) known.set(id, null);
    }
    labels.set(Object.fromEntries(known));
  }
}

export function requestLabel(id) {
  if (typeof id !== "string" || known.has(id) || queue.has(id)) return;
  queue.add(id);
  if (timer === null) timer = setTimeout(flush, DELAY_MS);
}

export function forgetLabels() {
  known.clear();
  labels.set({});
}
