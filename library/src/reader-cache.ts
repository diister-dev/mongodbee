import {
  currentRequestScope,
  onUntrackedWrite,
  type RequestScope,
} from "./request-context.ts";
import { deferToCommit } from "./transaction-scope.ts";

export interface EntryFootprint {
  readonly collection: string;
  readonly type: string | undefined;
  readonly scope: string | undefined;
  readonly paths: readonly string[];
}

export interface ReaderEntry {
  readonly key: string;
  readonly owner: ReaderCache;
  readonly database: string;
  readonly footprint: EntryFootprint | undefined;
  readonly dependents: Set<ReaderEntry>;
  readonly dependencies: Set<ReaderEntry>;
  readonly promise: Promise<unknown>;
  stale: boolean;
}

export interface WriteFootprint {
  readonly database: string;
  readonly collection: string;
  readonly types: readonly string[] | "*";
  readonly scopes: readonly string[] | "*";
  readonly touched: readonly string[] | "all";
}

export interface ReaderStats {
  hits: number;
  loads: number;
  bypasses: number;
  invalidations: number;
  discarded: number;
  primed: number;
}

export interface ReaderCache {
  readonly entries: Map<string, ReaderEntry>;
  readonly stats: ReaderStats;
  writes: number;
}

const caches = new WeakMap<RequestScope, ReaderCache>();

function emptyStats(): ReaderStats {
  return {
    hits: 0,
    loads: 0,
    bypasses: 0,
    invalidations: 0,
    discarded: 0,
    primed: 0,
  };
}

export function requestReaderCache(): ReaderCache | undefined {
  const scope = currentRequestScope();
  if (!scope) return undefined;
  const existing = caches.get(scope);
  if (existing) return existing;
  const created: ReaderCache = {
    entries: new Map(),
    stats: emptyStats(),
    writes: 0,
  };
  caches.set(scope, created);
  return created;
}

export function requestReaderStats(): ReaderStats | undefined {
  const scope = currentRequestScope();
  if (!scope) return undefined;
  return { ...(caches.get(scope)?.stats ?? emptyStats()) };
}

function release(entry: ReaderEntry): void {
  entry.stale = true;
  if (entry.owner.entries.get(entry.key) === entry)
    entry.owner.entries.delete(entry.key);
  for (const dependency of entry.dependencies)
    dependency.dependents.delete(entry);
  entry.dependencies.clear();
  const dependents = [...entry.dependents];
  entry.dependents.clear();
  for (const dependent of dependents) invalidateEntry(dependent);
}

export function invalidateEntry(entry: ReaderEntry): void {
  if (entry.stale) return;
  entry.owner.stats.invalidations++;
  release(entry);
}

export function forgetEntry(entry: ReaderEntry): void {
  if (entry.stale) return;
  release(entry);
}

export function linkEntries(inner: ReaderEntry, outer: ReaderEntry): boolean {
  if (inner.owner !== outer.owner) return false;
  if (outer.stale) return true;
  if (inner.stale) {
    invalidateEntry(outer);
    return true;
  }
  inner.dependents.add(outer);
  outer.dependencies.add(inner);
  return true;
}

function overlaps(a: string, b: string): boolean {
  return a === b || a.startsWith(`${b}.`) || b.startsWith(`${a}.`);
}

function matches(entry: ReaderEntry, write: WriteFootprint): boolean {
  const footprint = entry.footprint;
  if (!footprint) return false;
  if (entry.database !== write.database) return false;
  if (footprint.collection !== write.collection) return false;
  if (
    write.types !== "*" &&
    footprint.type !== undefined &&
    !write.types.includes(footprint.type)
  )
    return false;
  if (
    write.scopes !== "*" &&
    footprint.scope !== undefined &&
    !write.scopes.includes(footprint.scope)
  )
    return false;
  if (write.touched === "all") return true;
  const touched = write.touched;
  return footprint.paths.some((path) =>
    touched.some((candidate) => overlaps(candidate, path)),
  );
}

function requestCaches(): ReaderCache[] {
  return cachesOf([...requestScopes()]);
}

function invalidateIn(
  targets: readonly ReaderCache[],
  select: (entry: ReaderEntry) => boolean,
): void {
  for (const cache of targets) {
    cache.writes++;
    for (const entry of [...cache.entries.values()]) {
      if (select(entry)) invalidateEntry(entry);
    }
  }
}

export function invalidateReadersFor(writes: readonly WriteFootprint[]): void {
  if (writes.length === 0) return;
  invalidateIn(requestCaches(), (entry) =>
    writes.some((write) => matches(entry, write)),
  );
}

export function invalidateReadersAtCommit(
  writes: readonly WriteFootprint[],
): void {
  if (writes.length === 0) return;
  const targets = [...requestScopes()];
  deferToCommit(() =>
    invalidateIn(cachesOf(targets), (entry) =>
      writes.some((write) => matches(entry, write)),
    ),
  );
}

function* requestScopes(): Generator<RequestScope> {
  for (
    let scope = currentRequestScope();
    scope !== undefined;
    scope = scope.parent
  )
    yield scope;
}

function cachesOf(scopes: readonly RequestScope[]): ReaderCache[] {
  return scopes.flatMap((scope) => {
    const cache = caches.get(scope);
    return cache ? [cache] : [];
  });
}

export function invalidateAllReaders(): void {
  invalidateIn(requestCaches(), () => true);
  const targets = [...requestScopes()];
  if (targets.length > 0)
    deferToCommit(() => invalidateIn(cachesOf(targets), () => true));
}

onUntrackedWrite(invalidateAllReaders);
