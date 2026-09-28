import { getContext, setContext } from "svelte";

const KEY = Symbol("mongodbee-studio");

const FALLBACK = {
  resolveType: () => undefined,
  openDocument: () => {},
  openScope: undefined,
  write: { enabled: false, reason: null },
  refreshOverview: () => {},
};

export function provideStudio(api) {
  setContext(KEY, api);
}

export function useStudio() {
  return getContext(KEY) ?? FALLBACK;
}
