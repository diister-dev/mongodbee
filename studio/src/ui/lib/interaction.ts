export interface WarmState {
  isWarm(now: number): boolean;
  opened(): void;
  closed(now: number): void;
}

export function createWarmState(windowMs = 400): WarmState {
  let open = 0;
  let lastClosed = Number.NEGATIVE_INFINITY;
  return {
    isWarm(now: number) {
      return open > 0 || now - lastClosed <= windowMs;
    },
    opened() {
      open += 1;
    },
    closed(now: number) {
      open = Math.max(0, open - 1);
      lastClosed = now;
    },
  };
}

export type RowAction =
  | { type: "next" }
  | { type: "previous" }
  | { type: "first" }
  | { type: "last" }
  | { type: "clear" }
  | { type: "set"; index: number };

export function moveSelection(
  current: number | null,
  count: number,
  action: RowAction,
): number | null {
  if (count <= 0) return null;
  switch (action.type) {
    case "clear":
      return null;
    case "first":
      return 0;
    case "last":
      return count - 1;
    case "set":
      return Math.min(count - 1, Math.max(0, action.index));
    case "next":
      return current === null ? 0 : Math.min(count - 1, current + 1);
    case "previous":
      return current === null ? count - 1 : Math.max(0, current - 1);
  }
}

export function keyToRowAction(key: string): RowAction | null {
  switch (key) {
    case "j":
    case "ArrowDown":
      return { type: "next" };
    case "k":
    case "ArrowUp":
      return { type: "previous" };
    case "Home":
      return { type: "first" };
    case "End":
      return { type: "last" };
    default:
      return null;
  }
}

export interface OperationLike {
  type: string;
}

export interface OperationGroup<T extends OperationLike> {
  verb: string;
  noun: string;
  items: T[];
  label: string;
}

const VERBS: Record<string, string> = {
  create: "Created",
  seed: "Seeded",
  transform: "Transformed",
  delete: "Deleted",
  dedupe: "Deduplicated",
  rename: "Renamed",
  update: "Updated",
  mark: "Marked",
  flow: "Moved",
};

export function operationFamily(type: string): { verb: string; noun: string } {
  const [head] = type.split("_");
  const verb = VERBS[head] ?? head.charAt(0).toUpperCase() + head.slice(1);
  if (head === "create") return { verb, noun: "collection" };
  if (head === "update") return { verb, noun: "index set" };
  if (head === "flow") return { verb, noun: "flow" };
  if (type.includes("documents")) return { verb, noun: "document set" };
  return { verb, noun: "target" };
}

export function groupOperations<T extends OperationLike>(
  operations: readonly T[],
): OperationGroup<T>[] {
  const groups: OperationGroup<T>[] = [];
  for (const operation of operations) {
    const { verb, noun } = operationFamily(operation.type);
    const last = groups[groups.length - 1];
    if (last && last.verb === verb && last.noun === noun) {
      last.items.push(operation);
    } else {
      groups.push({ verb, noun, items: [operation], label: "" });
    }
  }
  for (const group of groups) {
    const count = group.items.length;
    group.label = `${group.verb} ${count} ${count === 1 ? group.noun : pluralNoun(group.noun)}`;
  }
  return groups;
}

function pluralNoun(noun: string): string {
  if (noun.endsWith("x")) return `${noun}es`;
  return `${noun}s`;
}

export function matchScore(query: string, text: string): number {
  const q = query.trim().toLowerCase();
  if (!q) return 1;
  const t = text.toLowerCase();
  const index = t.indexOf(q);
  if (index === 0) return 100 - Math.min(50, t.length - q.length);
  if (index > 0) return 60 - Math.min(40, index);
  let position = 0;
  let gaps = 0;
  for (const char of q) {
    const found = t.indexOf(char, position);
    if (found < 0) return 0;
    gaps += found - position;
    position = found + 1;
  }
  return Math.max(1, 30 - gaps);
}

export interface RankableOption {
  label: string;
  hint?: string;
  group?: string;
}

export function rankOptions<T extends RankableOption>(
  options: readonly T[],
  query: string,
): T[] {
  const q = query.trim();
  const scored = options
    .map((option, order) => ({
      option,
      order,
      score: q
        ? Math.max(
            matchScore(q, option.label),
            matchScore(q, option.hint ?? "") * 0.5,
          )
        : 1,
    }))
    .filter((entry) => entry.score > 0);
  if (q) scored.sort((a, b) => b.score - a.score || a.order - b.order);
  const groups = new Map<string, T[]>();
  for (const { option } of scored) {
    const key = option.group ?? "";
    const bucket = groups.get(key) ?? [];
    bucket.push(option);
    groups.set(key, bucket);
  }
  return [...groups.values()].flat();
}
