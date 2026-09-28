export interface PathNode {
  kind: string;
  entries?: Record<string, PathNode>;
  item?: PathNode;
  options?: PathNode[];
}

export const MAX_PATH_DEPTH = 4;

function objectBranches<T extends PathNode>(node: T): T[] {
  if (node.kind === "array" && node.item) return objectBranches(node.item as T);
  if (node.entries) return [node];
  if (node.options) {
    return node.options.flatMap((option) => objectBranches(option as T));
  }
  return [];
}

export function nodeAtPath<T extends PathNode>(
  root: Record<string, T>,
  path: string,
): T | undefined {
  const [head, ...rest] = path.split(".");
  let current = root[head];
  for (const segment of rest) {
    if (!current) return undefined;
    const next = objectBranches(current)
      .map((branch) => branch.entries?.[segment] as T | undefined)
      .find((found) => found !== undefined);
    current = next as T;
  }
  return current;
}

export function nestedPaths<T extends PathNode>(
  root: Record<string, T>,
  depth: number = MAX_PATH_DEPTH,
): { path: string; node: T }[] {
  const found: { path: string; node: T }[] = [];
  const walk = (prefix: string, node: T, level: number) => {
    if (level >= depth) return;
    const seen = new Set<string>();
    for (const branch of objectBranches(node)) {
      for (const [key, child] of Object.entries(branch.entries ?? {})) {
        if (seen.has(key)) continue;
        seen.add(key);
        const path = `${prefix}.${key}`;
        found.push({ path, node: child as T });
        walk(path, child as T, level + 1);
      }
    }
  };
  for (const [key, node] of Object.entries(root)) walk(key, node, 1);
  return found;
}
