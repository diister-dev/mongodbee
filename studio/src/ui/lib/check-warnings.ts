export interface WarningRow {
  id: string;
  status: string;
  warnings?: readonly string[];
}

export interface SharedWarning {
  text: string;
  occurrences: number;
}

export interface SplitWarnings {
  shared: SharedWarning[];
  specific: Record<string, string[]>;
}

function finished(row: WarningRow): boolean {
  return row.status === "valid" || row.status === "failed";
}

export function splitWarnings(
  rows: readonly WarningRow[],
  settled = true,
): SplitWarnings {
  const unique = new Map<string, string[]>();
  for (const row of rows) unique.set(row.id, [...new Set(row.warnings ?? [])]);
  const ran = rows.filter(finished);
  const common = new Set<string>();
  if (settled && ran.length >= 2) {
    const [first, ...rest] = ran;
    for (const text of unique.get(first.id) ?? []) {
      if (rest.every((row) => unique.get(row.id)?.includes(text))) {
        common.add(text);
      }
    }
  }
  const shared: SharedWarning[] = [...common].map((text) => ({
    text,
    occurrences: ran.reduce(
      (sum, row) =>
        sum + (row.warnings ?? []).filter((warning) => warning === text).length,
      0,
    ),
  }));
  const specific: Record<string, string[]> = {};
  for (const row of rows) {
    specific[row.id] = (unique.get(row.id) ?? []).filter(
      (text) => !common.has(text),
    );
  }
  return { shared, specific };
}
