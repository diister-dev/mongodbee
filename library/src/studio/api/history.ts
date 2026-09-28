import { getAllOperations } from "../../migration/history.ts";
import type { StudioContext } from "../context.ts";

export const HISTORY_LIMIT = 200;

export interface HistoryEntry {
  migrationId: string;
  migrationName: string;
  operation: "applied" | "reverted" | "failed";
  status: "success" | "failure";
  executedAt: Date;
  duration?: number;
  error?: string;
  adopted?: true;
  mongodbeeVersion: string;
  knownFile: boolean;
}

export interface HistoryReport {
  total: number;
  shown: number;
  applied: number;
  reverted: number;
  failed: number;
  entries: HistoryEntry[];
}

export async function getHistory(
  context: StudioContext,
): Promise<HistoryReport> {
  const operations = await getAllOperations(context.db);
  const known = new Set(context.migrations.map((migration) => migration.id));
  const recent = operations.slice(-HISTORY_LIMIT).reverse();
  return {
    total: operations.length,
    shown: recent.length,
    applied: operations.filter(
      (op) => op.operation === "applied" && op.status === "success",
    ).length,
    reverted: operations.filter(
      (op) => op.operation === "reverted" && op.status === "success",
    ).length,
    failed: operations.filter(
      (op) => op.status === "failure" || op.operation === "failed",
    ).length,
    entries: recent.map((op) => {
      const entry: HistoryEntry = {
        migrationId: op.migrationId,
        migrationName: op.migrationName,
        operation: op.operation,
        status: op.status,
        executedAt: op.executedAt,
        mongodbeeVersion: op.mongodbeeVersion,
        knownFile: known.has(op.migrationId),
      };
      if (op.duration !== undefined) entry.duration = op.duration;
      if (op.error) entry.error = op.error;
      if (op.adopted) entry.adopted = true;
      return entry;
    }),
  };
}
