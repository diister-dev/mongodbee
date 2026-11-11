import type { Db } from "../mongodb.ts";
import { PRIMARY } from "../read-preference.ts";
import {
  planTimeSeriesChange,
  storedTimeSeriesOptions,
  type TimeSeriesChangePlan,
  TimeSeriesOptionsDriftError,
  type TimeSeriesOptions,
  timeSeriesCreateOptions,
} from "../time-series.ts";
import { createLogger } from "./logger.ts";

const log = createLogger("time-series");

/** MongoDB's error code when `create` targets a namespace that already exists. */
const NAMESPACE_EXISTS = 48;

/** What the database holds under a time series collection's name. */
export type TimeSeriesInspection =
  | { readonly state: "missing" }
  | {
      /** A regular collection or a view holds the name. */
      readonly state: "not-time-series";
      readonly type: string;
    }
  | {
      readonly state: "exists";
      readonly stored: TimeSeriesOptions;
      /** From the stored options to the declared ones. */
      readonly plan: TimeSeriesChangePlan;
    };

/**
 * Compares the time series collection `collectionName` with `options`,
 * without changing anything: the drift check behind `ensureTimeSeries`, the
 * migrations and whoever wants to report it.
 */
export async function inspectTimeSeries(
  db: Db,
  collectionName: string,
  options: TimeSeriesOptions,
): Promise<TimeSeriesInspection> {
  const [info] = await db
    .listCollections({ name: collectionName }, { readPreference: PRIMARY })
    .toArray();
  if (!info) return { state: "missing" };
  const stored = storedTimeSeriesOptions(
    (info as { options?: unknown }).options,
  );
  if (info.type !== "timeseries" || stored === undefined) {
    return { state: "not-time-series", type: info.type ?? "collection" };
  }
  return {
    state: "exists",
    stored,
    plan: planTimeSeriesChange(stored, options),
  };
}

function notTimeSeries(
  collectionName: string,
  type: string,
): TimeSeriesOptionsDriftError {
  return new TimeSeriesOptionsDriftError(collectionName, [
    `"${collectionName}" already exists as a ${type === "view" ? "view" : "regular collection"}, and MongoDB cannot convert it into a time series collection`,
  ]);
}

/**
 * Applies an existing collection's applicable differences with `collMod`;
 * throws {@link TimeSeriesOptionsDriftError} when one cannot be applied,
 * before changing anything.
 */
export async function applyTimeSeriesPlan(
  db: Db,
  collectionName: string,
  plan: TimeSeriesChangePlan,
): Promise<void> {
  if (plan.blocked.length > 0) {
    throw new TimeSeriesOptionsDriftError(collectionName, plan.blocked);
  }
  for (const command of plan.commands) {
    await db.command({ collMod: collectionName, ...command });
  }
  if (plan.changes.length > 0) {
    log.info(
      `time series "${collectionName}": ${plan.changes.join("; ")} (collMod)`,
    );
  }
}

/**
 * Brings `collectionName` to `options`: creates the time series collection
 * when it is missing, otherwise applies what `collMod` accepts
 * (`expireAfterSeconds`, wider buckets). A difference MongoDB cannot apply
 * (`timeField`, `metaField`, narrower buckets, a regular collection under
 * the name) throws {@link TimeSeriesOptionsDriftError} and changes nothing.
 *
 * Two processes booting together may both try to create it; as in
 * `ensureValidator`, the loser compares what the winner created.
 */
export async function ensureTimeSeries(
  db: Db,
  collectionName: string,
  options: TimeSeriesOptions,
): Promise<void> {
  let inspection = await inspectTimeSeries(db, collectionName, options);
  if (inspection.state === "missing") {
    log.debug(`ensureTimeSeries(${collectionName}): createCollection`);
    try {
      await db.createCollection(
        collectionName,
        timeSeriesCreateOptions(options),
      );
      return;
    } catch (error) {
      if ((error as { code?: unknown }).code !== NAMESPACE_EXISTS) throw error;
      inspection = await inspectTimeSeries(db, collectionName, options);
    }
  }
  if (inspection.state === "missing") {
    throw new Error(
      `ensureTimeSeries(${collectionName}): the collection was reported as existing and cannot be listed`,
    );
  }
  if (inspection.state === "not-time-series") {
    throw notTimeSeries(collectionName, inspection.type);
  }
  await applyTimeSeriesPlan(db, collectionName, inspection.plan);
}
