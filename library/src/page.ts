import type { Logger } from "./utils/logger.ts";

/** One page of a `paginate()` call, on every collection kind. */
export type Page<R> = {
  /** Total documents matching the query, omitted with `skipTotal`. */
  total?: number;
  /** 0-based count of documents before this page's first row, omitted with `skipTotal`. */
  position?: number;
  data: R[];
  /** Present only when `peek` was requested. */
  hasMore?: boolean;
  /**
   * Stored documents this page left out because they fail the schema.
   * Present only when some were: a list keeps serving the valid rows, and
   * the caller can tell the page is short. `findInvalid()` lists them.
   */
  skippedInvalid?: number;
};

export function warnSkippedInvalid(
  log: Logger,
  collectionName: string,
  skipped: number,
  operation = "paginate",
): void {
  if (skipped === 0) return;
  log.warn(
    `${operation}(${collectionName}): skipped ${skipped} stored document(s) that fail the schema`,
  );
}
