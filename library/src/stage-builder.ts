/**
 * @fileoverview Shared pieces of the aggregation stage builders exposed by
 * `paginate({ pipeline })` / `aggregate()` on the multi and scoped surfaces.
 */

/**
 * `$$localValue` carries the correlated join key of a `lookup`/`anyLookup`
 * sub-pipeline: the library binds it to the local field and the sub-pipeline's
 * base `$match` compares the foreign field against it. A user `let` that
 * redefines it would silently REPOINT the join — every joined array quietly
 * wrong, no error anywhere — so it is refused loudly.
 */
export function assertLetDoesNotShadowJoinBinding(
  userLet: Record<string, unknown> | undefined,
): void {
  if (userLet && "localValue" in userLet) {
    throw new Error(
      "lookup: `let.localValue` is reserved — it carries the correlated " +
        "join key of the lookup sub-pipeline; rename the variable",
    );
  }
}
