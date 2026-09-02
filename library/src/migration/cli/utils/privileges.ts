/**
 * CLI wrapper around the migration privilege pre-flight
 *
 * Prints the outcome of {@link checkMigrationPrivileges} the way the other
 * pre-flight steps print theirs, and turns `status: "missing"` into a thrown
 * error so the command aborts before touching the database.
 *
 * @module
 */

import { bold, dim, green, red, yellow } from "@std/fmt/colors";
import type { Db } from "../../../mongodb.ts";
import {
  checkMigrationPrivileges,
  DB_ADMIN_ONLY_ACTIONS,
  type MigrationPrivilegeCheck,
} from "../../privileges.ts";

/**
 * Options for {@link ensureMigrationPrivileges}
 */
export interface EnsureMigrationPrivilegesOptions {
  /** `--skip-privilege-check`: do not query the server at all */
  skip?: boolean;
  /** `--dry-run`: report a missing privilege, do not abort on it */
  dryRun?: boolean;
  /** Actions to require (defaults to every action a migration run can need) */
  actions?: readonly string[];
}

/**
 * Verifies the connected account can run a migration, printing the result.
 *
 * - `ok`: one green line naming the account and its roles.
 * - `skipped`: one yellow line with the reason; the command proceeds — there
 *   is nothing to refuse when access control is disabled, and a server that
 *   cannot answer `connectionStatus` must not block a run that may succeed.
 * - `missing`: the missing actions, the role(s) that would grant them, and
 *   the exact `grantRolesToUser` call; then throws — unless `dryRun`, where
 *   nothing will be written anyway and the preview stays useful.
 *
 * @returns the check, or `undefined` when skipped through `skip`
 * @throws Error when a required action is missing and `dryRun` is not set
 */
export async function ensureMigrationPrivileges(
  db: Db,
  options: EnsureMigrationPrivilegesOptions = {},
): Promise<MigrationPrivilegeCheck | undefined> {
  if (options.skip) {
    console.log(
      dim("🔐 Account privilege check skipped (--skip-privilege-check)"),
    );
    console.log();
    return undefined;
  }

  const check = await checkMigrationPrivileges(db, {
    actions: options.actions,
  });

  if (check.status === "skipped") {
    console.log(yellow(`⚠ Account privileges not verified: ${check.reason}`));
    console.log();
    return check;
  }

  const account = check.users.map((u) => `${u.user}@${u.db}`).join(", ");
  const roles = check.roles.length > 0
    ? check.roles.map((r) => `${r.role}@${r.db}`).join(", ")
    : "no role";

  if (check.status === "ok") {
    console.log(
      green(`🔐 Account privileges verified ${dim(`(${account}: ${roles})`)}`),
    );
    console.log();
    return check;
  }

  const needsDbAdmin = check.missing.some((a) =>
    DB_ADMIN_ONLY_ACTIONS.includes(a)
  );
  const needsReadWrite = check.missing.some((a) =>
    !DB_ADMIN_ONLY_ACTIONS.includes(a)
  );
  const suggestedRoles = [
    ...(needsReadWrite ? ["readWrite"] : []),
    ...(needsDbAdmin ? ["dbAdmin"] : []),
  ];
  const primaryUser = check.users[0];

  console.log(
    red(bold("✗ Insufficient privileges: this account cannot run migrations")),
  );
  console.log(red(`  Account:  ${account}`));
  console.log(red(`  Roles:    ${roles}`));
  console.log(red(`  Database: ${check.database}`));
  console.log(red("  Missing actions on the database:"));
  for (const action of check.missing) {
    const scoped = check.collectionScoped[action];
    const note = scoped
      ? dim(` (granted only on collection(s): ${scoped.join(", ")})`)
      : "";
    console.log(red(`    - ${action}`) + note);
  }
  console.log();
  console.log(
    dim(
      "  A migration rewrites validators and indexes around every change, so it",
    ),
  );
  console.log(
    dim(
      "  needs DDL actions (collMod, createIndex, ...) on top of read/write ones.",
    ),
  );
  console.log(
    dim(
      `  Grant the built-in role(s) ${
        suggestedRoles.join(" + ")
      } on "${check.database}" (or dbOwner), e.g. in mongosh:`,
    ),
  );
  if (primaryUser) {
    console.log(dim(`    use ${primaryUser.db}`));
    console.log(
      dim(
        `    db.grantRolesToUser("${primaryUser.user}", [${
          suggestedRoles
            .map((role) => `{ role: "${role}", db: "${check.database}" }`)
            .join(", ")
        }])`,
      ),
    );
  }
  console.log(
    dim("  Or pass --skip-privilege-check to run without this verification."),
  );
  console.log();

  if (options.dryRun) {
    console.log(
      yellow("  [DRY RUN] Continuing — a real run would stop here."),
    );
    console.log();
    return check;
  }

  throw new Error(
    `Insufficient privileges: account ${account} is missing ${
      check.missing.join(", ")
    } on database "${check.database}". ` +
      `Grant ${suggestedRoles.join(" + ")} (or dbOwner) and retry.`,
  );
}
