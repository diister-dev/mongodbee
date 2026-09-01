import { assert, assertEquals } from "@std/assert";
import * as v from "../../src/schema.ts";
import { dbId } from "../../src/ids.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { validateLastMigrationMatchesProjectSchema } from "../../src/migration/schema-validation.ts";
import { buildPrivacyPlan, personal, personId } from "../../src/privacy/mod.ts";

const BEFORE = {
  collections: {
    users: {
      _id: dbId("user"),
      email: v.pipe(v.string(), v.email()),
      note: v.string(),
    },
  },
};

const AFTER = {
  collections: {
    users: {
      _id: personId("user"),
      email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
      note: personal(v.string(), { role: "content" }),
    },
  },
};

const DRIFTED = {
  collections: {
    users: {
      _id: personId("user"),
      email: personal(v.pipe(v.string(), v.email()), { role: "quasi" }),
      note: personal(v.string(), { role: "content" }),
    },
  },
};

Deno.test("retrofit: a project that adds privacy declarations must freeze them in its last migration", () => {
  const baseline = migrationDefinition("001", "baseline", {
    parent: null,
    schemas: BEFORE,
    migrate: (b) => b.compile(),
  });
  const stale = validateLastMigrationMatchesProjectSchema(baseline, AFTER);
  assertEquals(stale.valid, false);

  const classified = migrationDefinition("002", "classify", {
    parent: baseline,
    schemas: AFTER,
    migrate: (b) => b.compile(),
  });
  assertEquals(
    validateLastMigrationMatchesProjectSchema(classified, AFTER).valid,
    true,
  );
});

Deno.test("retrofit: a role change alone is a snapshot difference", () => {
  const classified = migrationDefinition("002", "classify", {
    parent: null,
    schemas: AFTER,
    migrate: (b) => b.compile(),
  });
  const gate = validateLastMigrationMatchesProjectSchema(classified, DRIFTED);
  assertEquals(gate.valid, false);
  assert(gate.errors.some((e) => e.includes("users")));
});

Deno.test("retrofit: each migration carries its own plan, older steps simply know less", () => {
  const before = buildPrivacyPlan({ schemas: BEFORE });
  assertEquals(before.persons.size, 0);
  assertEquals(before.targets.get("collections/users/")!.owner.kind, "none");
  assertEquals(
    before.targets.get("collections/users/")!.paths.find((p) =>
      p.path === "email"
    )!.tier,
    "unknown",
  );

  const after = buildPrivacyPlan({ schemas: AFTER });
  assertEquals(after.persons.size, 1);
  assertEquals(after.targets.get("collections/users/")!.owner.kind, "self");
  assertEquals(
    after.targets.get("collections/users/")!.paths.find((p) =>
      p.path === "email"
    )!.tier,
    "declared",
  );
  assertEquals(after.summary.unknown, 0);
});
