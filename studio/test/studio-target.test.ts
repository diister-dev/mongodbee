import { test } from "../../library/test/+harness.ts";
import { assert, assertEquals } from "../../library/test/+assert.ts";
import { mkdtemp, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { installLibrary } from "../../library/test/migration/cli/shared.ts";
import { hasTargetOverrides, loadStudioProject } from "../src/context.ts";
import { writeCheckFixture } from "../../library/test/migration/cli/check-fixture.ts";

async function withProject(work: (dir: string) => Promise<void>) {
  const dir = await mkdtemp(path.join(tmpdir(), "mongodbee_target_"));
  try {
    await installLibrary(dir);
    await writeCheckFixture(dir, "valid");
    await work(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("hasTargetOverrides only counts explicit target options", () => {
  assertEquals(hasTargetOverrides({}), false);
  assertEquals(hasTargetOverrides({ cwd: "/x", configPath: "c.ts" }), false);
  assertEquals(hasTargetOverrides({ dbName: "other" }), true);
  assertEquals(hasTargetOverrides({ migrationsDir: "./m" }), true);
});

test("studio target: --project opens another project's configuration", async () => {
  await withProject(async (dir) => {
    const project = await loadStudioProject({ cwd: dir });
    assertEquals(project.root.cwd, dir);
    assertEquals(project.dbName, "studio_check_fixture");
    assertEquals(project.migrations.length, 2);
    assertEquals(project.schemasSource, "project");
    assertEquals(project.paths.migrations, path.join(dir, "migrations"));
  });
});

test("studio target: explicit options override the configuration", async () => {
  await withProject(async (dir) => {
    const project = await loadStudioProject({
      cwd: dir,
      dbName: "another_db",
      uri: "mongodb://example.invalid:27017",
    });
    assertEquals(project.dbName, "another_db");
    assertEquals(project.connectionUri, "mongodb://example.invalid:27017");
    assertEquals(project.migrations.length, 2);
  });
});

test("studio target: works without any configuration file", async () => {
  await withProject(async (dir) => {
    const elsewhere = await mkdtemp(path.join(tmpdir(), "mongodbee_nocfg_"));
    try {
      await rename(
        path.join(dir, "mongodbee.config.json"),
        path.join(dir, "moved.json"),
      );
      const project = await loadStudioProject({
        cwd: elsewhere,
        dbName: "studio_check_fixture",
        migrationsDir: path.join(dir, "migrations"),
        schemaPath: path.join(dir, "schemas.ts"),
      });
      assertEquals(project.dbName, "studio_check_fixture");
      assertEquals(project.migrations.length, 2);
      assertEquals(project.schemasSource, "project");
      assert(
        project.warnings.some((w) => w.startsWith("No configuration used")),
        "explains that no configuration was used",
      );
    } finally {
      await rm(elsewhere, { recursive: true, force: true });
    }
  });
});
