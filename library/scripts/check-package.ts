/**
 * Guards the two published manifests against drift, and stages the files they
 * ship.
 *
 * Modelled on zod's `scripts/check-versions.ts`: it CHECKS that `package.json`
 * and `jsr.json` agree rather than rewriting one from the other. Syncing was
 * the first design here and it was worse — a build that mutates a tracked file
 * fights the formatter (`JSON.stringify` expands short arrays, biome collapses
 * them), so `bun run build && bun run fmt:check` failed on a clean checkout.
 * A check cannot do that, and a mismatch is a real mistake worth failing on.
 *
 * `README.md` and `LICENSE` are a different matter: they live at the repository
 * root, npm and JSR only ship what sits inside the package directory, and both
 * copies are gitignored. Staging them mutates nothing that is tracked.
 *
 * Run `bun run version:set <version>` to bump both manifests at once; with no
 * arguments this only checks and stages.
 *
 * @module
 */

import { copyFile, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const packageDir = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const repoRoot = path.resolve(packageDir, "..");

const packagePath = path.join(packageDir, "package.json");
const jsrPath = path.join(packageDir, "jsr.json");

/** Replaces the `version` field in place, leaving the rest byte-for-byte. */
function withVersion(source: string, version: string): string {
  const patched = source.replace(
    /("version"\s*:\s*)"[^"]*"/,
    `$1${JSON.stringify(version)}`,
  );
  if (patched === source) {
    throw new Error("no `version` field found to update");
  }
  return patched;
}

const writeFlag = process.argv.indexOf("--write");
const requested = writeFlag === -1 ? undefined : process.argv[writeFlag + 1];

if (writeFlag !== -1 && !requested) {
  throw new Error("--write needs a version, e.g. --write 0.1.9");
}

let packageSource = await readFile(packagePath, "utf8");
let jsrSource = await readFile(jsrPath, "utf8");

if (requested) {
  packageSource = withVersion(packageSource, requested);
  jsrSource = withVersion(jsrSource, requested);
  await writeFile(packagePath, packageSource);
  await writeFile(jsrPath, jsrSource);
}

const manifest = JSON.parse(packageSource);
const jsr = JSON.parse(jsrSource);

// The two registries carry different names on purpose — `mongodbee` on npm,
// `@diister/mongodbee` on JSR — so only the version is compared.

if (jsr.version !== manifest.version) {
  throw new Error(
    `version mismatch — package.json is ${String(
      manifest.version,
    )}, jsr.json is ${String(
      jsr.version,
    )}. Run \`bun run version:set <version>\`.`,
  );
}

// `src/version.ts` is what the CLI prints and the telemetry tracer reports. It
// is a tracked module rather than a JSON import because the manifest sits at a
// different relative depth in `dist/` than in the source tree — so it is
// checked here too, and rewritten only under `--write`.
const versionPath = path.join(packageDir, "src", "version.ts");
const versionSource = await readFile(versionPath, "utf8");
if (requested) {
  await writeFile(
    versionPath,
    versionSource.replace(
      /(export const VERSION = )"[^"]*"/,
      `$1${JSON.stringify(requested)}`,
    ),
  );
} else {
  const declared = /export const VERSION = "([^"]*)"/.exec(versionSource)?.[1];
  if (declared !== manifest.version) {
    throw new Error(
      `version mismatch — package.json is ${String(
        manifest.version,
      )}, src/version.ts is ${String(declared)}. Run \`bun run version:set <version>\`.`,
    );
  }
}

for (const file of ["README.md", "LICENSE"]) {
  await copyFile(path.join(repoRoot, file), path.join(packageDir, file));
}

process.stdout.write(`checked ${manifest.name}@${manifest.version}\n`);
