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

// npm refused the unscoped `mongodbee` as too close to `mongodb`, so both
// registries carry the scoped name and a mismatch is a mistake.
if (jsr.name !== manifest.name) {
  throw new Error(
    `name mismatch — package.json says ${String(manifest.name)}, jsr.json says ${String(
      jsr.name,
    )}`,
  );
}

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

const studioDir = path.join(repoRoot, "studio");
const studioPath = path.join(studioDir, "package.json");
const STUDIO_NAME = "@diister/mongodbee-studio";

function withDependency(source: string, name: string, version: string): string {
  const pattern = new RegExp(`("${name}"\\s*:\\s*)"[^"]*"`, "g");
  if (!pattern.test(source)) {
    throw new Error(`no \`${name}\` entry found to update`);
  }
  return source.replace(pattern, `$1${JSON.stringify(version)}`);
}

let studioSource = await readFile(studioPath, "utf8");
if (requested) {
  studioSource = withDependency(
    withVersion(studioSource, requested),
    manifest.name,
    requested,
  );
  await writeFile(studioPath, studioSource);
  packageSource = withDependency(packageSource, STUDIO_NAME, requested);
  await writeFile(packagePath, packageSource);
}
const studio = JSON.parse(studioSource);
const core = JSON.parse(packageSource);
const studioChecks: Array<[string, unknown, unknown]> = [
  ["studio version", studio.version, core.version],
  [
    `studio dependency on ${core.name}`,
    studio.dependencies?.[core.name],
    core.version,
  ],
  [
    `core optional peer ${STUDIO_NAME}`,
    core.peerDependencies?.[STUDIO_NAME],
    core.version,
  ],
  [
    "studio mongodb driver",
    studio.dependencies?.mongodb,
    core.dependencies?.mongodb,
  ],
];
for (const [label, actual, expected] of studioChecks) {
  if (actual !== expected) {
    throw new Error(
      `${label} is ${String(actual)}, expected ${String(expected)}. Run \`bun run version:set <version>\`.`,
    );
  }
}

for (const file of ["README.md", "LICENSE"]) {
  await copyFile(path.join(repoRoot, file), path.join(packageDir, file));
}
await copyFile(path.join(repoRoot, "LICENSE"), path.join(studioDir, "LICENSE"));

process.stdout.write(
  `checked ${manifest.name}@${manifest.version} and ${STUDIO_NAME}@${studio.version}\n`,
);
