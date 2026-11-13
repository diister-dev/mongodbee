/**
 * Publishes a release, one step at a time, so that a release that broke
 * halfway finishes by running this again.
 *
 * Every step first asks whether its exact version is already out — on npm
 * (live or staged), on JSR, as a git tag, as a GitHub release — and skips
 * itself if so. A re-run after a failure therefore only does what is left.
 *
 * Steps, in order:
 *
 * 1. `npm`      — stage `@diister/mongodbee` on npm (`npm stage publish`).
 * 2. `jsr`      — publish `@diister/mongodbee` on JSR.
 * 3. `studio`   — stage `@diister/mongodbee-studio` on npm.
 * 4. `tag`      — push the `v<version>` tag (only with `--github`).
 * 5. `release`  — create the GitHub release (only with `--github`).
 *
 * The publishing steps are independent: one failing does not stop the next,
 * so a JSR outage still lets npm through. The tag and the GitHub release only
 * happen once every publishing step is done, so they never announce a
 * release that is not fully out.
 *
 * Usage, from `library/`, after `bun run build` here and in `studio/`:
 *
 *     bun run scripts/release.ts [--dry-run] [--registry both|npm|jsr] [--github]
 *
 * `--dry-run` asks the registries (read-only) and prints what it would do,
 * without publishing, tagging or releasing anything.
 *
 * @module
 */

import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

/** The versions and names a release is cut from, read from the manifests. */
export interface Manifests {
  readonly library: { readonly name: string; readonly version: string };
  readonly jsr: { readonly name: string; readonly version: string };
  readonly studio: {
    readonly name: string;
    readonly version: string;
    /** The version of `@diister/mongodbee` the studio pins. */
    readonly core: string | undefined;
  };
}

/** Whether a staged version could be seen: `npm stage list` needs auth. */
export type Staged = boolean | "unknown";

/** The read-only questions a step asks before acting. Mocked in tests. */
export interface Registries {
  npmPublished(name: string, version: string): Promise<boolean>;
  npmStaged(name: string, version: string): Promise<Staged>;
  jsrPublished(name: string, version: string): Promise<boolean>;
  tagExists(tag: string): Promise<boolean>;
  releaseExists(tag: string): Promise<boolean>;
}

/** What a command printed and how it ended. */
export interface Ran {
  readonly code: number;
  readonly output: string;
}

/** Runs a command. Mocked in tests. */
export type Exec = (command: readonly string[], cwd: string) => Promise<Ran>;

export type StepId = "npm" | "jsr" | "studio" | "tag" | "release";
export type Registry = "both" | "npm" | "jsr";

export interface Options {
  readonly dryRun: boolean;
  readonly registry: Registry;
  readonly github: boolean;
  /** `GITHUB_REF_NAME` on a tag push: the tag must name the manifests' version. */
  readonly refTag: string | undefined;
}

/** Where each step ended up. */
export type Outcome =
  | { readonly kind: "skipped"; readonly reason: string }
  | { readonly kind: "done"; readonly note?: string }
  | { readonly kind: "would-run"; readonly command: readonly string[] }
  | { readonly kind: "not-selected" }
  | { readonly kind: "blocked"; readonly reason: string }
  | { readonly kind: "failed"; readonly reason: string };

export interface StepReport {
  readonly id: StepId;
  readonly outcome: Outcome;
}

export interface Dirs {
  readonly library: string;
  readonly studio: string;
  readonly root: string;
}

export interface Deps {
  readonly registries: Registries;
  readonly exec: Exec;
  readonly log: (line: string) => void;
  readonly sleep: (ms: number) => Promise<void>;
  readonly dirs: Dirs;
}

/** How many times a JSR publish is tried before giving up. */
export const JSR_ATTEMPTS = 3;
/** The pause before the n-th retry of a JSR publish is n times this. */
export const JSR_RETRY_STEP_MS = 20_000;

/**
 * npm tags "latest" whatever the version says, so the dist-tag comes from the
 * version's own prerelease identifier — a beta must never become what
 * `npm install mongodbee` hands a new user.
 */
export function distTagOf(version: string): string {
  const dash = version.indexOf("-");
  if (dash === -1) return "latest";
  return version.slice(dash + 1).split(".")[0] || "latest";
}

export function isPrerelease(version: string): boolean {
  return version.includes("-");
}

/** The reasons the manifests cannot be released as they stand, if any. */
export function manifestProblems(
  manifests: Manifests,
  refTag: string | undefined,
): string[] {
  const version = manifests.library.version;
  const problems: string[] = [];
  if (manifests.jsr.version !== version) {
    problems.push(
      `jsr.json is ${manifests.jsr.version}, package.json is ${version}`,
    );
  }
  if (manifests.studio.version !== version) {
    problems.push(
      `the studio is ${manifests.studio.version}, the library is ${version}`,
    );
  }
  if (manifests.studio.core !== version) {
    problems.push(
      `the studio depends on ${manifests.library.name}@${manifests.studio.core}, not ${version}`,
    );
  }
  if (refTag !== undefined && refTag !== `v${version}`) {
    problems.push(
      `tag ${refTag} does not match package.json version ${version}`,
    );
  }
  return problems;
}

/** npm's answer when the exact version is already waiting in the stage. */
export function saysAlreadyStaged(output: string): boolean {
  return /\bE409\b/.test(output) || /already staged/i.test(output);
}

/** npm's answer when the exact version is already live. */
export function saysAlreadyPublished(output: string): boolean {
  return /cannot publish over (the )?previously published version/i.test(
    output,
  );
}

interface Step {
  readonly id: StepId;
  readonly label: string;
  readonly selected: boolean;
  readonly gated: boolean;
  /** Why the step is already done, or null when it still has to run. */
  readonly alreadyDone: () => Promise<string | null>;
  readonly command: readonly string[];
  readonly cwd: string;
  readonly act: () => Promise<Outcome>;
}

function npmStageStep(
  id: StepId,
  label: string,
  pkg: { readonly name: string; readonly version: string },
  cwd: string,
  selected: boolean,
  deps: Deps,
): Step {
  const spec = `${pkg.name}@${pkg.version}`;
  const command = [
    "npm",
    "stage",
    "publish",
    "--access",
    "public",
    "--tag",
    distTagOf(pkg.version),
  ];
  return {
    id,
    label,
    selected,
    gated: false,
    command,
    cwd,
    alreadyDone: async () => {
      if (await deps.registries.npmPublished(pkg.name, pkg.version)) {
        return `${spec} is already published on npm`;
      }
      const staged = await deps.registries.npmStaged(pkg.name, pkg.version);
      if (staged === true) {
        return `${spec} is already staged on npm: approve it with npm stage list && npm stage approve <id>`;
      }
      return null;
    },
    act: async () => {
      const ran = await deps.exec(command, cwd);
      if (ran.code === 0) {
        return {
          kind: "done",
          note: "staged: release it with npm stage list && npm stage approve <id>",
        };
      }
      if (saysAlreadyStaged(ran.output)) {
        return {
          kind: "skipped",
          reason: `${spec} was already staged (npm answered 409)`,
        };
      }
      if (saysAlreadyPublished(ran.output)) {
        return { kind: "skipped", reason: `${spec} was already published` };
      }
      return { kind: "failed", reason: `npm stage publish exited ${ran.code}` };
    },
  };
}

function jsrStep(manifests: Manifests, selected: boolean, deps: Deps): Step {
  const { name, version } = manifests.jsr;
  const command = ["npx", "jsr", "publish"];
  const cwd = deps.dirs.library;
  const published = () => deps.registries.jsrPublished(name, version);
  return {
    id: "jsr",
    label: `publish ${name}@${version} on JSR`,
    selected,
    gated: false,
    command,
    cwd,
    alreadyDone: async () =>
      (await published()) ? `${name}@${version} is already on JSR` : null,
    act: async () => {
      for (let attempt = 1; attempt <= JSR_ATTEMPTS; attempt++) {
        const ran = await deps.exec(command, cwd);
        if (ran.code === 0) return { kind: "done" };
        if (await published()) {
          return { kind: "done", note: "reached JSR despite the error" };
        }
        if (attempt < JSR_ATTEMPTS) {
          const wait = attempt * JSR_RETRY_STEP_MS;
          deps.log(
            `  JSR publish failed (attempt ${attempt}), retrying in ${wait / 1000} s`,
          );
          await deps.sleep(wait);
        }
      }
      return {
        kind: "failed",
        reason: `jsr publish failed ${JSR_ATTEMPTS} times`,
      };
    },
  };
}

function tagStep(tag: string, selected: boolean, deps: Deps): Step {
  const cwd = deps.dirs.root;
  const command = ["git", "push", "origin", `refs/tags/${tag}`];
  return {
    id: "tag",
    label: `push the tag ${tag}`,
    selected,
    gated: true,
    command,
    cwd,
    alreadyDone: async () =>
      (await deps.registries.tagExists(tag))
        ? `${tag} is already on origin`
        : null,
    act: async () => {
      const created = await deps.exec(
        ["git", "tag", "--annotate", tag, "--message", tag],
        cwd,
      );
      if (created.code !== 0 && !/already exists/.test(created.output)) {
        return { kind: "failed", reason: `git tag exited ${created.code}` };
      }
      const pushed = await deps.exec(command, cwd);
      return pushed.code === 0
        ? { kind: "done" }
        : { kind: "failed", reason: `git push exited ${pushed.code}` };
    },
  };
}

function releaseStep(
  tag: string,
  version: string,
  selected: boolean,
  deps: Deps,
): Step {
  const cwd = deps.dirs.root;
  const command = [
    "gh",
    "release",
    "create",
    tag,
    "--verify-tag",
    "--generate-notes",
    "--title",
    tag,
    ...(isPrerelease(version) ? ["--prerelease"] : []),
  ];
  return {
    id: "release",
    label: `create the GitHub release ${tag}`,
    selected,
    gated: true,
    command,
    cwd,
    alreadyDone: async () =>
      (await deps.registries.releaseExists(tag))
        ? `the GitHub release ${tag} already exists`
        : null,
    act: async () => {
      const ran = await deps.exec(command, cwd);
      return ran.code === 0
        ? { kind: "done" }
        : { kind: "failed", reason: `gh release create exited ${ran.code}` };
    },
  };
}

/** The steps of a release, in the order they run. */
function stepsOf(manifests: Manifests, options: Options, deps: Deps): Step[] {
  const version = manifests.library.version;
  const tag = `v${version}`;
  const npm = options.registry !== "jsr";
  const jsr = options.registry !== "npm";
  return [
    npmStageStep(
      "npm",
      `stage ${manifests.library.name}@${version} on npm under '${distTagOf(version)}'`,
      manifests.library,
      deps.dirs.library,
      npm,
      deps,
    ),
    jsrStep(manifests, jsr, deps),
    npmStageStep(
      "studio",
      `stage ${manifests.studio.name}@${manifests.studio.version} on npm under '${distTagOf(manifests.studio.version)}'`,
      manifests.studio,
      deps.dirs.studio,
      npm,
      deps,
    ),
    tagStep(tag, options.github, deps),
    releaseStep(tag, version, options.github, deps),
  ];
}

const failedOrBlocked = (report: StepReport) =>
  report.outcome.kind === "failed" || report.outcome.kind === "blocked";

/**
 * Runs (or, with `dryRun`, describes) every step that is not already done.
 * Never throws for a step: a probe or command that fails is that step's
 * `failed` outcome, and the run goes on to the next independent step.
 */
export async function release(
  manifests: Manifests,
  options: Options,
  deps: Deps,
): Promise<StepReport[]> {
  const problems = manifestProblems(manifests, options.refTag);
  if (problems.length > 0) {
    for (const problem of problems) deps.log(`refused: ${problem}`);
    return stepsOf(manifests, options, deps).map((step) => ({
      id: step.id,
      outcome: { kind: "blocked", reason: problems.join("; ") },
    }));
  }

  const reports: StepReport[] = [];
  for (const step of stepsOf(manifests, options, deps)) {
    const outcome = await settle(step, options, reports);
    deps.log(line(step, outcome));
    reports.push({ id: step.id, outcome });
  }
  return reports;
}

async function settle(
  step: Step,
  options: Options,
  before: readonly StepReport[],
): Promise<Outcome> {
  if (!step.selected) return { kind: "not-selected" };
  if (step.gated) {
    const unfinished = before.filter(failedOrBlocked).map((r) => r.id);
    if (unfinished.length > 0) {
      return {
        kind: "blocked",
        reason: `waits for ${unfinished.join(", ")} to be out`,
      };
    }
  }
  try {
    const done = await step.alreadyDone();
    if (done !== null) return { kind: "skipped", reason: done };
    if (options.dryRun) return { kind: "would-run", command: step.command };
    return await step.act();
  } catch (error) {
    return {
      kind: "failed",
      reason: error instanceof Error ? error.message : String(error),
    };
  }
}

function line(step: Step, outcome: Outcome): string {
  const head = `[${step.id}] ${step.label}`;
  switch (outcome.kind) {
    case "skipped":
      return `${head}: skip, ${outcome.reason}`;
    case "done":
      return `${head}: done${outcome.note ? `, ${outcome.note}` : ""}`;
    case "would-run":
      return `${head}: would run \`${outcome.command.join(" ")}\` in ${path.relative(process.cwd(), step.cwd) || "."}`;
    case "not-selected":
      return `${head}: not selected`;
    case "blocked":
      return `${head}: blocked, ${outcome.reason}`;
    case "failed":
      return `${head}: FAILED, ${outcome.reason}`;
  }
}

/** True when nothing is left to do: the run may exit 0. */
export function succeeded(reports: readonly StepReport[]): boolean {
  return !reports.some(failedOrBlocked);
}

export function parseArgs(argv: readonly string[]): Options {
  let dryRun = false;
  let github = false;
  let registry: Registry = "both";
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--dry-run") dryRun = true;
    else if (arg === "--github") github = true;
    else if (arg === "--registry") {
      const value = argv[++i] || "both";
      if (value !== "both" && value !== "npm" && value !== "jsr") {
        throw new Error(`--registry takes both, npm or jsr, not ${value}`);
      }
      registry = value;
    } else throw new Error(`unknown argument ${arg}`);
  }
  const ref = process.env.GITHUB_REF ?? "";
  const refTag = ref.startsWith("refs/tags/")
    ? ref.slice("refs/tags/".length)
    : undefined;
  return { dryRun, registry, github, refTag };
}

/**
 * Finds `version` in what `npm stage list --json` printed. The listing's shape
 * is not documented, so any object carrying both the package name and the
 * version, at any depth, counts.
 */
export function listingHas(
  listing: unknown,
  name: string,
  version: string,
): boolean {
  if (Array.isArray(listing)) {
    return listing.some((item) => listingHas(item, name, version));
  }
  if (listing === null || typeof listing !== "object") return false;
  const values = Object.values(listing);
  const strings = values.filter((v): v is string => typeof v === "string");
  const named = strings.some((s) => s === name || s === `${name}@${version}`);
  if (
    named &&
    strings.some((s) => s === version || s.endsWith(`@${version}`))
  ) {
    return true;
  }
  return values.some((value) => listingHas(value, name, version));
}

function liveRegistries(exec: Exec, root: string): Registries {
  const fetchStatus = async (url: string): Promise<Response> => {
    const response = await fetch(url);
    if (response.status !== 200 && response.status !== 404) {
      throw new Error(`${url} answered ${response.status}`);
    }
    return response;
  };
  return {
    npmPublished: async (name, version) => {
      const url = `https://registry.npmjs.org/${name.replace("/", "%2f")}/${version}`;
      return (await fetchStatus(url)).status === 200;
    },
    npmStaged: async (name, version) => {
      const ran = await exec(["npm", "stage", "list", name, "--json"], root);
      if (ran.code !== 0) return "unknown";
      try {
        return listingHas(JSON.parse(ran.output), name, version);
      } catch {
        return "unknown";
      }
    },
    jsrPublished: async (name, version) => {
      const response = await fetchStatus(`https://jsr.io/${name}/meta.json`);
      if (response.status === 404) return false;
      const meta = (await response.json()) as {
        versions?: Record<string, unknown>;
      };
      return meta.versions?.[version] !== undefined;
    },
    tagExists: async (tag) => {
      const ran = await exec(
        ["git", "ls-remote", "--tags", "origin", `refs/tags/${tag}`],
        root,
      );
      if (ran.code !== 0) throw new Error(`git ls-remote exited ${ran.code}`);
      return ran.output.trim() !== "";
    },
    releaseExists: async (tag) => {
      const ran = await exec(["gh", "release", "view", tag], root);
      if (ran.code === 0) return true;
      if (/release not found/i.test(ran.output)) return false;
      throw new Error(`gh release view exited ${ran.code}`);
    },
  };
}

const liveExec: Exec = (command, cwd) =>
  new Promise((resolve) => {
    const [program, ...args] = command;
    const child = spawn(program, args, {
      cwd,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    const keep = (chunk: Buffer) => {
      output += chunk.toString();
    };
    child.stdout.on("data", keep);
    child.stderr.on("data", keep);
    child.on("error", (error) => resolve({ code: 127, output: String(error) }));
    child.on("close", (code) => resolve({ code: code ?? 1, output }));
  });

/** Echoes an acting command's output, so the workflow log keeps it. */
const echoingExec: Exec = async (command, cwd) => {
  const ran = await liveExec(command, cwd);
  if (ran.output.trim()) {
    process.stdout.write(
      ran.output.endsWith("\n") ? ran.output : `${ran.output}\n`,
    );
  }
  return ran;
};

async function readManifests(dirs: Dirs): Promise<Manifests> {
  const json = async (file: string) =>
    JSON.parse(await readFile(file, "utf8")) as {
      name: string;
      version: string;
      dependencies?: Record<string, string>;
    };
  const library = await json(path.join(dirs.library, "package.json"));
  const jsr = await json(path.join(dirs.library, "jsr.json"));
  const studio = await json(path.join(dirs.studio, "package.json"));
  return {
    library: { name: library.name, version: library.version },
    jsr: { name: jsr.name, version: jsr.version },
    studio: {
      name: studio.name,
      version: studio.version,
      core: studio.dependencies?.[library.name],
    },
  };
}

async function main(): Promise<void> {
  const library = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "..",
  );
  const dirs: Dirs = {
    library,
    studio: path.resolve(library, "..", "studio"),
    root: path.resolve(library, ".."),
  };
  const options = parseArgs(process.argv.slice(2));
  const manifests = await readManifests(dirs);
  if (options.dryRun)
    console.log("dry run: nothing is published, tagged or released");
  const reports = await release(manifests, options, {
    registries: liveRegistries(liveExec, dirs.root),
    exec: echoingExec,
    log: (text) => console.log(text),
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    dirs,
  });
  if (!succeeded(reports)) {
    console.error(
      "release incomplete: fix the cause and run this again, done steps are skipped",
    );
    process.exitCode = 1;
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) await main();
