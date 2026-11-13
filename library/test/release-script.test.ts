import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import {
  distTagOf,
  type Exec,
  JSR_ATTEMPTS,
  listingHas,
  type Manifests,
  manifestProblems,
  type Options,
  type Ran,
  type Registries,
  release,
  type StepReport,
  succeeded,
} from "../scripts/release.ts";

const VERSION = "1.2.3-beta.4";
const TAG = `v${VERSION}`;
const CORE = "@diister/mongodbee";
const STUDIO = "@diister/mongodbee-studio";

const manifests: Manifests = {
  library: { name: CORE, version: VERSION },
  jsr: { name: CORE, version: VERSION },
  studio: { name: STUDIO, version: VERSION, core: VERSION },
};

const options = (over: Partial<Options> = {}): Options => ({
  dryRun: false,
  registry: "both",
  github: true,
  refTag: undefined,
  ...over,
});

/** Registries where nothing of this release exists yet, unless told so. */
interface World {
  npmLive: Set<string>;
  npmStaged: Set<string>;
  jsr: Set<string>;
  tags: Set<string>;
  releases: Set<string>;
  stageList: "visible" | "unknown";
}

const emptyWorld = (): World => ({
  npmLive: new Set(),
  npmStaged: new Set(),
  jsr: new Set(),
  tags: new Set(),
  releases: new Set(),
  stageList: "visible",
});

const registriesOf = (world: World): Registries => ({
  npmPublished: async (name, version) =>
    world.npmLive.has(`${name}@${version}`),
  npmStaged: async (name, version) =>
    world.stageList === "unknown"
      ? "unknown"
      : world.npmStaged.has(`${name}@${version}`),
  jsrPublished: async (name, version) => world.jsr.has(`${name}@${version}`),
  tagExists: async (tag) => world.tags.has(tag),
  releaseExists: async (tag) => world.releases.has(tag),
});

type Answer = (command: readonly string[], cwd: string) => Ran | undefined;

/** Records every command, applies the successful ones to the world. */
function execOf(world: World, answer: Answer = () => undefined) {
  const calls: string[] = [];
  const exec: Exec = async (command, cwd) => {
    calls.push(`${cwd}: ${command.join(" ")}`);
    const forced = answer(command, cwd);
    if (forced) return forced;
    const joined = command.join(" ");
    if (joined.startsWith("npm stage publish")) {
      world.npmStaged.add(`${cwd === "/studio" ? STUDIO : CORE}@${VERSION}`);
    } else if (joined === "npx jsr publish")
      world.jsr.add(`${CORE}@${VERSION}`);
    else if (joined.startsWith("git push")) world.tags.add(TAG);
    else if (joined.startsWith("gh release create")) world.releases.add(TAG);
    return { code: 0, output: "" };
  };
  return { exec, calls };
}

async function run(world: World, opts: Options, answer?: Answer) {
  const { exec, calls } = execOf(world, answer);
  const lines: string[] = [];
  const reports = await release(manifests, opts, {
    registries: registriesOf(world),
    exec,
    log: (line) => lines.push(line),
    sleep: async () => {},
    dirs: { library: "/library", studio: "/studio", root: "/" },
  });
  return { reports, calls, lines };
}

const kinds = (reports: readonly StepReport[]) =>
  Object.fromEntries(reports.map((r) => [r.id, r.outcome.kind]));

test("release: a fresh release runs every step once, in order", async () => {
  const { reports, calls } = await run(emptyWorld(), options());
  assertEquals(kinds(reports), {
    npm: "done",
    jsr: "done",
    studio: "done",
    tag: "done",
    release: "done",
  });
  assertEquals(calls, [
    "/library: npm stage publish --access public --tag beta",
    "/library: npx jsr publish",
    "/studio: npm stage publish --access public --tag beta",
    `/: git tag --annotate ${TAG} --message ${TAG}`,
    `/: git push origin refs/tags/${TAG}`,
    `/: gh release create ${TAG} --verify-tag --generate-notes --title ${TAG} --prerelease`,
  ]);
  assert(succeeded(reports));
});

test("release: a re-run after a JSR failure only publishes JSR, then tags", async () => {
  const world = emptyWorld();
  const first = await run(world, options(), (command) =>
    command.join(" ") === "npx jsr publish"
      ? { code: 1, output: "500" }
      : undefined,
  );
  assertEquals(kinds(first.reports), {
    npm: "done",
    jsr: "failed",
    studio: "done",
    tag: "blocked",
    release: "blocked",
  });
  assertEquals(
    first.calls.filter((c) => c.includes("jsr publish")).length,
    JSR_ATTEMPTS,
  );
  assert(!succeeded(first.reports));

  const second = await run(world, options());
  assertEquals(kinds(second.reports), {
    npm: "skipped",
    jsr: "done",
    studio: "skipped",
    tag: "done",
    release: "done",
  });
  assert(!second.calls.some((c) => c.includes("npm stage publish")));
  assert(succeeded(second.reports));

  const third = await run(world, options());
  assertEquals(third.calls, []);
  assert(third.reports.every((r) => r.outcome.kind === "skipped"));
});

test("release: a version already live on npm is never staged again", async () => {
  const world = emptyWorld();
  world.npmLive.add(`${CORE}@${VERSION}`);
  const { reports, calls, lines } = await run(world, options());
  assertEquals(reports[0].outcome.kind, "skipped");
  assert(lines[0].includes("already published on npm"));
  assert(!calls.some((c) => c.startsWith("/library: npm stage")));
});

test("release: a staged version npm will not list is skipped on its 409", async () => {
  const world = emptyWorld();
  world.stageList = "unknown";
  const { reports } = await run(world, options(), (command, cwd) =>
    command[1] === "stage" && cwd === "/library"
      ? { code: 1, output: "npm error code E409\nnpm error 409 Conflict" }
      : undefined,
  );
  assertEquals(reports[0].outcome.kind, "skipped");
  assert(succeeded(reports));
});

test("release: a JSR publish that errors but landed counts as done", async () => {
  const world = emptyWorld();
  const { reports, calls } = await run(world, options(), (command) => {
    if (command.join(" ") !== "npx jsr publish") return undefined;
    world.jsr.add(`${CORE}@${VERSION}`);
    return { code: 1, output: "timeout" };
  });
  assertEquals(reports[1].outcome, {
    kind: "done",
    note: "reached JSR despite the error",
  });
  assertEquals(calls.filter((c) => c.includes("jsr publish")).length, 1);
});

test("release: a dry run asks the registries and runs nothing", async () => {
  const world = emptyWorld();
  world.jsr.add(`${CORE}@${VERSION}`);
  const { reports, calls, lines } = await run(world, options({ dryRun: true }));
  assertEquals(calls, []);
  assertEquals(kinds(reports), {
    npm: "would-run",
    jsr: "skipped",
    studio: "would-run",
    tag: "would-run",
    release: "would-run",
  });
  assert(lines[0].includes("would run `npm stage publish"));
  assertEquals(world.npmStaged.size + world.tags.size + world.releases.size, 0);
});

test("release: without --github, no tag and no release are touched", async () => {
  const { reports, calls } = await run(
    emptyWorld(),
    options({ github: false }),
  );
  assertEquals(reports[3].outcome.kind, "not-selected");
  assertEquals(reports[4].outcome.kind, "not-selected");
  assert(!calls.some((c) => c.includes("git") || c.includes("gh ")));
});

test("release: --registry jsr leaves npm alone", async () => {
  const { reports, calls } = await run(
    emptyWorld(),
    options({ registry: "jsr", github: false }),
  );
  assertEquals(kinds(reports).npm, "not-selected");
  assertEquals(kinds(reports).studio, "not-selected");
  assertEquals(calls, ["/library: npx jsr publish"]);
});

test("release: an existing tag and GitHub release are skipped", async () => {
  const world = emptyWorld();
  world.tags.add(TAG);
  world.releases.add(TAG);
  const { reports, calls } = await run(world, options({ refTag: TAG }));
  assertEquals(reports[3].outcome.kind, "skipped");
  assertEquals(reports[4].outcome.kind, "skipped");
  assert(!calls.some((c) => c.includes("git ") || c.includes("gh ")));
});

test("release: a probe that throws fails its step, not the run", async () => {
  const world = emptyWorld();
  const { exec } = execOf(world);
  const reports = await release(manifests, options({ github: false }), {
    registries: {
      ...registriesOf(world),
      jsrPublished: async () => {
        throw new Error("jsr.io answered 503");
      },
    },
    exec,
    log: () => {},
    sleep: async () => {},
    dirs: { library: "/library", studio: "/studio", root: "/" },
  });
  assertEquals(kinds(reports), {
    npm: "done",
    jsr: "failed",
    studio: "done",
    tag: "not-selected",
    release: "not-selected",
  });
});

test("release: mismatched manifests or tag publish nothing", async () => {
  const { reports, calls } = await run(
    emptyWorld(),
    options({ refTag: "v9.9.9" }),
  );
  assertEquals(calls, []);
  assert(reports.every((r) => r.outcome.kind === "blocked"));
  assertEquals(
    manifestProblems(
      { ...manifests, studio: { ...manifests.studio, core: "1.0.0" } },
      undefined,
    ).length,
    1,
  );
});

test("release: the dist-tag comes from the prerelease identifier", () => {
  assertEquals(distTagOf("1.0.0"), "latest");
  assertEquals(distTagOf("0.23.0-beta.49"), "beta");
  assertEquals(distTagOf("2.0.0-rc.1"), "rc");
});

test("release: a stage listing is searched for the exact version", () => {
  const listing = [
    { id: "a", package: CORE, version: "1.2.3-beta.3" },
    { id: "b", package: { name: CORE, version: VERSION } },
  ];
  assert(listingHas(listing, CORE, VERSION));
  assert(!listingHas(listing, CORE, "1.2.3-beta.5"));
  assert(!listingHas(listing, STUDIO, VERSION));
  assert(
    listingHas({ staged: [{ spec: `${CORE}@${VERSION}` }] }, CORE, VERSION),
  );
});
