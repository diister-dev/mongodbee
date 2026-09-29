import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

export interface UiAsset {
  body: Blob;
  type: string;
}

export type UiBundle = Map<string, UiAsset>;

export interface UiManifestEntry {
  path: string;
  type: string;
}

export interface UiManifest {
  files: UiManifestEntry[];
}

export const PREBUILT_UI_DIR = "studio-ui";
export const UI_MANIFEST = "manifest.json";

interface BuildArtifact extends Blob {
  path: string;
  kind: string;
}

interface BuildOutput {
  success: boolean;
  outputs: BuildArtifact[];
  logs: unknown[];
}

interface BunBundler {
  build(options: Record<string, unknown>): Promise<BuildOutput>;
}

const SOURCE_ENTRY = "./ui/index.html";

export function resolveUiEntry(): string {
  const file = fileURLToPath(new URL(SOURCE_ENTRY, import.meta.url));
  if (existsSync(file)) return file;
  throw new Error("mongodbee studio: the UI sources were not found");
}

function existingUiDir(relative: string): string | undefined {
  const dir = fileURLToPath(new URL(relative, import.meta.url));
  return existsSync(path.join(dir, UI_MANIFEST)) ? dir : undefined;
}

export function prebuiltUiDir(): string | undefined {
  if (import.meta.url.endsWith(".js")) {
    return existingUiDir(`../${PREBUILT_UI_DIR}/`);
  }
  return undefined;
}

export function sourceBuildUiDir(): string | undefined {
  return existingUiDir(`../dist/${PREBUILT_UI_DIR}/`);
}

function hasBun(): boolean {
  return Reflect.get(globalThis, "Bun") !== undefined;
}

function isBunBundler(value: unknown): value is BunBundler {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof Reflect.get(value, "build") === "function"
  );
}

function bunRuntime(): BunBundler {
  const bun: unknown = Reflect.get(globalThis, "Bun");
  if (!isBunBundler(bun)) {
    throw new Error(
      "mongodbee studio: building the UI from source needs Bun. Run `bun run build:ui` in the studio package first",
    );
  }
  return bun;
}

async function loadSveltePlugin(): Promise<unknown> {
  const specifier = "bun-plugin-svelte";
  try {
    const plugin = await import(specifier);
    return plugin.SveltePlugin({ development: false });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `mongodbee studio needs "svelte" and "bun-plugin-svelte" to build the UI from source: ${message}`,
    );
  }
}

export async function buildUi(outdir?: string): Promise<BuildArtifact[]> {
  const result = await bunRuntime().build({
    entrypoints: [resolveUiEntry()],
    target: "browser",
    minify: true,
    publicPath: "/",
    naming: {
      entry: "[dir]/[name].[ext]",
      chunk: "[name]-[hash].[ext]",
      asset: "[name]-[hash].[ext]",
    },
    plugins: [await loadSveltePlugin()],
    ...(outdir ? { outdir } : {}),
  });
  if (!result.success) {
    throw new Error(
      `mongodbee studio: the UI failed to build\n${result.logs
        .map(String)
        .join("\n")}`,
    );
  }
  return result.outputs;
}

export function routeFor(file: string): string {
  return `/${file.replace(/\\/g, "/").replace(/^\.?\//, "")}`;
}

function addAsset(bundle: UiBundle, route: string, asset: UiAsset): void {
  bundle.set(route, asset);
  if (route.endsWith(".html")) bundle.set("/", asset);
}

export async function buildUiBundle(): Promise<UiBundle> {
  const bundle: UiBundle = new Map();
  for (const output of await buildUi()) {
    addAsset(bundle, routeFor(output.path), {
      body: output,
      type: output.type,
    });
  }
  return bundle;
}

export async function loadPrebuiltUi(dir: string): Promise<UiBundle> {
  const manifest = JSON.parse(
    await readFile(path.join(dir, UI_MANIFEST), "utf8"),
  ) as UiManifest;
  const bundle: UiBundle = new Map();
  for (const entry of manifest.files) {
    const bytes = await readFile(path.join(dir, entry.path));
    addAsset(bundle, routeFor(entry.path), {
      body: new Blob([bytes], { type: entry.type }),
      type: entry.type,
    });
  }
  return bundle;
}

export async function loadUiBundle(): Promise<UiBundle> {
  const prebuilt = prebuiltUiDir();
  if (prebuilt) return loadPrebuiltUi(prebuilt);
  if (hasBun()) return buildUiBundle();
  const built = sourceBuildUiDir();
  if (built) return loadPrebuiltUi(built);
  return buildUiBundle();
}
