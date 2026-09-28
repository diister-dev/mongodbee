import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import {
  buildUi,
  PREBUILT_UI_DIR,
  UI_MANIFEST,
  type UiManifest,
} from "../src/ui-bundle.ts";

const packageDir = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const outdir = path.join(packageDir, "dist", PREBUILT_UI_DIR);

await rm(outdir, { recursive: true, force: true });
await mkdir(outdir, { recursive: true });

const outputs = await buildUi(outdir);
const manifest: UiManifest = {
  files: outputs.map((output) => ({
    path: path.relative(outdir, output.path).split(path.sep).join("/"),
    type: output.type,
  })),
};
await writeFile(
  path.join(outdir, UI_MANIFEST),
  `${JSON.stringify(manifest, null, 2)}\n`,
);

const size = outputs.reduce((sum, output) => sum + output.size, 0);
process.stdout.write(
  `built the studio UI: ${manifest.files.length} files, ${(size / 1024).toFixed(1)} KiB in dist/${PREBUILT_UI_DIR}\n`,
);
