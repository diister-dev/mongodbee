import process from "node:process";
import { yellow } from "../../../utils/colors.ts";
import { isRecord } from "../../../utils/guards.ts";

export const STUDIO_PACKAGE = "@diister/mongodbee-studio";

export const STUDIO_UNAVAILABLE_MESSAGE =
  `mongodbee studio lives in its own package, ${STUDIO_PACKAGE}.\n` +
  "Install it next to @diister/mongodbee, with the same version, then run the command again:\n" +
  `  npm install --save-dev ${STUDIO_PACKAGE}\n` +
  `  bun add --dev ${STUDIO_PACKAGE}\n` +
  `  deno add --dev npm:${STUDIO_PACKAGE}`;

type StudioHandler = (options: Record<string, unknown>) => Promise<void>;

function isMissingStudio(error: unknown): boolean {
  const code = isRecord(error) ? error.code : undefined;
  const message = error instanceof Error ? error.message : String(error);
  const missing =
    code === "ERR_MODULE_NOT_FOUND" ||
    /not found|cannot find (module|package)|could not resolve/i.test(message);
  return missing && message.includes(STUDIO_PACKAGE);
}

function handlerOf(module: unknown): StudioHandler | undefined {
  if (!isRecord(module)) return undefined;
  const handler = module.studioCommand;
  if (typeof handler !== "function") return undefined;
  return (options) => Promise.resolve(handler(options));
}

export async function loadStudioCommand(
  load: (specifier: string) => Promise<unknown> = (specifier) =>
    import(specifier),
): Promise<StudioHandler | undefined> {
  try {
    return handlerOf(await load(STUDIO_PACKAGE));
  } catch (error) {
    if (isMissingStudio(error)) return undefined;
    throw error;
  }
}

export async function studioEntry(
  options: Record<string, unknown> = {},
): Promise<void> {
  const handler = await loadStudioCommand();
  if (!handler) {
    console.log(yellow(STUDIO_UNAVAILABLE_MESSAGE));
    process.exitCode = 1;
    return;
  }
  await handler(options);
}
