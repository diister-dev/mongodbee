import { yellow } from "../../../utils/colors.ts";

export const STUDIO_UNAVAILABLE_MESSAGE =
  "mongodbee studio ships with the npm package.\n" +
  "Install @diister/mongodbee from npm in your project, then run it with the project's runtime:\n" +
  "  npx mongodbee studio\n" +
  "  bunx mongodbee studio\n" +
  "  deno run -A npm:@diister/mongodbee/migration/cli/bin studio";

type StudioHandler = (options: Record<string, unknown>) => Promise<void>;

function studioModuleSpecifier(): string {
  return import.meta.url.endsWith(".ts") ? "./studio.ts" : "./studio.js";
}

async function loadStudioCommand(): Promise<StudioHandler | undefined> {
  const specifier = studioModuleSpecifier();
  try {
    const module = await import(specifier);
    return module.studioCommand as StudioHandler;
  } catch (error) {
    const code = (error as { code?: string }).code;
    const message = error instanceof Error ? error.message : String(error);
    const missing =
      code === "ERR_MODULE_NOT_FOUND" ||
      /not found|cannot find module|module not found/i.test(message);
    if (missing && /studio/i.test(message)) {
      return undefined;
    }
    throw error;
  }
}

export async function studioEntry(
  options: Record<string, unknown> = {},
): Promise<void> {
  const handler = await loadStudioCommand();
  if (!handler) {
    console.log(yellow(STUDIO_UNAVAILABLE_MESSAGE));
    return;
  }
  await handler(options);
}
