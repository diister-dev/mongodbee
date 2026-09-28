import process from "node:process";
import { blue, bold, dim, green, yellow } from "../../../utils/colors.ts";
import { MongoClient } from "../../../mongodb.ts";
import { loadStudioProject } from "../../../studio/context.ts";

export interface StudioCommandOptions {
  configPath?: string;
  cwd?: string;
  project?: string;
  uri?: string;
  db?: string;
  migrations?: string;
  schemas?: string;
  port?: number | string;
  host?: string;
  write?: boolean;
}

export const STUDIO_URI_ENV = "MONGODBEE_STUDIO_URI";

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== ""
    ? value.trim()
    : undefined;
}

function parsePort(raw: number | string | undefined): number | undefined {
  if (raw === undefined || raw === "") return undefined;
  const port = typeof raw === "number" ? raw : Number.parseInt(raw, 10);
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`Invalid --port value: ${raw}`);
  }
  return port;
}

export async function studioCommand(
  options: StudioCommandOptions = {},
): Promise<void> {
  console.log(bold(blue("🐝 MongoDBee Studio")));
  console.log();

  const project = await loadStudioProject({
    configPath: optionalString(options.configPath),
    cwd: optionalString(options.project) ?? optionalString(options.cwd),
    uri:
      optionalString(options.uri) ??
      optionalString(process.env[STUDIO_URI_ENV]),
    dbName: optionalString(options.db),
    migrationsDir: optionalString(options.migrations),
    schemaPath: optionalString(options.schemas),
  });
  const client = new MongoClient(project.connectionUri);
  await client.connect();
  const db = client.db(project.dbName);

  const { startStudioServer } = await import("../../../studio/server.ts");
  const server = await startStudioServer(
    {
      db,
      schemas: project.schemas,
      schemasSource: project.schemasSource,
      migrations: project.migrations,
      migrationFiles: project.migrationFiles,
      warnings: project.warnings,
      paths: project.paths,
      project: project.root,
      write: options.write === true,
    },
    { port: parsePort(options.port), host: options.host },
  );

  console.log(dim(`Project: ${project.root.cwd}`));
  console.log(dim(`Database: ${project.dbName}`));
  console.log(
    dim(`Schemas: ${project.schemasSource} (${project.paths.schemas})`),
  );
  console.log(
    dim(
      `Migrations: ${project.migrations.length} (${project.paths.migrations})`,
    ),
  );
  for (const warning of project.warnings) {
    console.log(yellow(`⚠ ${warning}`));
  }
  console.log();
  console.log(
    green(
      `Studio ready on ${bold(server.url)} ${options.write === true ? "(write enabled: edits go to the database)" : "(read-only)"}`,
    ),
  );
  console.log(dim("Press Ctrl+C to stop"));

  await new Promise<void>((resolve) => {
    const shutdown = () => {
      process.off("SIGINT", shutdown);
      process.off("SIGTERM", shutdown);
      resolve();
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  });

  await server.stop();
  await client.close(true);
}
