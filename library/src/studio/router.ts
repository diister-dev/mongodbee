import { VERSION } from "../version.ts";
import type { StudioContext } from "./context.ts";
import {
  clampInteger,
  errorResponse,
  jsonResponse,
  StudioHttpError,
} from "./http.ts";
import { getOverview } from "./api/overview.ts";
import {
  getDocument,
  listDocuments,
  listScopes,
  parseDocumentsQuery,
} from "./api/documents.ts";
import { getCollectionSchema } from "./api/schema.ts";
import { getIndexReport } from "./api/indexes.ts";
import { getMigrationsReport } from "./api/migrations.ts";
import { getPlan } from "./api/plan.ts";
import { getDrift } from "./api/drift.ts";
import { getHistory } from "./api/history.ts";
import { streamCheck } from "./api/check.ts";
import { getCollectionSummary } from "./api/summary.ts";
import { listFieldValues } from "./api/values.ts";
import { getLabels } from "./api/labels.ts";
import { getFieldCoverage } from "./api/coverage.ts";
import {
  assertWriteRequest,
  type DeleteBody,
  deleteDocument,
  type InsertBody,
  insertDocument,
  readJsonBody,
  type RestoreBody,
  restoreDocument,
  type UpdateBody,
  updateDocument,
  type ValidateBody,
  validateDocument,
} from "./api/write.ts";

const WRITE_METHODS = new Set(["POST", "PATCH", "DELETE"]);

async function write(
  context: StudioContext,
  request: Request,
): Promise<unknown> {
  if (!context.write) {
    throw new StudioHttpError(
      405,
      "The studio is read-only: start it with --write to edit documents",
    );
  }
  assertWriteRequest(request);
  const url = new URL(request.url);
  const match = COLLECTION_ROUTE.exec(url.pathname);
  if (match) {
    const name = decodeURIComponent(match[1]);
    const target = `${request.method} ${match[2]}`;
    if (target === "PATCH document") {
      return await updateDocument(
        context,
        name,
        await readJsonBody<UpdateBody>(request),
      );
    }
    if (target === "POST documents") {
      return await insertDocument(
        context,
        name,
        await readJsonBody<InsertBody>(request),
      );
    }
    if (target === "DELETE document") {
      return await deleteDocument(
        context,
        name,
        await readJsonBody<DeleteBody>(request),
      );
    }
    if (target === "POST restore") {
      return await restoreDocument(
        context,
        name,
        await readJsonBody<RestoreBody>(request),
      );
    }
    if (target === "POST validate") {
      return await validateDocument(
        context,
        name,
        await readJsonBody<ValidateBody>(request),
      );
    }
  }
  throw new StudioHttpError(
    404,
    `No write route for ${request.method} ${url.pathname}`,
  );
}

const COLLECTION_ROUTE =
  /^\/api\/collections\/([^/]+)\/(documents|document|schema|indexes|scopes|summary|values|coverage|restore|validate)$/;

export function getMeta(context: StudioContext): Record<string, unknown> {
  return {
    version: VERSION,
    database: context.db.databaseName,
    schemasSource: context.schemasSource,
    migrationsCount: context.migrations.length,
    warnings: context.warnings,
    paths: context.paths ?? {},
    readOnly: !context.write,
    write: Boolean(context.write),
    buildId: context.buildId ?? null,
  };
}

async function route(context: StudioContext, url: URL): Promise<unknown> {
  const pathname = url.pathname;
  const params = url.searchParams;

  if (pathname === "/api/meta") return getMeta(context);
  if (pathname === "/api/overview") {
    return await getOverview(context, {
      topScopes: clampInteger(params.get("scopes"), 10, 1, 100),
    });
  }
  if (pathname === "/api/migrations") return await getMigrationsReport(context);
  if (pathname === "/api/labels") return await getLabels(context, params);
  if (pathname === "/api/migrations/plan") return await getPlan(context);
  if (pathname === "/api/migrations/drift") return await getDrift(context);
  if (pathname === "/api/migrations/history") return await getHistory(context);

  const match = COLLECTION_ROUTE.exec(pathname);
  if (match) {
    const name = decodeURIComponent(match[1]);
    switch (match[2]) {
      case "documents":
        return await listDocuments(context, name, parseDocumentsQuery(params));
      case "document": {
        const id = params.get("id");
        if (!id) throw new StudioHttpError(400, "Missing id parameter");
        return await getDocument(context, name, id);
      }
      case "schema":
        return await getCollectionSchema(context, name);
      case "indexes":
        return await getIndexReport(context, name);
      case "summary":
        return await getCollectionSummary(context, name);
      case "values":
        return await listFieldValues(context, name, params);
      case "coverage":
        return await getFieldCoverage(context, name, params);
      case "scopes":
        return await listScopes(
          context,
          name,
          clampInteger(params.get("limit"), 50, 1, 200),
        );
    }
  }

  throw new StudioHttpError(404, `No route for ${pathname}`);
}

export async function handleApiRequest(
  context: StudioContext,
  request: Request,
): Promise<Response> {
  if (WRITE_METHODS.has(request.method) && context.write) {
    try {
      return jsonResponse(await write(context, request));
    } catch (error) {
      return errorResponse(error);
    }
  }
  if (request.method !== "GET" && request.method !== "HEAD") {
    return errorResponse(
      new StudioHttpError(405, "The studio is read-only: only GET is allowed"),
    );
  }
  try {
    const url = new URL(request.url);
    if (url.pathname === "/api/migrations/check")
      return streamCheck(context, url);
    return jsonResponse(await route(context, url));
  } catch (error) {
    return errorResponse(error);
  }
}
