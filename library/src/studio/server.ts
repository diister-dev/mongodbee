import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import type { StudioContext } from "./context.ts";
import { handleApiRequest } from "./router.ts";
import { loadUiBundle, type UiBundle } from "./ui-bundle.ts";

export const DEFAULT_STUDIO_PORT = 4983;
export const DEFAULT_STUDIO_HOST = "127.0.0.1";

export interface StudioServerOptions {
  port?: number;
  host?: string;
  ui?: boolean;
}

export interface StudioServer {
  url: string;
  port: number;
  stop(): Promise<void>;
}

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]", "::1"]);

function allowedHost(request: Request, bound: string): boolean {
  const header = request.headers.get("host");
  if (!header) return false;
  const hostname = header.startsWith("[")
    ? header.slice(0, header.indexOf("]") + 1)
    : header.split(":")[0];
  return LOOPBACK_HOSTS.has(hostname) || hostname === bound;
}

function serveAsset(bundle: UiBundle, pathname: string): Response | undefined {
  const asset = bundle.get(pathname);
  if (!asset) return undefined;
  const html = asset.type.startsWith("text/html");
  const headers: Record<string, string> = {
    "content-type": asset.type,
    "cache-control": html
      ? "no-cache, no-store, must-revalidate, max-age=0"
      : "public, max-age=31536000, immutable",
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
  };
  if (html) {
    headers.pragma = "no-cache";
    headers.expires = "0";
  }
  return new Response(asset.body, { headers });
}

export function createStudioHandler(
  context: StudioContext,
  bundle: UiBundle,
  host: string,
): (request: Request) => Promise<Response> {
  return async (request) => {
    if (!allowedHost(request, host)) {
      return new Response("Forbidden host", { status: 403 });
    }
    const { pathname } = new URL(request.url);
    if (pathname.startsWith("/api/")) {
      return await handleApiRequest(context, request);
    }
    const asset = serveAsset(bundle, pathname);
    if (asset) return asset;
    const fallback = pathname.includes(".")
      ? undefined
      : serveAsset(bundle, "/");
    return fallback ?? new Response("Not found", { status: 404 });
  };
}

export const MAX_BODY_BYTES = 1_000_000;

export function isLoopbackHost(host: string): boolean {
  return LOOPBACK_HOSTS.has(host) || LOOPBACK_HOSTS.has(`[${host}]`);
}

function readBody(incoming: IncomingMessage): Promise<Uint8Array | null> {
  return new Promise((resolve, reject) => {
    const chunks: Uint8Array[] = [];
    let size = 0;
    let tooLarge = false;
    incoming.on("data", (chunk: Uint8Array) => {
      if (tooLarge) return;
      size += chunk.byteLength;
      if (size > MAX_BODY_BYTES) {
        tooLarge = true;
        resolve(null);
        return;
      }
      chunks.push(chunk);
    });
    incoming.on("end", () => {
      if (tooLarge) return;
      const body = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) {
        body.set(chunk, offset);
        offset += chunk.byteLength;
      }
      resolve(body);
    });
    incoming.on("error", reject);
  });
}

async function toRequestWithBody(
  incoming: IncomingMessage,
  signal: AbortSignal,
): Promise<Request | null> {
  const method = incoming.method ?? "GET";
  if (method === "GET" || method === "HEAD") return toRequest(incoming, signal);
  const body = await readBody(incoming);
  if (body === null) return null;
  const base = toRequest(incoming, signal);
  return new Request(base.url, {
    method,
    headers: base.headers,
    body: body as unknown as BodyInit,
    signal,
  });
}

function toRequest(incoming: IncomingMessage, signal: AbortSignal): Request {
  const headers = new Headers();
  for (const [name, value] of Object.entries(incoming.headers)) {
    if (value === undefined) continue;
    if (Array.isArray(value)) {
      for (const item of value) headers.append(name, item);
    } else {
      headers.set(name, value);
    }
  }
  const authority = incoming.headers.host ?? "localhost";
  return new Request(`http://${authority}${incoming.url ?? "/"}`, {
    method: incoming.method ?? "GET",
    headers,
    signal,
  });
}

async function writeResponse(
  response: Response,
  outgoing: ServerResponse,
  signal: AbortSignal,
): Promise<void> {
  const headers: Record<string, string | string[]> = {};
  response.headers.forEach((value, name) => {
    headers[name] = value;
  });
  outgoing.writeHead(response.status, response.statusText, headers);
  if (!response.body) {
    outgoing.end();
    return;
  }
  const reader = response.body.getReader();
  const stop = () => {
    reader.cancel().catch(() => {});
  };
  signal.addEventListener("abort", stop, { once: true });
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done || signal.aborted) break;
      outgoing.write(value);
    }
  } catch {
    stop();
  } finally {
    signal.removeEventListener("abort", stop);
    outgoing.end();
  }
}

export async function startStudioServer(
  context: StudioContext,
  options: StudioServerOptions = {},
): Promise<StudioServer> {
  const host = options.host ?? DEFAULT_STUDIO_HOST;
  if (context.write && !isLoopbackHost(host)) {
    throw new Error(
      `Writing keeps the studio on the loopback interface; "${host}" is not a loopback address`,
    );
  }
  const bundle: UiBundle =
    options.ui === false ? new Map() : await loadUiBundle();
  const served: StudioContext = {
    ...context,
    buildId: `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`,
  };
  const handle = createStudioHandler(served, bundle, host);

  const server = createServer((incoming, outgoing) => {
    const controller = new AbortController();
    outgoing.on("close", () => controller.abort());
    toRequestWithBody(incoming, controller.signal)
      .then((request) =>
        request === null
          ? new Response(
              JSON.stringify({ error: "The request body is too large" }),
              { status: 413, headers: { "content-type": "application/json" } },
            )
          : handle(request),
      )
      .then((response) => writeResponse(response, outgoing, controller.signal))
      .catch((error) => {
        if (!outgoing.headersSent) {
          outgoing.writeHead(500, { "content-type": "text/plain" });
        }
        outgoing.end(error instanceof Error ? error.message : String(error));
      });
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port ?? DEFAULT_STUDIO_PORT, host, () => {
      server.off("error", reject);
      resolve();
    });
  });

  const port = (server.address() as AddressInfo).port;
  const displayHost = host.includes(":") ? `[${host}]` : host;
  return {
    url: `http://${displayHost}:${port}`,
    port,
    stop: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections?.();
        server.close(() => resolve());
      }),
  };
}
