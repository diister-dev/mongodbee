export class ApiError extends Error {
  constructor(message, { status = null, path = "", transport = false, method = "GET" } = {}) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.path = path;
    this.transport = transport;
    this.method = method;
  }
}

export async function api(path, params = {}) {
  const url = new URL(path, window.location.origin);
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    if (Array.isArray(value)) {
      for (const item of value) url.searchParams.append(key, String(item));
      continue;
    }
    url.searchParams.set(key, String(value));
  }
  let response;
  try {
    response = await fetch(url, { headers: { accept: "application/json" } });
  } catch (error) {
    throw new ApiError(error instanceof Error ? error.message : String(error), { path, transport: true });
  }
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new ApiError(body.error ?? `${response.status} ${response.statusText}`, { status: response.status, path });
  }
  return body;
}

export function collectionPath(name, resource) {
  return `/api/collections/${encodeURIComponent(name)}/${resource}`;
}

export async function send(method, path, body) {
  const url = new URL(path, window.location.origin);
  let response;
  try {
    response = await fetch(url, {
      method,
      headers: { accept: "application/json", "content-type": "application/json", "x-mongodbee-studio": "write" },
      body: JSON.stringify(body),
    });
  } catch (error) {
    throw new ApiError(error instanceof Error ? error.message : String(error), { path, transport: true, method });
  }
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new ApiError(result.error ?? `${response.status} ${response.statusText}`, {
      status: response.status,
      path,
      method,
    });
    error.details = result;
    throw error;
  }
  return result;
}
