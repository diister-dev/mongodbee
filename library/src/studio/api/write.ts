import * as v from "../../schema.ts";
import { collection } from "../../collection.ts";
import { multiCollection } from "../../multi-collection.ts";
import { scopedMultiCollection } from "../../scoped-multi-collection.ts";
import { duplicateKeyOf, isDuplicateKeyError } from "../../duplicate-key.ts";
import { REMOVE_FIELD } from "../../sanitizer.ts";
import {
  type DocumentId,
  isDocumentId,
  type StoredDocument,
  storedCollection,
} from "../../stored-document.ts";
import type { CatalogEntry } from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import { encodeId, parseExtendedJson, StudioHttpError } from "../http.ts";
import { fieldsOf, isTypeDefinition } from "../../type-definition.ts";
import { requireEntry } from "./documents.ts";
import { getMigrationsReport } from "./migrations.ts";

export const WRITE_HEADER = "x-mongodbee-studio";
export const PROTECTED_FIELDS = ["_id", "_type", "_scope"] as const;
const FIELD_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

export interface UpdateBody {
  id: string;
  type?: string;
  scope?: string;
  set?: Record<string, unknown>;
  unset?: string[];
  expected?: Record<string, unknown>;
}

export interface InsertBody {
  type?: string;
  scope?: string;
  document: Record<string, unknown>;
}

export interface DeleteBody {
  id: string;
  type?: string;
  scope?: string;
  confirm: string;
}

export interface WriteIssue {
  path: string;
  message: string;
}

export interface WriteLogEntry {
  action: "insert" | "update" | "delete";
  collection: string;
  id: string;
  fields?: string[];
}

interface WriteTarget {
  type?: string;
  scope?: string;
}

interface Writer {
  update(
    target: WriteTarget,
    filter: Record<string, unknown>,
    set: Record<string, unknown>,
    unset: string[],
  ): Promise<{ matched: number; modified: number }>;
  insert(
    target: WriteTarget,
    document: Record<string, unknown>,
  ): Promise<unknown>;
  remove(target: WriteTarget, id: DocumentId): Promise<number>;
}

function stringId(id: DocumentId): string {
  if (typeof id !== "string") {
    throw new StudioHttpError(
      400,
      "Documents of this collection have string ids",
    );
  }
  return id;
}

function typeOf(target: WriteTarget): string {
  if (!target.type) throw new StudioHttpError(400, "Choose a type first");
  return target.type;
}

function scopeOf(target: WriteTarget): string {
  if (!target.scope) throw new StudioHttpError(400, "Choose a scope first");
  return target.scope;
}

function removals(set: Record<string, unknown>, unset: string[]) {
  const doc: Record<string, unknown> = { ...set };
  for (const path of unset) doc[path] = REMOVE_FIELD;
  return doc;
}

async function writerFor(
  context: StudioContext,
  entry: CatalogEntry,
): Promise<Writer> {
  const options = { schemaManagement: "managed" as const };
  if (entry.kind === "collection") {
    const source = entry.types[entry.name];
    if (!source) {
      throw new StudioHttpError(409, `No schema for "${entry.name}"`);
    }
    const handle = isTypeDefinition(source)
      ? await collection(context.db, entry.name, source, options)
      : await collection(context.db, entry.name, source, options);
    return {
      async update(_target, filter, set, unset) {
        const update: {
          $set?: Record<string, unknown>;
          $unset?: Record<string, "">;
        } = {};
        if (Object.keys(set).length > 0) update.$set = set;
        if (unset.length > 0) {
          update.$unset = Object.fromEntries(unset.map((path) => [path, ""]));
        }
        const result = await handle.updateOne(filter, update);
        return { matched: result.matchedCount, modified: result.modifiedCount };
      },
      async insert(_target, document) {
        return await handle.insertOne(document);
      },
      async remove(_target, id) {
        const filter: Record<string, unknown> = { _id: id };
        const result = await handle.deleteOne(filter);
        return result.deletedCount;
      },
    };
  }
  if (entry.kind === "multiCollection") {
    const handle = await multiCollection(
      context.db,
      entry.name,
      entry.types,
      options,
    );
    return {
      async update(target, filter, set, unset) {
        return await handle.updateWhere(
          typeOf(target),
          filter,
          removals(set, unset),
        );
      },
      async insert(target, document) {
        return await handle.insertOne(typeOf(target), document);
      },
      async remove(target, id) {
        return await handle.deleteId(typeOf(target), stringId(id));
      },
    };
  }
  if (entry.kind === "scopedMultiCollection" && entry.scope) {
    const handle = await scopedMultiCollection(context.db, entry.name, {
      scope: entry.scope,
      types: entry.types,
      ...options,
    });
    return {
      async update(target, filter, set, unset) {
        return await handle
          .scope(scopeOf(target))
          .updateWhere(typeOf(target), filter, removals(set, unset));
      },
      async insert(target, document) {
        return await handle
          .scope(scopeOf(target))
          .insertOne(typeOf(target), document);
      },
      async remove(target, id) {
        return await handle
          .scope(scopeOf(target))
          .deleteId(typeOf(target), stringId(id));
      },
    };
  }
  throw new StudioHttpError(
    409,
    `"${entry.name}" is a ${entry.kind}; the studio only writes to collections, multi-collections and scoped collections`,
  );
}

const cache = new WeakMap<StudioContext, Map<string, Promise<Writer>>>();

function handleFor(
  context: StudioContext,
  entry: CatalogEntry,
): Promise<Writer> {
  let perContext = cache.get(context);
  if (!perContext) {
    perContext = new Map();
    cache.set(context, perContext);
  }
  const cached = perContext.get(entry.name);
  if (cached) return cached;
  const created = writerFor(context, entry);
  perContext.set(entry.name, created);
  created.catch(() => perContext.delete(entry.name));
  return created;
}

export async function assertWritable(context: StudioContext): Promise<void> {
  if (!context.write) {
    throw new StudioHttpError(
      405,
      "The studio is read-only: start it with --write to edit documents",
    );
  }
  if (context.schemasSource !== "project") {
    throw new StudioHttpError(
      409,
      "Writing needs the project's schemas.ts; the studio could not load it",
    );
  }
  const report = await getMigrationsReport(context);
  if (report.pending > 0) {
    throw new StudioHttpError(
      409,
      `${report.pending} migration${report.pending === 1 ? " is" : "s are"} pending: apply them with mongodbee migrate before editing, so documents are written in the shape the database is in`,
    );
  }
}

export function assertWriteRequest(request: Request): void {
  if (request.headers.get(WRITE_HEADER) !== "write") {
    throw new StudioHttpError(403, `Writes need the ${WRITE_HEADER} header`);
  }
  const type = request.headers.get("content-type") ?? "";
  if (!type.startsWith("application/json")) {
    throw new StudioHttpError(415, "Writes take a JSON body");
  }
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) {
    throw new StudioHttpError(
      403,
      "Writes are only accepted from the studio itself",
    );
  }
}

export async function readJsonBody<T>(request: Request): Promise<T> {
  const text = await request.text();
  const value = parseExtendedJson(text);
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new StudioHttpError(400, "The body must be a JSON object");
  }
  return value as T;
}

function checkField(path: string): void {
  if (!FIELD_NAME.test(path)) {
    throw new StudioHttpError(400, `Invalid field "${path}"`);
  }
  if ((PROTECTED_FIELDS as readonly string[]).includes(path)) {
    throw new StudioHttpError(400, `${path} cannot be edited`);
  }
}

function parseId(raw: unknown): DocumentId {
  if (typeof raw !== "string" || raw === "") {
    throw new StudioHttpError(400, "Missing document id");
  }
  const id = parseExtendedJson(raw);
  if (!isDocumentId(id)) {
    throw new StudioHttpError(
      400,
      "The studio writes documents whose id is a string or an ObjectId",
    );
  }
  return id;
}

function requireType(entry: CatalogEntry, type: string | undefined): string {
  if (!type || !(type in entry.types)) {
    throw new StudioHttpError(
      400,
      `Choose one of the types of ${entry.name}: ${Object.keys(entry.types).join(", ")}`,
    );
  }
  return type;
}

function requireScope(entry: CatalogEntry, scope: string | undefined): string {
  if (entry.kind !== "scopedMultiCollection") return "";
  if (!scope) {
    throw new StudioHttpError(400, `Writing to ${entry.name} needs a scope`);
  }
  return scope;
}

export function issuesOf(error: unknown): WriteIssue[] | undefined {
  if (!v.isValiError(error)) return undefined;
  return error.issues.map((issue) => ({
    path: (issue.path ?? [])
      .map((item) => String((item as { key?: unknown }).key ?? ""))
      .filter(Boolean)
      .join("."),
    message: issue.message,
  }));
}

function fieldsFor(
  entry: CatalogEntry,
  type: string | undefined,
): Record<string, v.GenericSchema> {
  const source = entry.types[type ?? entry.name];
  return (source ? fieldsOf(source) : {}) as Record<string, v.GenericSchema>;
}

function pathOf(issue: v.BaseIssue<unknown>, prefix: string): string {
  const inner = (issue.path ?? [])
    .map((item) => String((item as { key?: unknown }).key ?? ""))
    .filter(Boolean);
  return [prefix, ...inner].filter(Boolean).join(".");
}

export function checkUpdate(
  fields: Record<string, v.GenericSchema>,
  set: Record<string, unknown>,
  unset: string[],
): WriteIssue[] {
  const issues: WriteIssue[] = [];
  for (const [key, value] of Object.entries(set)) {
    const schema = fields[key];
    if (!schema) {
      issues.push({ path: key, message: "Not declared in the schema" });
      continue;
    }
    const result = v.safeParse(schema, value);
    if (!result.success) {
      for (const issue of result.issues) {
        issues.push({ path: pathOf(issue, key), message: issue.message });
      }
    }
  }
  for (const key of unset) {
    const schema = fields[key];
    if (schema && !v.safeParse(schema, undefined).success) {
      issues.push({ path: key, message: "Required: it cannot be removed" });
    }
  }
  return issues;
}

export function checkInsert(
  fields: Record<string, v.GenericSchema>,
  document: Record<string, unknown>,
): WriteIssue[] {
  const { _id: _ignored, ...rest } = document;
  const issues: WriteIssue[] = [];
  for (const key of Object.keys(rest)) {
    if (!(key in fields)) {
      issues.push({ path: key, message: "Not declared in the schema" });
    }
  }
  const result = v.safeParse(v.object(fields), rest);
  if (!result.success) {
    for (const issue of result.issues) {
      issues.push({ path: pathOf(issue, ""), message: issue.message });
    }
  }
  return issues;
}

function refuse(issues: WriteIssue[]): void {
  if (issues.length === 0) return;
  throw new StudioHttpError(
    422,
    `The document does not match its schema (${issues.length} issue${issues.length === 1 ? "" : "s"})`,
    { issues },
  );
}

function translate(error: unknown): never {
  if (error instanceof StudioHttpError) throw error;
  const issues = issuesOf(error);
  if (issues) {
    throw new StudioHttpError(
      422,
      `The document does not match its schema (${issues.length} issue${issues.length === 1 ? "" : "s"})`,
      { issues },
    );
  }
  if (isDuplicateKeyError(error)) {
    const details = duplicateKeyOf(error);
    throw new StudioHttpError(
      409,
      "Another document already has this value for a unique index",
      details ? { duplicate: details } : undefined,
    );
  }
  const message = error instanceof Error ? error.message : String(error);
  if (/no element (found|that match)/i.test(message)) {
    throw new StudioHttpError(
      404,
      "No such document of this type in this scope",
    );
  }
  if (/Document failed validation/i.test(message)) {
    throw new StudioHttpError(422, "MongoDB's validator refused the document", {
      issues: [{ path: "", message }],
    });
  }
  throw error;
}

function log(context: StudioContext, entry: WriteLogEntry): void {
  const fields = entry.fields?.length
    ? ` fields ${entry.fields.join(", ")}`
    : "";
  console.log(
    `[studio] ${entry.action} ${entry.collection} ${entry.id}${fields}`,
  );
  void context;
}

export async function updateDocument(
  context: StudioContext,
  name: string,
  body: UpdateBody,
): Promise<{ matched: number; modified: number }> {
  await assertWritable(context);
  const entry = await requireEntry(context, name);
  const id = parseId(body.id);
  const set = body.set ?? {};
  const unset = body.unset ?? [];
  const expected = body.expected ?? {};
  for (const path of [...Object.keys(set), ...unset]) checkField(path);
  if (Object.keys(set).length === 0 && unset.length === 0) {
    return { matched: 1, modified: 0 };
  }
  const guard: Record<string, unknown> = {};
  for (const path of [...Object.keys(set), ...unset]) {
    guard[path] = path in expected ? expected[path] : { $exists: false };
  }
  const checkedType =
    entry.kind === "collection" ? undefined : requireType(entry, body.type);
  requireScope(entry, body.scope);
  refuse(checkUpdate(fieldsFor(entry, checkedType), set, unset));
  const handle = await handleFor(context, entry);
  let matched: number;
  let modified: number;
  try {
    const result = await handle.update(
      { type: checkedType, scope: body.scope },
      { _id: id, ...guard },
      set,
      unset,
    );
    matched = result.matched;
    modified = result.modified;
  } catch (error) {
    translate(error);
  }
  if (matched === 0) {
    throw new StudioHttpError(
      409,
      "The document changed since it was opened, or no longer exists: reload it before editing",
    );
  }
  log(context, {
    action: "update",
    collection: entry.name,
    id: encodeId(id),
    fields: [...Object.keys(set), ...unset],
  });
  return { matched, modified };
}

export async function insertDocument(
  context: StudioContext,
  name: string,
  body: InsertBody,
): Promise<{ id: unknown }> {
  await assertWritable(context);
  const entry = await requireEntry(context, name);
  const document = body.document;
  if (!document || typeof document !== "object" || Array.isArray(document)) {
    throw new StudioHttpError(400, "Missing document");
  }
  for (const key of Object.keys(document)) {
    if (key === "_id") continue;
    checkField(key);
  }
  const checkedType =
    entry.kind === "collection" ? undefined : requireType(entry, body.type);
  requireScope(entry, body.scope);
  refuse(checkInsert(fieldsFor(entry, checkedType), document));
  const handle = await handleFor(context, entry);
  let id: unknown;
  try {
    id = await handle.insert(
      { type: checkedType, scope: body.scope },
      document,
    );
  } catch (error) {
    translate(error);
  }
  log(context, { action: "insert", collection: entry.name, id: encodeId(id) });
  return { id };
}

export async function deleteDocument(
  context: StudioContext,
  name: string,
  body: DeleteBody,
): Promise<{ deleted: number; restoreToken?: string }> {
  await assertWritable(context);
  const entry = await requireEntry(context, name);
  const id = parseId(body.id);
  const label = typeof id === "string" ? id : id.toHexString();
  if (body.confirm !== label) {
    throw new StudioHttpError(
      400,
      `Type the document id, ${label}, to confirm the deletion`,
    );
  }
  const handle = await handleFor(context, entry);
  const snapshot = await storedCollection(context.db, entry.name).findOne({
    _id: id,
  });
  let deleted: number;
  const checkedType =
    entry.kind === "collection" ? undefined : requireType(entry, body.type);
  requireScope(entry, body.scope);
  try {
    deleted = await handle.remove({ type: checkedType, scope: body.scope }, id);
  } catch (error) {
    translate(error);
  }
  if (deleted === 0) {
    throw new StudioHttpError(404, `No document ${label} to delete`);
  }
  log(context, { action: "delete", collection: entry.name, id: encodeId(id) });
  if (!snapshot) return { deleted };
  return {
    deleted,
    restoreToken: keepForRestore(context, entry.name, snapshot),
  };
}

export const RESTORE_WINDOW_MS = 10 * 60 * 1000;

interface KeptDocument {
  collection: string;
  document: StoredDocument;
  expires: number;
}

const kept = new WeakMap<StudioContext, Map<string, KeptDocument>>();

function keepForRestore(
  context: StudioContext,
  collectionName: string,
  document: StoredDocument,
): string {
  let store = kept.get(context);
  if (!store) {
    store = new Map();
    kept.set(context, store);
  }
  const now = Date.now();
  for (const [token, item] of store) {
    if (item.expires < now) store.delete(token);
  }
  const token = `${now.toString(36)}${Math.random().toString(36).slice(2, 10)}`;
  store.set(token, {
    collection: collectionName,
    document,
    expires: now + RESTORE_WINDOW_MS,
  });
  return token;
}

export interface RestoreBody {
  token: string;
}

export async function restoreDocument(
  context: StudioContext,
  name: string,
  body: RestoreBody,
): Promise<{ id: unknown }> {
  await assertWritable(context);
  const entry = await requireEntry(context, name);
  const store = kept.get(context);
  const item =
    typeof body.token === "string" ? store?.get(body.token) : undefined;
  if (!item || item.collection !== entry.name || item.expires < Date.now()) {
    throw new StudioHttpError(
      410,
      "This deletion can no longer be undone from the studio",
    );
  }
  try {
    await storedCollection(context.db, entry.name).insertOne(item.document);
  } catch (error) {
    translate(error);
  }
  store?.delete(body.token);
  log(context, {
    action: "insert",
    collection: entry.name,
    id: encodeId(item.document._id),
  });
  return { id: item.document._id };
}

export interface ValidateBody {
  type?: string;
  document: Record<string, unknown>;
}

export async function validateDocument(
  context: StudioContext,
  name: string,
  body: ValidateBody,
): Promise<{ issues: WriteIssue[] }> {
  const entry = await requireEntry(context, name);
  const document = body.document;
  if (!document || typeof document !== "object" || Array.isArray(document)) {
    throw new StudioHttpError(400, "Missing document");
  }
  const type =
    entry.kind === "collection" ? undefined : requireType(entry, body.type);
  const rest = { ...document };
  for (const key of PROTECTED_FIELDS) delete rest[key];
  return { issues: checkInsert(fieldsFor(entry, type), rest) };
}
