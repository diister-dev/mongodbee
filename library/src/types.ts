/**
 * @fileoverview Frontend-safe, **type-only** utilities.
 *
 * This module imports NOTHING at runtime — no mongodb driver, no valibot, no
 * other mongodbee internals. It exists so browser / SvelteKit code (or any
 * consumer that must not pull the server-only ODM graph) can import these
 * helpers via `@diister/mongodbee/types` without dragging in the whole
 * library. Keep it dependency-free: only `type` declarations belong here.
 *
 * @module
 */

/**
 * The fields a scoped multi-collection manages on every document: `_id`
 * (auto / scope-derived), `_type` (discriminator) and `_scope` (partition
 * key). Single source of truth — if the reserved set ever changes, update
 * it here only.
 */
export type ScopedMetaField = "_id" | "_type" | "_scope";

/**
 * Strip the scoped meta fields from a stored/output document type, yielding
 * the user-writable input shape. Use this instead of hand-rolling
 * `Omit<Doc, "_id" | "_type" | "_scope">` at every repository callsite.
 *
 * @example
 * ```typescript
 * import type { OmitScopedMeta } from "@diister/mongodbee/types";
 * type ParticipantInput = OmitScopedMeta<ParticipantDoc>;
 * ```
 */
export type OmitScopedMeta<T> = Omit<T, ScopedMetaField>;
