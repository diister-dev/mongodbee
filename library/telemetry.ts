/**
 * @module
 *
 * Public OpenTelemetry telemetry surface of MongoDBee 🐝
 *
 * Re-exports the opt-in tracing configuration types and the attribute-name
 * constants. See `doc/TELEMETRY.md` for the full guide.
 *
 * @example
 * ```typescript
 * import type { TelemetryOptions } from "@diister/mongodbee/telemetry";
 *
 * const telemetry: TelemetryOptions = { enabled: true };
 * const users = await collection(db, "users", schema, { telemetry });
 * ```
 */
export { TELEMETRY_ATTRIBUTES } from "./src/telemetry.ts";
export type { TelemetryOptions } from "./src/telemetry.ts";
