import { COMPUTED_ROOT } from "../computed-guard.ts";
import { type ComputedField, computedTopology } from "../computed-topology.ts";
import { isRecord } from "../utils/guards.ts";
import type { MigrationDefinition } from "./types.ts";

export function migrationComputedField(
  migration: MigrationDefinition,
  type: string,
  name: string,
): ComputedField {
  return computedTopology(migration.schemas).field(type, name);
}

export function parentDeclaresComputed(
  migration: MigrationDefinition,
  type: string,
): boolean {
  const parent = migration.parent?.schemas;
  return (
    parent !== undefined && computedTopology(parent).fieldsOf(type).length > 0
  );
}

export function withComputedValue(
  document: Record<string, unknown>,
  name: string,
  value: unknown,
): Record<string, unknown> {
  const root = document[COMPUTED_ROOT];
  return {
    ...document,
    [COMPUTED_ROOT]: { ...(isRecord(root) ? root : {}), [name]: value },
  };
}

export function withoutComputedValue(
  document: Record<string, unknown>,
  name: string,
  keepRoot: boolean,
): Record<string, unknown> {
  const { [COMPUTED_ROOT]: root, ...rest } = document;
  if (!keepRoot || !isRecord(root)) return rest;
  const { [name]: _removed, ...kept } = root;
  return { ...rest, [COMPUTED_ROOT]: kept };
}

export function computedUnsetPath(name: string, keepRoot: boolean): string {
  return keepRoot ? `${COMPUTED_ROOT}.${name}` : COMPUTED_ROOT;
}
