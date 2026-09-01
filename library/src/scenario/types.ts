import type { ResolveNode } from "@diister/valibot-mock";
import type { DatabaseState, SchemasDefinition } from "../migration/types.ts";

export interface SeedAnchors {
  readonly collections?: Record<string, Record<string, unknown>[]>;
  readonly multiCollections?: Record<string, Record<string, unknown>[]>;
  readonly multiModels?: Record<
    string,
    { modelType: string; content: Record<string, unknown>[] }
  >;
  readonly scopedMultiCollections?: Record<string, Record<string, unknown>[]>;
}

export interface SeedShapeContext {
  readonly parent?: Record<string, unknown>;
  readonly scope: string | null;
  random(): number;
}

export type SeedCount = number | ((context: SeedShapeContext) => number);

export type SeedShapeEntry =
  | SeedCount
  | { readonly per: string; readonly count: SeedCount };

export type SeedShape = Record<string, SeedShapeEntry>;

export interface SeedRandom {
  random(): number;
  int(min: number, max: number): number;
  chance(probability: number): boolean;
  oneOf<T>(values: readonly T[]): T;
  weighted<T>(choices: readonly (readonly [T, number])[]): T;
  dateBetween(from: Date | string, to: Date | string): Date;
}

export interface SeedWorld extends SeedRandom {
  docs(target: string): readonly Record<string, unknown>[];
  pick(
    target: string,
    filter?: (doc: Record<string, unknown>) => boolean,
  ): Record<string, unknown> | undefined;
}

export interface SeedRuleContext extends SeedWorld {
  readonly target: string;
  readonly path: string;
  readonly scope: string | null;
  readonly parent?: Record<string, unknown>;
  readonly index: number;
  readonly ordinal: number;
  readonly count: number;
  readonly faker: ResolveNode["faker"];
}

export interface SeedFinalizeContext extends SeedWorld {
  readonly target: string;
  readonly scope: string | null;
  readonly parent?: Record<string, unknown>;
  readonly index: number;
  readonly ordinal: number;
  readonly count: number;
  readonly doc: Record<string, unknown>;
}

export type SeedFinalize = (
  context: SeedFinalizeContext,
) => Record<string, unknown> | void;

export interface SeedAfterContext extends SeedWorld {
  readonly state: DatabaseState;
}

export type SeedAfter = (context: SeedAfterContext) => void;

export type SeedRule = (context: SeedRuleContext) => unknown;

export type SeedRules = Record<string, Record<string, SeedRule>>;

export interface SeedInvariantContext {
  readonly state: DatabaseState;
  readonly schemas: SchemasDefinition;
  docs(target: string): readonly Record<string, unknown>[];
}

export type SeedInvariant = (context: SeedInvariantContext) => string[];

export interface SeedScenario {
  readonly name: string;
  readonly birth: string;
  readonly seed?: number;
  readonly refDate?: Date;
  readonly anchors?: SeedAnchors;
  readonly shape?: SeedShape;
  readonly rules?: SeedRules;
  readonly finalize?: Record<string, SeedFinalize>;
  readonly after?: SeedAfter;
  readonly invariants?: readonly SeedInvariant[];
  readonly uncorrelatedSpaces?: readonly string[];
}

export type ScenarioViolationKind =
  | "generation"
  | "correlation"
  | "duplicate_id"
  | "dangling_reference"
  | "owner_unresolved"
  | "invalid_document"
  | "invariant";

export interface ScenarioViolation {
  readonly kind: ScenarioViolationKind;
  readonly target: string;
  readonly message: string;
  readonly count?: number;
}

export interface ScenarioReport {
  readonly scenario: string;
  readonly birth: string;
  readonly at: string;
  readonly applied: readonly string[];
  readonly generated: Readonly<Record<string, number>>;
  readonly violations: readonly ScenarioViolation[];
  readonly ok: boolean;
}

export interface ScenarioRunResult {
  readonly state: DatabaseState;
  readonly report: ScenarioReport;
}
