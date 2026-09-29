import type { FieldFamily } from "./values.ts";

export type QueryOperator =
  | "eq"
  | "ne"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "contains"
  | "starts"
  | "in"
  | "exists"
  | "missing";

export interface QueryCondition {
  id: number;
  field: string;
  op: QueryOperator;
  value: string;
}

export const OPERATOR_LABEL: Record<QueryOperator, string> = {
  eq: "is",
  ne: "is not",
  gt: "above",
  gte: "at least",
  lt: "below",
  lte: "at most",
  contains: "contains",
  starts: "starts with",
  in: "is one of",
  exists: "is set",
  missing: "is empty",
};

const PRESENCE: QueryOperator[] = ["exists", "missing"];

export function operatorsFor(family: FieldFamily): QueryOperator[] {
  switch (family) {
    case "text":
      return ["contains", "eq", "ne", "in", "starts", ...PRESENCE];
    case "number":
      return ["eq", "ne", "in", "gt", "gte", "lt", "lte", ...PRESENCE];
    case "date":
      return ["eq", "ne", "gt", "gte", "lt", "lte", ...PRESENCE];
    case "boolean":
    case "choice":
    case "reference":
    case "identity":
      return ["eq", "ne", "in", ...PRESENCE];
    case "list":
    case "object":
      return ["eq", ...PRESENCE];
    default:
      return ["eq", "ne", "contains", "gt", "lt", ...PRESENCE];
  }
}

export function defaultOperator(family: FieldFamily): QueryOperator {
  return operatorsFor(family)[0];
}

export function needsValue(op: QueryOperator): boolean {
  return !PRESENCE.includes(op);
}

export function isComplete(condition: QueryCondition): boolean {
  return (
    condition.field !== "" &&
    (!needsValue(condition.op) || condition.value !== "")
  );
}

export function conditionParam(condition: QueryCondition): string {
  return `${condition.field}:${condition.op}:${needsValue(condition.op) ? condition.value : ""}`;
}

export function conditionParams(
  conditions: readonly QueryCondition[],
): string[] {
  return conditions.filter(isComplete).map(conditionParam);
}

const OPERATORS = Object.keys(OPERATOR_LABEL);

function isOperator(value: string): value is QueryOperator {
  return OPERATORS.includes(value);
}

export function parseConditionParam(
  raw: string,
  id: number,
): QueryCondition | undefined {
  const first = raw.indexOf(":");
  const second = first < 0 ? -1 : raw.indexOf(":", first + 1);
  if (first <= 0 || second < 0) return undefined;
  const op = raw.slice(first + 1, second);
  if (!isOperator(op)) return undefined;
  return { id, field: raw.slice(0, first), op, value: raw.slice(second + 1) };
}

export function describeCondition(condition: QueryCondition): string {
  const head = `${condition.field} ${OPERATOR_LABEL[condition.op]}`;
  return needsValue(condition.op) ? `${head} ${condition.value}` : head;
}
