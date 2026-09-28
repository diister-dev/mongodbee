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
  exists: "is set",
  missing: "is empty",
};

const PRESENCE: QueryOperator[] = ["exists", "missing"];

export function operatorsFor(family: FieldFamily): QueryOperator[] {
  switch (family) {
    case "text":
      return ["contains", "eq", "ne", "starts", ...PRESENCE];
    case "number":
    case "date":
      return ["eq", "ne", "gt", "gte", "lt", "lte", ...PRESENCE];
    case "boolean":
    case "choice":
    case "reference":
    case "identity":
      return ["eq", "ne", ...PRESENCE];
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

export function describeCondition(condition: QueryCondition): string {
  const head = `${condition.field} ${OPERATOR_LABEL[condition.op]}`;
  return needsValue(condition.op) ? `${head} ${condition.value}` : head;
}
