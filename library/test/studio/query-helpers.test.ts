import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import {
  conditionParams,
  defaultOperator,
  describeCondition,
  isComplete,
  needsValue,
  operatorsFor,
} from "../../src/studio/ui/lib/query.ts";

test("operators fit the field family, text defaults to contains", () => {
  assertEquals(defaultOperator("text"), "contains");
  assertEquals(defaultOperator("number"), "eq");
  assertEquals(operatorsFor("number").includes("gte"), true);
  assertEquals(operatorsFor("boolean").includes("gt"), false);
  assertEquals(operatorsFor("choice"), ["eq", "ne", "exists", "missing"]);
});

test("conditions serialise only when complete", () => {
  const conditions = [
    { id: 1, field: "age", op: "gte" as const, value: "18" },
    { id: 2, field: "name", op: "contains" as const, value: "" },
    { id: 3, field: "address", op: "missing" as const, value: "ignored" },
    { id: 4, field: "", op: "eq" as const, value: "x" },
  ];
  assertEquals(conditions.map(isComplete), [true, false, true, false]);
  assertEquals(conditionParams(conditions), ["age:gte:18", "address:missing:"]);
  assertEquals(needsValue("exists"), false);
  assertEquals(describeCondition(conditions[0]), "age at least 18");
  assertEquals(describeCondition(conditions[2]), "address is empty");
});
