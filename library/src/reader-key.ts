import { BSON } from "mongodb";
import type { Db } from "./mongodb.ts";
import { ReaderArgumentError } from "./reader-errors.ts";

export function encodeKey(value: unknown, reader: string): string {
  if (value === undefined) return "u";
  if (value === null) return "n";
  switch (typeof value) {
    case "string":
      return `s${JSON.stringify(value)}`;
    case "number":
      return `d${Object.is(value, -0) ? "-0" : String(value)}`;
    case "boolean":
      return value ? "T" : "F";
    case "bigint":
      return `i${value}`;
    case "object": {
      if (value instanceof Date) return `t${value.getTime()}`;
      if ("_bsontype" in value)
        return `x${BSON.EJSON.stringify({ value }, { relaxed: false })}`;
      if (Array.isArray(value))
        return `[${value.map((item) => encodeKey(item, reader)).join(",")}]`;
      const prototype = Object.getPrototypeOf(value);
      if (prototype === Object.prototype || prototype === null) {
        const record = value as Record<string, unknown>;
        return `{${Object.keys(record)
          .sort()
          .map(
            (key) => `${JSON.stringify(key)}:${encodeKey(record[key], reader)}`,
          )
          .join(",")}}`;
      }
      break;
    }
  }
  throw new ReaderArgumentError(
    `reader "${reader}" cannot key an argument of type ${typeof value === "object" ? (value.constructor?.name ?? "object") : typeof value}; pass strings, numbers, booleans, bigints, null, undefined, dates, BSON values, arrays or plain objects`,
  );
}

export function cacheKey(
  db: Db,
  identity: number,
  reader: string,
  args: readonly unknown[],
): string {
  return `${db.databaseName}\u0000${identity}\u0000${encodeKey(args, reader)}`;
}
