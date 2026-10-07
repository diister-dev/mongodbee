const frozenValues = new WeakSet<object>();

const DATE_SETTERS = Object.getOwnPropertyNames(Date.prototype).filter((name) =>
  name.startsWith("set"),
);

function refuseMutation(): never {
  throw new TypeError("a reader value is frozen");
}

function lockMethods(target: object, names: readonly string[]): void {
  for (const name of names)
    Object.defineProperty(target, name, { value: refuseMutation });
}

function seal<T extends object>(value: T): T {
  Object.freeze(value);
  frozenValues.add(value);
  return value;
}

function freezeValue(
  value: unknown,
  owned: boolean,
  seen: Map<object, unknown>,
): unknown {
  if (value === null || typeof value !== "object") return value;
  if (frozenValues.has(value)) return value;
  if ("_bsontype" in value || ArrayBuffer.isView(value)) return value;
  if (seen.has(value)) return seen.get(value);
  if (value instanceof Date) {
    const date = owned ? value : new Date(value.getTime());
    seen.set(value, date);
    lockMethods(date, DATE_SETTERS);
    return seal(date);
  }
  if (value instanceof Map) {
    const map: Map<unknown, unknown> = owned ? value : new Map();
    seen.set(value, map);
    const items = [...value];
    Map.prototype.clear.call(map);
    for (const [key, item] of items)
      Map.prototype.set.call(map, key, freezeValue(item, owned, seen));
    lockMethods(map, ["set", "delete", "clear"]);
    return seal(map);
  }
  if (value instanceof Set) {
    const set: Set<unknown> = owned ? value : new Set();
    seen.set(value, set);
    const items = [...value];
    Set.prototype.clear.call(set);
    for (const item of items)
      Set.prototype.add.call(set, freezeValue(item, owned, seen));
    lockMethods(set, ["add", "delete", "clear"]);
    return seal(set);
  }
  if (Array.isArray(value)) {
    const array: unknown[] = owned ? value : new Array(value.length);
    seen.set(value, array);
    for (let index = 0; index < value.length; index++)
      array[index] = freezeValue(value[index], owned, seen);
    return seal(array);
  }
  const source = value as Record<string, unknown>;
  const record: Record<string, unknown> = owned
    ? source
    : Object.create(Object.getPrototypeOf(value));
  seen.set(value, record);
  for (const key of Object.keys(source))
    record[key] = freezeValue(source[key], owned, seen);
  return seal(record);
}

export function freezeOwned<T>(value: T): T {
  return freezeValue(value, true, new Map()) as T;
}

export function freezeCopy<T>(value: T): T {
  return freezeValue(value, false, new Map()) as T;
}
