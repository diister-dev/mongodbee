import { test } from "./+harness.ts";
import { assertEquals } from "./+assert.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { index } from "../src/index-builder.ts";
import { defineType } from "../src/type-definition.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { from, scoped } from "../src/computed.ts";
import { reader } from "../src/readers.ts";

const Participant = defineType({
  schema: v.object({
    status: v.picklist(["active", "pending"]),
    personRef: v.variant("kind", [
      v.object({ kind: v.literal("user"), userId: v.string() }),
      v.object({ kind: v.literal("accountless"), identityId: v.string() }),
    ]),
    secret: v.string(),
  }),
  indexes: (f) => [index(f.personRef.userId)],
});

const Model = defineModel("typed", { schema: { participant: Participant } });
const Typed = scoped(Model, refId("typed"));

const participations = reader(
  "typed-participations",
  from(Typed, "participant")
    .by((p) => p.personRef.userId)
    .where((p) => [p.status, ["active", "pending"]])
    .select(["status"]),
);

const single = reader(
  "typed-single",
  from(Typed, "participant").one().select(["status"]),
);

export async function typeChecks(): Promise<void> {
  const rows = await participations("typed:a", "user:a");
  const status: "active" | "pending" | undefined = rows[0]?.status;
  // @ts-expect-error secret is not selected, so it is not in the value
  rows[0]?.secret;
  // @ts-expect-error a reader value is readonly
  rows[0]!.status = "active";
  // @ts-expect-error the key comes after the scope
  await participations("typed:a");
  const one = await single("typed:a");
  const maybe: { readonly status: "active" | "pending" } | null = one;
  const many = await participations.many("typed:a", ["user:a", "user:b"]);
  const first: "active" | "pending" | undefined =
    many.get("user:a")?.[0]?.status;
  // @ts-expect-error a reader without by() has no keys to read many of
  await single.many("typed:a", []);
  const primed: number = await single.primeFrom(async () => 1);
  // @ts-expect-error only a singleton reader can be primed
  await participations.primeFrom(async () => 1);

  from(Typed, "participant")
    // @ts-expect-error a where value must be a value of its field
    .where((p) => [p.status, "actve"]);

  const hop = from(Typed, "participant").through(
    "participant",
    Participant,
    (p) => p.status,
  );
  // @ts-expect-error a reader reads one source, not through another
  hop.select(["status"]);

  const composite = reader("typed-composite", async (key: string) => ({
    key,
    at: new Date(0),
    list: [1, 2],
    byKey: new Map([["a", 1]]),
  }));
  const value = await composite("k");
  // @ts-expect-error a composite value is deeply readonly
  value.list.push(3);
  // @ts-expect-error a date in a reader value is frozen
  value.at.setTime(0);
  const time: number = value.at.getTime();
  // @ts-expect-error a composite value holds no function
  reader("typed-function", async () => ({ run: () => 1 }));
  // @ts-expect-error a composite argument must be keyable plain data
  reader("typed-set-argument", async (ids: Set<string>) => ids.size);

  const opaque = reader("typed-opaque", async () => ({
    extra: {} as Record<string, unknown>,
  }));
  const extra: Readonly<Record<string, unknown>> = (await opaque()).extra;
  // @ts-expect-error an unknown value stays unknown, so it may be nullish
  const payload: NonNullable<unknown> = extra.anything;

  void [status, maybe, first, time, primed, payload];
}

test("readers types: the declarations above typecheck", () => {
  assertEquals(typeof typeChecks, "function");
});
