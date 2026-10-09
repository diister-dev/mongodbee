import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import { collection } from "../../../src/collection.ts";
import { classifyCommand } from "../../../src/migration/cli/commands/classify.ts";
import { extractCommand } from "../../../src/migration/cli/commands/extract.ts";
import { markMigrationAsAdopted } from "../../../src/migration/state.ts";
import { type Db, MongoClient } from "../../../src/mongodb.ts";
import {
  buildPrivacyPlan,
  defaultTimeShiftMs,
  fieldsOf,
  remapId,
} from "../../../src/privacy/mod.ts";
import * as v from "../../../src/schema.ts";
import { scopedMultiCollection } from "../../../src/scoped-multi-collection.ts";
import { assert, assertEquals, assertNotEquals } from "../../+assert.ts";
import { test } from "../../+harness.ts";
import { withTempDir } from "../../migration/cli/shared.ts";
import { buildWorld, SCHEMAS, SIZES } from "./fixture.ts";

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";
const FIXTURE = new URL("./fixture.ts", import.meta.url).href;
const SRC = new URL("../../../src/", import.meta.url).href;
const BIRTH = "2026_01_01_0900_DIIVLIKE1@birth";
const SECRET = "usecase-secret";
const SHIFT_DAYS = 30;

const tag = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
const DB = {
  source: `mongodbee_test_usecase_src_${tag}`,
  target: `mongodbee_test_usecase_dst_${tag}`,
  same: `mongodbee_test_usecase_same_${tag}`,
  other: `mongodbee_test_usecase_other_${tag}`,
  shifted: `mongodbee_test_usecase_shift_${tag}`,
};

type Doc = Record<string, unknown>;
type Dump = Map<string, Doc[]>;

const client = new MongoClient(TEST_URI);
let setup: Promise<void> | undefined;

async function writeProject(dir: string): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `import { resolveDynamic } from ${JSON.stringify(FIXTURE)};
export default { database: { connection: { uri: ${JSON.stringify(TEST_URI)} }, name: ${JSON.stringify(DB.source)} }, paths: { migrations: "./migrations", schemas: "./schemas.ts" }, privacy: { resolveDynamic } };`,
  );
  await writeFile(
    `${dir}/schemas.ts`,
    `export { SCHEMAS as schemas } from ${JSON.stringify(FIXTURE)};`,
  );
  await writeFile(
    `${dir}/migrations/${BIRTH}.ts`,
    `import { migrationDefinition } from "${SRC}migration/definition.ts";
import { SCHEMAS } from ${JSON.stringify(FIXTURE)};
export default migrationDefinition(${JSON.stringify(BIRTH)}, "birth", { parent: null, schemas: SCHEMAS, migrate: (b) => b.compile() });
`,
  );
}

async function seedSource(db: Db): Promise<void> {
  const world = buildWorld();
  await db.collection("+users").insertMany(world.users);
  await db.collection("+emails").insertMany(world.emails);
  await db.collection("+entreprises").insertMany(world.entreprises);
  await db.collection("+expositions").insertMany(world.expositions);
  await db.collection("+scans").insertMany(world.scans);
  await markMigrationAsAdopted(db, BIRTH, "birth");
}

function prepare(): Promise<void> {
  setup ??= (async () => {
    await client.connect();
    await withTempDir(async (dir) => {
      await writeProject(dir);
      await seedSource(client.db(DB.source));
      await classifyCommand({ cwd: dir, json: true, posture: "strict" });
      const extract = (toDb: string, secret: string, shiftDays?: number) =>
        extractCommand({
          cwd: dir,
          fromDb: DB.source,
          toDb,
          secret,
          json: true,
          ...(shiftDays !== undefined && { shiftDays }),
        });
      await extract(DB.target, SECRET);
      await extract(DB.same, SECRET);
      await extract(DB.other, `${SECRET}-other`);
      await extract(DB.shifted, SECRET, SHIFT_DAYS);
    });
  })();
  return setup;
}

async function dump(name: string): Promise<Dump> {
  const db = client.db(name);
  const out: Dump = new Map();
  for (const info of await db
    .listCollections({}, { nameOnly: true })
    .toArray()) {
    if (info.name.startsWith("__")) continue;
    out.set(
      info.name,
      (await db
        .collection(info.name)
        .find({})
        .sort({ _id: 1 })
        .toArray()) as Doc[],
    );
  }
  return out;
}

function* strings(value: unknown, path = ""): Generator<[string, string]> {
  if (typeof value === "string") yield [path, value];
  else if (Array.isArray(value)) {
    for (const item of value) yield* strings(item, `${path}.*`);
  } else if (
    value !== null &&
    typeof value === "object" &&
    !(value instanceof Date)
  ) {
    for (const [k, item] of Object.entries(value)) {
      yield* strings(item, path ? `${path}.${k}` : k);
    }
  }
}

const ID = /^[a-z_]+:[a-z0-9]+$/;
const all = (d: Dump) => [...d.values()].flat();
const ofType = (d: Dump, name: string, type: string) =>
  (d.get(name) ?? []).filter((doc) => doc._type === type);
const field = (p: Doc, key: string) =>
  (p.fields as Record<string, { v?: unknown }>)[key]?.v;

function joins(d: Dump): Map<string, number> {
  const ids = new Set(all(d).map((doc) => String(doc._id)));
  const out = new Map<string, number>();
  for (const [name, docs] of d) {
    for (const doc of docs) {
      for (const [path, s] of strings(doc)) {
        if (path === "_id" || !ID.test(s) || !ids.has(s)) continue;
        const key = `${name}/${doc._type ?? ""}:${path}`;
        out.set(key, (out.get(key) ?? 0) + 1);
      }
    }
  }
  return out;
}

function personMirrors(
  d: Dump,
  key: "email" | "firstname" | "lastname",
): number {
  const users = new Map((d.get("+users") ?? []).map((u) => [u._id, u]));
  const identities = new Map(
    ofType(d, "+expositions", "accountless_identity").map((a) => [a._id, a]),
  );
  return ofType(d, "+expositions", "participant").filter((p) => {
    const ref = p.personRef as {
      kind: string;
      userId?: string;
      accountlessIdentityId?: string;
    };
    const person =
      ref.kind === "user"
        ? users.get(ref.userId)
        : identities.get(ref.accountlessIdentityId);
    return person !== undefined && field(p, key) === person[key];
  }).length;
}

function personalValues(d: Dump): Set<string> {
  const out = new Set<string>();
  const add = (s: unknown) => {
    if (typeof s === "string" && s.length >= 4) out.add(s);
  };
  for (const u of d.get("+users") ?? []) {
    add(u.email);
    add(u.firstname);
    add(u.lastname);
    for (const [, s] of strings(u.notes)) if (!ID.test(s)) add(s);
  }
  for (const doc of d.get("+expositions") ?? []) {
    if (doc._type === "accountless_identity") {
      for (const key of ["email", "firstname", "lastname", "phone"])
        add(doc[key]);
    }
    if (doc._type === "participant") {
      for (const f of Object.values(
        doc.fields as Record<string, { t: string; v: unknown }>,
      )) {
        if (["text", "textarea", "email", "phone"].includes(f.t)) add(f.v);
      }
    }
    if (doc._type === "exhibitor_contact") {
      for (const c of doc.comments as { content: string }[]) add(c.content);
    }
    if (doc._type === "expo_organization") add(doc.displayName);
  }
  for (const m of d.get("+emails") ?? []) {
    add(m.to);
    for (const [, s] of strings(m.devSnapshot)) add(s);
  }
  for (const e of d.get("+entreprises") ?? []) {
    for (const [, s] of strings(e.identity)) add(s);
    for (const [, s] of strings(e.contact)) add(s);
  }
  return out;
}

const SLOW = { timeout: 600_000 };

test({
  name: "usecase: the realistic schemas classify without error under the strict posture",
  ...SLOW,
  fn: async () => {
    const plan = buildPrivacyPlan({ schemas: SCHEMAS, posture: "strict" });
    assertEquals(
      plan.findings.filter((f) => f.level !== "info"),
      [],
    );
    assertEquals([...plan.persons.keys()].sort(), [
      "accountless_identity",
      "participant",
      "user",
    ]);
    await prepare();
  },
});

test({
  name: "usecase: the extract keeps every collection, type, scope and document count",
  ...SLOW,
  fn: async () => {
    await prepare();
    const source = await dump(DB.source);
    const target = await dump(DB.target);
    const count = (d: Dump) =>
      Object.fromEntries(
        [...d].flatMap(([name, docs]) => {
          const byType = new Map<string, number>();
          for (const doc of docs) {
            const key = `${name}/${doc._type ?? ""}`;
            byType.set(key, (byType.get(key) ?? 0) + 1);
          }
          return [...byType];
        }),
      );
    assertEquals(count(target), count(source));
    const scopes = (d: Dump) =>
      new Set((d.get("+expositions") ?? []).map((x) => x._scope)).size;
    assertEquals(scopes(target), SIZES.expositions);
    assert(ofType(source, "+expositions", "participant").length >= 900);
    assertEquals((source.get("+scans") ?? []).length, SIZES.scans);
  },
});

test({
  name: "usecase: every reference that joins in the source joins in the target",
  ...SLOW,
  fn: async () => {
    await prepare();
    const source = joins(await dump(DB.source));
    const target = joins(await dump(DB.target));
    assertEquals(target, source);
    assert(
      (source.get("+scans/business_scan:participantId") ?? 0) === SIZES.scans,
    );
  },
});

test({
  name: "usecase: emails coincide across users, accountless identities, participant fields and the outbox",
  ...SLOW,
  fn: async () => {
    await prepare();
    const source = await dump(DB.source);
    const target = await dump(DB.target);
    assertEquals(
      personMirrors(target, "email"),
      personMirrors(source, "email"),
    );
    const users = new Set((target.get("+users") ?? []).map((u) => u.email));
    for (const mail of target.get("+emails") ?? [])
      assert(users.has(mail.to), String(mail.to));
    const repeated = (d: Dump) => {
      const byEmail = new Map<unknown, Set<unknown>>();
      for (const a of ofType(d, "+expositions", "accountless_identity")) {
        byEmail.set(a.email, (byEmail.get(a.email) ?? new Set()).add(a._scope));
      }
      return [...byEmail.values()].filter((s) => s.size > 1).length;
    };
    assert(repeated(source) > 0);
    assertEquals(repeated(target), repeated(source));
  },
});

test({
  name: "usecase: names coincide between a person and its participant fields",
  ...SLOW,
  fn: async () => {
    await prepare();
    const source = await dump(DB.source);
    const target = await dump(DB.target);
    for (const key of ["firstname", "lastname"] as const) {
      assertEquals(personMirrors(target, key), personMirrors(source, key));
    }
  },
});

test({
  name: "usecase: unique indexes hold and every document matches its collection validator",
  ...SLOW,
  fn: async () => {
    await prepare();
    const db = client.db(DB.target);
    const target = await dump(DB.target);
    const emails = (target.get("+users") ?? []).map((u) => String(u.email));
    assertEquals(new Set(emails).size, emails.length);
    const users = await db.collection("+users").indexes();
    assert(
      users.some((i) => i.unique && "email" in i.key),
      JSON.stringify(users),
    );
    for (const info of await db.listCollections().toArray()) {
      const validator = (info as { options?: { validator?: Doc } }).options
        ?.validator;
      if (validator === undefined) continue;
      assertEquals(
        await db.collection(info.name).countDocuments({ $nor: [validator] }),
        0,
        info.name,
      );
    }
    const plan = buildPrivacyPlan({ schemas: SCHEMAS, posture: "strict" });
    for (const t of plan.targets.values()) {
      const fields = fieldsOf(SCHEMAS, t);
      if (fields === undefined) continue;
      const docs =
        t.type === undefined || t.bucket === "collections"
          ? (target.get(t.collection) ?? [])
          : (target.get(t.collection) ?? []).filter((d) => d._type === t.type);
      for (const doc of docs) {
        const { _type: _t, _scope: _s, ...rest } = doc;
        const parsed = v.safeParse(v.object(fields as v.ObjectEntries), rest);
        assert(
          parsed.success,
          `${t.key} ${String(doc._id)}: ${parsed.issues?.[0]?.message}`,
        );
      }
    }
  },
});

test({
  name: "usecase: no personal value of the source survives anywhere in the target",
  ...SLOW,
  fn: async () => {
    await prepare();
    const source = await dump(DB.source);
    const target = await dump(DB.target);
    const personal = personalValues(source);
    assert(personal.size > 500, String(personal.size));
    const verbatim: string[] = [];
    const embedded: string[] = [];
    const needles = [...personal]
      .filter((s) => s.length >= 6)
      .map((s) => s.toLowerCase());
    for (const [name, docs] of target) {
      for (const doc of docs) {
        for (const [path, s] of strings(doc)) {
          const where = `${name}/${doc._type ?? ""}:${path}`;
          if (personal.has(s)) verbatim.push(`${where} = ${s}`);
          const lower = s.toLowerCase();
          const hit = needles.find((n) => n !== lower && lower.includes(n));
          if (hit !== undefined) embedded.push(`${where} contains ${hit}`);
        }
      }
    }
    assertEquals(verbatim.slice(0, 10), []);
    assertEquals(embedded.slice(0, 10), []);
  },
});

test({
  name: "usecase: the same secret gives the same extract, another secret another one",
  ...SLOW,
  fn: async () => {
    await prepare();
    const target = await dump(DB.target);
    assertEquals(await dump(DB.same), target);
    const other = await dump(DB.other);
    const emails = (d: Dump) => (d.get("+users") ?? []).map((u) => u.email);
    const shared = emails(other).filter((e) => new Set(emails(target)).has(e));
    assertEquals(shared, []);
    assertNotEquals(
      (other.get("+users") ?? []).map((u) => u._id),
      (target.get("+users") ?? []).map((u) => u._id),
    );
  },
});

test({
  name: "usecase: --shift-days moves dates and id timestamps together and keeps the joins",
  ...SLOW,
  fn: async () => {
    await prepare();
    const source = await dump(DB.source);
    const shifted = await dump(DB.shifted);
    assertEquals(joins(shifted), joins(source));
    const shiftMs = SHIFT_DAYS * 86_400_000;
    const byId = new Map(all(shifted).map((d) => [String(d._id), d]));
    for (const scan of source.get("+scans") ?? []) {
      const out = byId.get(remapId(SECRET, String(scan._id), shiftMs));
      assert(out !== undefined, String(scan._id));
      assertEquals(
        (out.at as Date).getTime() - (scan.at as Date).getTime(),
        shiftMs,
      );
    }
    const target = await dump(DB.target);
    const defaultShift = defaultTimeShiftMs(SECRET);
    const user = (source.get("+users") ?? [])[0];
    assert(
      (target.get("+users") ?? []).some(
        (u) => u._id === remapId(SECRET, String(user._id), defaultShift),
      ),
      "without --shift-days the strict posture applies its default shift",
    );
  },
});

test({
  name: "usecase: mongodbee opens the target with its collection APIs and queries by reference",
  ...SLOW,
  fn: async () => {
    await prepare();
    const db = client.db(DB.target);
    const users = await collection(db, "+users", SCHEMAS.collections["+users"]);
    const expositions = await scopedMultiCollection(
      db,
      "+expositions",
      SCHEMAS.scopedMultiCollections["+expositions"],
    );
    const scans = await scopedMultiCollection(
      db,
      "+scans",
      SCHEMAS.scopedMultiCollections["+scans"],
    );
    const scopes = await db
      .collection("+expositions")
      .distinct("_scope", { _type: "information" });
    let joined = 0;
    let paged = 0;
    for (const scope of scopes.map(String)) {
      const view = expositions.scope(scope);
      const participants = await view.find("participant", {
        "personRef.kind": "user",
      });
      for (const p of participants.slice(0, 20)) {
        const ref = p.personRef as { userId: string };
        assert(await users.findOne({ _id: ref.userId }), ref.userId);
        const back = await view.find("participant", {
          "personRef.userId": ref.userId,
        });
        assert(back.some((x) => x._id === p._id));
        joined++;
      }
      let page = await scans
        .scope(scope)
        .paginate("business_scan", undefined, { limit: 50 });
      const total = page.total ?? 0;
      for (;;) {
        for (const scan of page.data) {
          assert(
            await view.findOne("participant", { _id: scan.participantId }),
            scan.participantId,
          );
          paged++;
        }
        const last = page.data[page.data.length - 1];
        if (page.data.length < 50 || last === undefined) break;
        page = await scans.scope(scope).paginate("business_scan", undefined, {
          limit: 50,
          afterId: last._id,
        });
      }
      assert(total > 0);
    }
    assert(joined > 0);
    assertEquals(paged, SIZES.scans);
  },
});

test({
  name: "usecase: cleanup drops the source and every extract",
  ...SLOW,
  fn: async () => {
    try {
      await prepare();
    } finally {
      for (const name of Object.values(DB))
        await client.db(name).dropDatabase();
      await client.close();
    }
  },
});
