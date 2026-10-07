import { test } from "../+harness.ts";
import { assert, assertEquals, assertNotEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { unique, withIndex } from "../../src/indexes.ts";
import { dbId, refId } from "../../src/ids.ts";
import { defineType } from "../../src/type-definition.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  mirrorOf,
  notPersonal,
  personal,
  personId,
  remapId,
  type PrivacyPosture,
} from "../../src/privacy/mod.ts";

const USER_ID = "user:01j5zk3v8n2q4x6y8z0b1c3d5e";
const ORG_ID = "org:01j5zk3v8n2q4x6y8z0b1c3d5f";
const USERS = "collections/+users/";
const COPIES = "collections/+copies/";

function transformerFor(
  schemas: unknown,
  posture: PrivacyPosture = "personal",
) {
  const plan = buildPrivacyPlan({ schemas: schemas as never, posture });
  return {
    plan,
    transformer: createPrivacyTransformer({
      plan,
      schemas: schemas as never,
      secret: "s3cret",
      timeShiftMs: 0,
    }),
  };
}

function mirrorSchemas(source: unknown, copy: unknown) {
  return {
    collections: {
      "+users": { _id: personId("user"), source },
      "+copies": {
        _id: personal(dbId("copy"), { of: "user" }),
        userId: refId("user"),
        copy,
      },
    },
  };
}

function mirrorPair(
  schemas: unknown,
  value: unknown,
  posture: PrivacyPosture = "personal",
) {
  const { transformer } = transformerFor(schemas, posture);
  const source = transformer.transform(USERS, { _id: USER_ID, source: value });
  const copy = transformer.transform(COPIES, {
    _id: "copy:01j5zk3v8n2q4x6y8z0b1c3d5e",
    userId: USER_ID,
    copy: value,
  });
  return { source: source.doc.source, copy: copy.doc.copy };
}

test({
  // TODO(privacy): R1, mirror must replay the source's treatment (keep/remap/generalise), not always produce a pseudonym
  ignore: true,
  name: "R1 mirror: a copy of a kept (contact, personal posture) source keeps the same value as its source",
  fn: () => {
    const schemas = mirrorSchemas(
      personal(v.string(), { role: "contact" }),
      mirrorOf(v.string(), "user.source"),
    );
    const { source, copy } = mirrorPair(schemas, "+33 6 12 34 56 78");
    assertEquals(copy, source);
  },
});

test({
  // TODO(privacy): R1, mirror must replay the source's treatment (keep/remap/generalise), not always produce a pseudonym
  ignore: true,
  name: "R1 mirror: a copy of a notPersonal source keeps the source value",
  fn: () => {
    const schemas = mirrorSchemas(
      notPersonal(v.string(), "public label"),
      mirrorOf(v.string(), "user.source"),
    );
    const { source, copy } = mirrorPair(schemas, "Salon du Livre", "strict");
    assertEquals(source, "Salon du Livre");
    assertEquals(copy, source);
  },
});

test({
  // TODO(privacy): R1, mirror must replay the source's treatment (keep/remap/generalise), not always produce a pseudonym
  ignore: true,
  name: "R1 mirror: a copy of a reference source joins on the remapped id",
  fn: () => {
    const schemas = mirrorSchemas(
      refId("org"),
      mirrorOf(v.string(), "user.source"),
    );
    const { source, copy } = mirrorPair(schemas, ORG_ID, "strict");
    assertEquals(source, remapId("s3cret", ORG_ID, 0));
    assertEquals(copy, source);
  },
});

test({
  // TODO(privacy): R2, canonical() folds case/space before hashing; a case-sensitive unique index needs the raw value in the message (or a collision retry per distinct raw value)
  ignore: true,
  name: "R2 unique: values differing only by case stay distinct on a case-sensitive unique pseudonym field",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          login: withIndex(personal(v.string(), { role: "direct" }), {
            unique: true,
          }),
        },
      },
    };
    const { transformer } = transformerFor(schemas);
    const a = transformer.transform(USERS, { _id: USER_ID, login: "Bob" });
    const b = transformer.transform(USERS, {
      _id: "user:01j5zk3v8n2q4x6y8z0b1c3d60",
      login: "bob",
    });
    assertNotEquals(a.doc.login, b.doc.login);
  },
});

test({
  // TODO(privacy): R3, transform isUnique() only reads field-level withIndex; it must also read indexesOf(source) unique composites like plan.ts does
  ignore: true,
  name: "R3 unique: a field covered by a defineType unique composite is made distinct or reported",
  fn: () => {
    const CODE = personal(v.pipe(v.string(), v.regex(/^[ab]$/)), {
      role: "direct",
    });
    const schemas = {
      collections: {
        "+users": defineType({
          schema: v.object({ _id: personId("user"), code: CODE }),
          indexes: (f) => [unique(f.code)],
        }),
      },
    };
    const { transformer } = transformerFor(schemas);
    const outputs = ["x", "y", "z"].map((code, i) =>
      transformer.transform(USERS, {
        _id: `user:01j5zk3v8n2q4x6y8z0b1c3d6${i}`,
        code,
      }),
    );
    const collided = outputs.some((o) =>
      o.notes.some((n) => n.kind === "collision"),
    );
    const distinct = new Set(outputs.map((o) => o.doc.code)).size === 3;
    assert(collided || distinct, "duplicates on a unique key, unreported");
  },
});

test({
  // TODO(privacy): R4, a unique index alone on a technical type (picklist/literal/number/date/boolean) must not promote the path to direct+pseudonym
  ignore: true,
  name: "R4 strict: a unique picklist discriminator keeps its value",
  fn: () => {
    const schemas = {
      collections: {
        "+security": {
          _id: notPersonal(dbId("security"), "platform keys"),
          type: withIndex(v.picklist(["badge", "kek", "dek_secret"]), {
            unique: true,
          }),
        },
        "+users": { _id: personId("user") },
      },
    };
    const { plan, transformer } = transformerFor(schemas, "strict");
    const path = plan.targets
      .get("collections/+security/")
      ?.paths.find((p) => p.path === "type");
    const out = transformer.transform("collections/+security/", {
      _id: "security:01j5zk3v8n2q4x6y8z0b1c3d5e",
      type: "badge",
    });
    assertEquals(path?.treatment.extract, "keep");
    assertEquals(out.doc.type, "badge");
  },
});

test({
  // TODO(privacy): R5, transform scopes mirrors only for scopedMultiCollections while extract passes the instance name as scope for multiModels; treat multiModels as scoped in findSource
  ignore: true,
  name: "R5 mirror: a copy inside a multi-model instance equals its source in the same instance",
  fn: () => {
    const schemas = {
      multiModels: {
        expo: {
          participant: {
            _id: personId("participant"),
            email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
          },
          badge: {
            _id: personal(dbId("badge"), { of: "participant" }),
            participantId: refId("participant"),
            emailCopy: mirrorOf(v.string(), "participant.email"),
          },
        },
      },
    };
    const { transformer } = transformerFor(schemas);
    const scope = "expo_01j5zk3v8n2q4x6y8z0b1c3d5e";
    const source = transformer.transform(
      "multiModels/expo/participant",
      {
        _id: "participant:01j5zk3v8n2q4x6y8z0b1c3d5e",
        _type: "participant",
        email: "alice@corp.fr",
      },
      { scope },
    );
    const copy = transformer.transform(
      "multiModels/expo/badge",
      {
        _id: "badge:01j5zk3v8n2q4x6y8z0b1c3d5e",
        _type: "badge",
        participantId: "participant:01j5zk3v8n2q4x6y8z0b1c3d5e",
        emailCopy: "alice@corp.fr",
      },
      { scope },
    );
    assertEquals(copy.doc.emailCopy, source.doc.email);
  },
});

test({
  // TODO(privacy): R6, plan.ts warns record keys are "copied verbatim" but transform mapKey fakes every non-vocabulary key regardless of posture/classification
  ignore: true,
  name: "R6 record keys: a technical record in personal posture keeps its keys",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          counters: v.record(v.string(), v.number()),
        },
      },
    };
    const { transformer } = transformerFor(schemas);
    const out = transformer.transform(USERS, {
      _id: USER_ID,
      counters: { fr: 1, en: 2 },
    });
    assertEquals(out.doc.counters, { fr: 1, en: 2 });
  },
});

test({
  // TODO(privacy): R7, walk picks the first union option that safeParses; v.object strips unknown keys so a wider later option is never chosen and its fields are dropped
  ignore: true,
  name: "R7 walk: a union of objects keeps the fields of the option that actually matches",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          address: v.union([
            v.object({ city: notPersonal(v.string(), "x") }),
            v.object({
              city: notPersonal(v.string(), "x"),
              zip: notPersonal(v.string(), "x"),
            }),
          ]),
        },
      },
    };
    const { transformer } = transformerFor(schemas);
    const out = transformer.transform(USERS, {
      _id: USER_ID,
      address: { city: "Lyon", zip: "69001" },
    });
    assertEquals(out.doc.address, { city: "Lyon", zip: "69001" });
  },
});

test({
  // TODO(privacy): R10, fakeMessage keys on leaf.path ("notes.*"), so every item of an array gets the same fake; key on leaf.keys instead
  ignore: true,
  name: "R10 fake: items of a faked array stay distinct",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          notes: v.array(personal(v.string(), { role: "content" })),
        },
      },
    };
    const { transformer } = transformerFor(schemas);
    const out = transformer.transform(USERS, {
      _id: USER_ID,
      notes: ["called on monday", "sent the quote", "signed"],
    });
    const notes = out.doc.notes as string[];
    assertEquals(new Set(notes).size, 3);
  },
});

test({
  // TODO(privacy): R11, identifier() remaps every string _id; an _id the schema pins (literal/picklist, singleton docs) must be kept
  ignore: true,
  name: "R11 ids: a literal singleton _id is kept and the document stays valid",
  fn: () => {
    const schemas = {
      collections: {
        "+users": { _id: personId("user") },
        settings: {
          _id: notPersonal(v.literal("global"), "singleton"),
          maintenance: v.boolean(),
        },
      },
    };
    const { transformer } = transformerFor(schemas);
    const out = transformer.transform("collections/settings/", {
      _id: "global",
      maintenance: false,
    });
    assertEquals(out.doc._id, "global");
    assertEquals(
      out.notes.filter((n) => n.kind === "invalid"),
      [],
    );
  },
});

test({
  // TODO(privacy): R13, transformState's remapInstanceName cannot see the transformer's effective shift; expose it (transformer.remapId) and use it in transformState
  ignore: true,
  name: "R13 api: under the strict default shift, a multi-model instance name and the references to it remap alike",
  fn: async () => {
    const { transformState } = await import(
      "../../src/migration/cli/commands/extract.ts"
    );
    const { createEmptyDatabaseState } = await import(
      "../../src/migration/types.ts"
    );
    const EXPO = "exposition:01j5zk3v8n2q4x6y8z0b1c3d5f";
    const schemas = {
      collections: {
        "+users": { _id: personId("user"), expositionId: refId("exposition") },
      },
      multiModels: {
        exposition: { zone: { _id: dbId("zone"), label: v.string() } },
      },
    };
    const plan = buildPrivacyPlan({
      schemas: schemas as never,
      posture: "strict",
    });
    const transformer = createPrivacyTransformer({
      plan,
      schemas: schemas as never,
      secret: "s3cret",
    });
    const state = createEmptyDatabaseState();
    state.collections["+users"] = {
      content: [{ _id: USER_ID, expositionId: EXPO }],
    };
    state.multiModels[EXPO] = { modelType: "exposition", content: [] };
    const out = transformState(state, plan, transformer, {
      schemas: schemas as never,
      remapInstanceName: (name) => remapId("s3cret", name),
    });
    const ref = out.state.collections["+users"].content[0].expositionId;
    assertEquals(Object.keys(out.state.multiModels), [ref]);
  },
});
