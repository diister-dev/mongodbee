import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import { checkComputed } from "../src/computed-apply.ts";
import { drainComputedPending } from "../src/computed-marks.ts";
import { getSessionContext } from "../src/session.ts";

const SCOPES = ["exposition:expoaaaaa01", "exposition:expobbbbb02"] as const;

const Organization = defineType({
  schema: v.object({
    name: v.string(),
    status: v.picklist(["pending", "validated"]),
  }),
});

const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
    organizationId: withIndex(refId("expo_organization")),
    status: v.picklist(["active", "removed"]),
    note: v.optional(v.string()),
  }),
});

const Scan = defineType({
  schema: v.object({
    scannedIds: withIndex(v.array(refId("participant"))),
    kind: v.picklist(["security", "business", "vip"]),
  }),
});

const ScansModel = defineModel("scans", { schema: { scan: Scan } });

const Participant = defineType({
  schema: v.object({
    name: v.string(),
    userId: withIndex(v.string(), { global: true }),
  }),
  computed: {
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId),
    membershipCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .count(),
    scanKinds: from(ScansModel, "scan")
      .by((s) => s.scannedIds)
      .sameScope()
      .collect((s) => s.kind)
      .distinct()
      .maxEntries(3),
    scanCount: from(ScansModel, "scan")
      .by((s) => s.scannedIds)
      .sameScope()
      .count(),
    validatedOrganizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .through("expo_organization", Organization, (m) => m.organizationId)
      .where((o) => [o.status, "validated"])
      .collect((o) => o._id),
    validatedOrganizationNames: from("org_membership", Membership)
      .by((m) => m.participantId)
      .through("expo_organization", Organization, (m) => m.organizationId)
      .where((o) => [o.status, "validated"])
      .collect((o) => o.name)
      .distinct(),
  },
});

const AccountTag = defineType({
  schema: v.object({
    accountId: withIndex(v.string()),
    tag: v.string(),
    weight: v.number(),
  }),
});

const Account = defineType({
  schema: v.object({ email: v.string() }),
  computed: {
    participationCount: from("participant", Participant)
      .by((p) => p.userId)
      .count(),
    tags: from("account_tags", AccountTag)
      .by((t) => t.accountId)
      .collect((t) => t.tag)
      .distinct()
      .maxEntries(50),
    tagCount: from("account_tags", AccountTag)
      .by((t) => t.accountId)
      .count(),
  },
});

const LeadComment = defineType({
  schema: v.object({ leadId: withIndex(refId("lead")), body: v.string() }),
});
const LeadWithCount = defineType({
  schema: v.object({ title: v.string() }),
  computed: {
    commentCount: from("lead_comment", LeadComment)
      .by((c) => c.leadId)
      .count(),
  },
});
const CrmModel = defineModel("crm", {
  schema: { lead: LeadWithCount, lead_comment: LeadComment },
});

const schemas = {
  collections: { accounts: Account, account_tags: AccountTag },
  multiCollections: { crm: CrmModel.schema },
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: {
        participant: Participant,
        org_membership: Membership,
        expo_organization: Organization,
      },
    },
    "+scans": { scope: refId("exposition"), types: ScansModel.schema },
  },
};

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

async function openAll(db: Db, inlineLimit?: number) {
  registerComputed(db, computedTopology(schemas), { inlineLimit });
  const accounts = await collection(db, "accounts", Account);
  const tags = await collection(db, "account_tags", AccountTag);
  const crm = await multiCollection(db, "crm", CrmModel);
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+expositions"].types,
  });
  const scans = await scopedMultiCollection(db, "+scans", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: ScansModel.schema,
  });
  return { accounts, tags, crm, expositions, scans };
}

type World = Awaited<ReturnType<typeof openAll>>;

async function ids(
  db: Db,
  collectionName: string,
  filter: Record<string, unknown> = {},
): Promise<Array<{ _id: unknown; _scope?: string }>> {
  return (await db
    .collection(collectionName)
    .find(filter, { projection: { _id: 1, _scope: 1 } })
    .sort({ _id: 1 })
    .toArray()) as never;
}

type Step = { name: string; run: () => Promise<void> };

function steps(db: Db, world: World, random: () => number): Step[] {
  const pick = <T>(items: readonly T[]): T | undefined =>
    items.length === 0 ? undefined : items[Math.floor(random() * items.length)];
  const scopeOf = () => pick(SCOPES)!;
  const participantsIn = async (scope: string) =>
    (
      await ids(db, "+expositions", { _type: "participant", _scope: scope })
    ).map((p) => String(p._id));
  const accountIds = async () =>
    (await ids(db, "accounts")).map((a) => String(a._id));
  const someParticipants = async (scope: string) => {
    const all = await participantsIn(scope);
    return all.filter(() => random() < 0.4);
  };

  return [
    {
      name: "account insertOne",
      run: async () =>
        void (await world.accounts.insertOne({
          email: `a${Math.floor(random() * 1e6)}@x.test`,
        })),
    },
    {
      name: "account replaceOne",
      run: async () => {
        const id = pick(await ids(db, "accounts"));
        if (id)
          await world.accounts.collection.replaceOne(
            { _id: id._id } as never,
            { email: "replaced@x.test" } as never,
          );
      },
    },
    {
      name: "account deleteOne",
      run: async () => {
        const id = pick(await ids(db, "accounts"));
        if (id && random() < 0.3)
          await world.accounts.deleteOne({ _id: id._id } as never);
      },
    },
    {
      name: "tag insertOne",
      run: async () => {
        const account = pick(await accountIds());
        if (account)
          await world.tags.insertOne({
            accountId: account,
            tag: pick(["a", "b", "c", "d"])!,
            weight: 1,
          });
      },
    },
    {
      name: "tag insertMany",
      run: async () => {
        const accounts = await accountIds();
        if (accounts.length)
          await world.tags.insertMany(
            [0, 1, 2].map(() => ({
              accountId: pick(accounts)!,
              tag: pick(["a", "b", "e"])!,
              weight: 2,
            })),
          );
      },
    },
    {
      name: "tag updateOne moves to another account",
      run: async () => {
        const tag = pick(await ids(db, "account_tags"));
        const account = pick(await accountIds());
        if (tag && account)
          await world.tags.updateOne({ _id: tag._id } as never, {
            $set: { accountId: account },
          });
      },
    },
    {
      name: "tag updateMany unrelated field",
      run: async () =>
        void (await world.tags.updateMany(
          { tag: "a" },
          { $inc: { weight: 1 } },
        )),
    },
    {
      name: "tag updateMany relabel",
      run: async () =>
        void (await world.tags.updateMany(
          { tag: pick(["a", "b"])! },
          { $set: { tag: pick(["c", "z"])! } },
        )),
    },
    {
      name: "tag findOneAndUpdate",
      run: async () => {
        const account = pick(await accountIds());
        if (account)
          await world.tags.findOneAndUpdate(
            { tag: pick(["a", "b", "c", "e", "z"])! },
            { $set: { accountId: account } },
          );
      },
    },
    {
      name: "tag findOneAndReplace",
      run: async () => {
        const account = pick(await accountIds());
        if (account)
          await world.tags.findOneAndReplace(
            { tag: pick(["a", "b", "c"])! },
            { accountId: account, tag: "r", weight: 0 },
          );
      },
    },
    {
      name: "tag findOneAndDelete",
      run: async () =>
        void (await world.tags.findOneAndDelete({
          tag: pick(["a", "b", "c", "e", "r", "z"])!,
        })),
    },
    {
      name: "tag deleteMany",
      run: async () =>
        void (
          random() < 0.3 &&
          (await world.tags.deleteMany({ tag: pick(["e", "z"])! }))
        ),
    },
    {
      name: "tag upsert through updateOne",
      run: async () => {
        const account = pick(await accountIds());
        if (account)
          await world.tags.updateOne(
            { tag: `u${Math.floor(random() * 3)}`, accountId: account },
            { $set: { weight: 5 } },
            { upsert: true },
          );
      },
    },
    {
      name: "tag bulkWrite",
      run: async () => {
        const accounts = await accountIds();
        const tag = pick(await ids(db, "account_tags"));
        if (!accounts.length) return;
        await world.tags.bulkWrite([
          {
            insertOne: {
              document: { accountId: pick(accounts)!, tag: "k", weight: 1 },
            },
          },
          ...(tag
            ? [
                {
                  updateOne: {
                    filter: { _id: tag._id },
                    update: { $set: { accountId: pick(accounts)! } },
                  },
                },
              ]
            : []),
          { deleteMany: { filter: { tag: "u0" } } },
        ] as never);
      },
    },
    {
      name: "tag raw driver write",
      run: async () => {
        const account = pick(await accountIds());
        if (account)
          await world.tags.collection.insertOne({
            accountId: account,
            tag: "raw",
            weight: 1,
          } as never);
      },
    },
    {
      name: "participant insertOne",
      run: async () => {
        const account = pick(await accountIds());
        if (account)
          await world.expositions
            .scope(scopeOf())
            .insertOne("participant", { name: "P", userId: account });
      },
    },
    {
      name: "participant moves to another account",
      run: async () => {
        const scope = scopeOf();
        const participant = pick(await participantsIn(scope));
        const account = pick(await accountIds());
        if (participant && account)
          await world.expositions
            .scope(scope)
            .updateOne("participant", participant, { userId: account });
      },
    },
    {
      name: "participant deleteId",
      run: async () => {
        const scope = scopeOf();
        const participant = pick(await participantsIn(scope));
        if (participant && random() < 0.3)
          await world.expositions
            .scope(scope)
            .deleteId("participant", participant);
      },
    },
    {
      name: "membership insertMany",
      run: async () => {
        const scope = scopeOf();
        const view = world.expositions.scope(scope);
        const participants = await participantsIn(scope);
        const organization = pick(
          (
            await ids(db, "+expositions", {
              _type: "expo_organization",
              _scope: scope,
            })
          ).map((o) => String(o._id)),
        );
        if (!participants.length || !organization) return;
        await view.insertMany(
          "org_membership",
          [0, 1].map(() => ({
            participantId: pick(participants)!,
            organizationId: organization,
            status: random() < 0.7 ? ("active" as const) : ("removed" as const),
          })),
        );
      },
    },
    {
      name: "membership status flip",
      run: async () => {
        const scope = scopeOf();
        const membership = pick(
          await ids(db, "+expositions", {
            _type: "org_membership",
            _scope: scope,
          }),
        );
        if (membership)
          await world.expositions
            .scope(scope)
            .updateOne("org_membership", String(membership._id), {
              status: random() < 0.5 ? "active" : "removed",
            });
      },
    },
    {
      name: "membership moves to another participant",
      run: async () => {
        const scope = scopeOf();
        const membership = pick(
          await ids(db, "+expositions", {
            _type: "org_membership",
            _scope: scope,
          }),
        );
        const participant = pick(await participantsIn(scope));
        if (membership && participant)
          await world.expositions
            .scope(scope)
            .updateOne("org_membership", String(membership._id), {
              participantId: participant,
            });
      },
    },
    {
      name: "membership note only",
      run: async () => {
        const scope = scopeOf();
        const membership = pick(
          await ids(db, "+expositions", {
            _type: "org_membership",
            _scope: scope,
          }),
        );
        if (membership)
          await world.expositions
            .scope(scope)
            .updateOne("org_membership", String(membership._id), { note: "n" });
      },
    },
    {
      name: "membership updateWhere",
      run: async () =>
        void (await world.expositions
          .scope(scopeOf())
          .updateWhere(
            "org_membership",
            { status: "active" },
            { status: "removed" },
          )),
    },
    {
      name: "membership updateMany by ids",
      run: async () => {
        const scope = scopeOf();
        const memberships = (
          await ids(db, "+expositions", {
            _type: "org_membership",
            _scope: scope,
          })
        ).slice(0, 3);
        const participant = pick(await participantsIn(scope));
        if (!memberships.length || !participant) return;
        await world.expositions.scope(scope).updateMany({
          org_membership: Object.fromEntries(
            memberships.map((m) => [
              String(m._id),
              { participantId: participant, status: "active" as const },
            ]),
          ),
        });
      },
    },
    {
      name: "membership findOneAndUpdate",
      run: async () =>
        void (await world.expositions
          .scope(scopeOf())
          .findOneAndUpdate(
            "org_membership",
            { status: "removed" },
            { status: "active" },
          )),
    },
    {
      name: "membership deleteMany",
      run: async () =>
        void (
          random() < 0.3 &&
          (await world.expositions
            .scope(scopeOf())
            .deleteMany("org_membership", { status: "removed" }))
        ),
    },
    {
      name: "membership moves to another organization",
      run: async () => {
        const scope = scopeOf();
        const membership = pick(
          await ids(db, "+expositions", {
            _type: "org_membership",
            _scope: scope,
          }),
        );
        const organization = pick(
          (
            await ids(db, "+expositions", {
              _type: "expo_organization",
              _scope: scope,
            })
          ).map((o) => String(o._id)),
        );
        if (membership && organization)
          await world.expositions
            .scope(scope)
            .updateOne("org_membership", String(membership._id), {
              organizationId: organization,
            });
      },
    },
    {
      name: "organization status flip",
      run: async () => {
        const scope = scopeOf();
        const organization = pick(
          await ids(db, "+expositions", {
            _type: "expo_organization",
            _scope: scope,
          }),
        );
        if (organization)
          await world.expositions
            .scope(scope)
            .updateOne("expo_organization", String(organization._id), {
              status: random() < 0.5 ? "validated" : "pending",
            });
      },
    },
    {
      name: "organization rename",
      run: async () => {
        const scope = scopeOf();
        const organization = pick(
          await ids(db, "+expositions", {
            _type: "expo_organization",
            _scope: scope,
          }),
        );
        if (organization)
          await world.expositions
            .scope(scope)
            .updateOne("expo_organization", String(organization._id), {
              name: pick(["O1", "O2", "O3"])!,
            });
      },
    },
    {
      name: "organization insertOne",
      run: async () =>
        void (await world.expositions
          .scope(scopeOf())
          .insertOne("expo_organization", {
            name: pick(["O3", "O4"])!,
            status: random() < 0.5 ? "validated" : "pending",
          })),
    },
    {
      name: "organization deleteId",
      run: async () => {
        const scope = scopeOf();
        const organization = pick(
          await ids(db, "+expositions", {
            _type: "expo_organization",
            _scope: scope,
          }),
        );
        if (organization && random() < 0.3)
          await world.expositions
            .scope(scope)
            .deleteId("expo_organization", String(organization._id));
      },
    },
    {
      name: "scan insertOne",
      run: async () => {
        const scope = scopeOf();
        const scanned = await someParticipants(scope);
        const foreign = pick(
          await participantsIn(SCOPES.find((s) => s !== scope)!),
        );
        await world.scans.scope(scope).insertOne("scan", {
          scannedIds: [
            ...scanned,
            ...(foreign && random() < 0.3 ? [foreign] : []),
          ],
          kind: pick(["security", "business", "vip"])!,
        });
      },
    },
    {
      name: "scan deleteIds",
      run: async () => {
        const scope = scopeOf();
        const scans = (await ids(db, "+scans", { _scope: scope })).filter(
          () => random() < 0.3,
        );
        if (scans.length)
          await world.scans.scope(scope).deleteIds(
            "scan",
            scans.map((s) => String(s._id)),
          );
      },
    },
    {
      name: "lead insertOne",
      run: async () => void (await world.crm.insertOne("lead", { title: "L" })),
    },
    {
      name: "comment insertOne",
      run: async () => {
        const lead = pick(
          (await ids(db, "crm", { _type: "lead" })).map((l) => String(l._id)),
        );
        if (lead)
          await world.crm.insertOne("lead_comment", {
            leadId: lead,
            body: "c",
          });
      },
    },
    {
      name: "comment moves to another lead",
      run: async () => {
        const comment = pick(await ids(db, "crm", { _type: "lead_comment" }));
        const lead = pick(
          (await ids(db, "crm", { _type: "lead" })).map((l) => String(l._id)),
        );
        if (comment && lead)
          await world.crm.updateOne("lead_comment", String(comment._id), {
            leadId: lead,
          });
      },
    },
    {
      name: "comment deleteAny",
      run: async () => {
        const comment = pick(await ids(db, "crm", { _type: "lead_comment" }));
        if (comment && random() < 0.4)
          await world.crm.deleteAny({ _id: comment._id } as never);
      },
    },
  ];
}

async function seedOrganizations(world: World): Promise<void> {
  for (const scope of SCOPES) {
    await world.expositions.scope(scope).insertMany("expo_organization", [
      { name: "O1", status: "validated" },
      { name: "O2", status: "pending" },
    ]);
  }
}

async function assertNoDrift(db: Db, label: string): Promise<void> {
  const result = await checkComputed(db, computedTopology(schemas));
  assert(
    result.drifts.length === 0,
    `${label}: ${JSON.stringify(result.drifts.slice(0, 3))}`,
  );
}

for (const [seed, inlineLimit] of [
  [1, undefined],
  [7, undefined],
  [42, undefined],
  [3, 1],
  [11, 1],
] as const) {
  test(`computed oracle: random writes across every collection kind keep every field equal to a full apply (seed ${seed}${inlineLimit ? `, inline limit ${inlineLimit} with marks drained` : ""})`, async (t) => {
    await withDatabase(t.name, async (db) => {
      const random = mulberry32(seed);
      const world = await openAll(db, inlineLimit);
      await seedOrganizations(world);
      const all = steps(db, world, random);
      const { withSession } = getSessionContext(db.client);
      const applied = new Map<string, number>();
      let markedSteps = 0;
      for (let index = 0; index < 160; index++) {
        const roll = random();
        const chosen = all[Math.floor(random() * all.length)]!;
        const label = `step ${index} ${chosen.name}`;
        if (roll < 0.2) {
          const second = all[Math.floor(random() * all.length)]!;
          await withSession(async () => {
            await chosen.run();
            await second.run();
          });
        } else if (roll < 0.28) {
          await withSession(async () => {
            await chosen.run();
            throw new Error("rolled back on purpose");
          }).catch((error: Error) =>
            assertEquals(error.message, "rolled back on purpose"),
          );
        } else {
          await chosen.run();
        }
        applied.set(chosen.name, (applied.get(chosen.name) ?? 0) + 1);
        const drained = await drainComputedPending(db, {
          topology: computedTopology(schemas),
        });
        markedSteps += drained.drained > 0 ? 1 : 0;
        assertEquals(drained.remaining, 0, `${label}: every mark is drained`);
        await assertNoDrift(db, label);
      }
      assert(
        applied.size >= all.length - 3,
        `most kinds of write were exercised: ${[...applied.keys()].length}/${all.length}`,
      );
      if (inlineLimit)
        assert(
          markedSteps >= 10,
          `the marks path was exercised on ${markedSteps} steps`,
        );
    });
  });
}
