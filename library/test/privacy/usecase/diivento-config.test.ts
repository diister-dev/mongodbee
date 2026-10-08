import { test } from "../../+harness.ts";
import { defineConfig } from "../../../src/config.ts";
import { assertEquals } from "../../+assert.ts";
import * as v from "../../../src/schema.ts";
import { dbId, refId } from "../../../src/ids.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  notPersonal,
  personId,
  remapId,
} from "../../../src/privacy/mod.ts";

const SECRET = "s3cret";
const ORG = "expo_organization:01j5zk3v8n2q4x6y8z0b1c3d5e";
const ROLE = "expo_organization_role:01j5zk3vt6bfqh0aer9a9s1nmc";

const SCHEMAS = {
  collections: {
    users: { _id: personId("user"), email: v.pipe(v.string(), v.email()) },
    organizations: { _id: refId("expo_organization"), displayName: v.string() },
    roles: {
      _id: notPersonal(refId("expo_organization_role"), "role configuration", {
        strict: "keep",
      }),
      organizationId: refId("expo_organization"),
      permissions: v.array(
        v.object({ key: v.string(), value: v.optional(v.any()) }),
      ),
    },
    flows: {
      _id: notPersonal(dbId("flow"), "flow configuration", { strict: "keep" }),
      name: v.string(),
    },
  },
};

function transform(key: string, doc: Record<string, unknown>) {
  const plan = buildPrivacyPlan({ schemas: SCHEMAS, posture: "strict" });
  return createPrivacyTransformer({
    plan,
    schemas: SCHEMAS,
    secret: SECRET,
    timeShiftMs: 0,
  }).transform(key, doc).doc;
}

test({
  name: "usecase: an id inside an untyped permission payload is remapped, not faked",
  fn: () => {
    const out = transform("collections/roles/", {
      _id: ROLE,
      organizationId: ORG,
      permissions: [
        {
          key: "expositions.participants.list",
          value: {
            with: { memberships_of_participant: { organizationId: ORG } },
          },
        },
      ],
    });
    const permissions = out.permissions as {
      value: {
        with: { memberships_of_participant: { organizationId: string } };
      };
    }[];
    assertEquals(out.organizationId, remapId(SECRET, ORG));
    assertEquals(
      permissions[0].value.with.memberships_of_participant.organizationId,
      remapId(SECRET, ORG),
    );
  },
});

test({
  name: "usecase gap: a document declared not personal with strict keep keeps its strings under the strict posture",
  fn: () => {
    const out = transform("collections/flows/", {
      _id: "flow:01j5zk3v8n2q4x6y8z0b1c3d5e",
      name: "Visiteur Salon Pro",
    });
    assertEquals(out.name, "Visiteur Salon Pro");
    assertEquals(out._id, remapId(SECRET, "flow:01j5zk3v8n2q4x6y8z0b1c3d5e"));
  },
});

test("usecase: defineConfig accepts the privacy hooks the extract reads", () => {
  const resolveDynamic = () => ({});
  const config = defineConfig({ privacy: { resolveDynamic } });
  assertEquals(config.privacy?.resolveDynamic, resolveDynamic);
});
