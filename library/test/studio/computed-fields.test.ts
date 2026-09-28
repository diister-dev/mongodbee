import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { defineType } from "../../src/type-definition.ts";
import { from } from "../../src/computed.ts";
import { typeFields } from "../../src/studio/api/schema.ts";
import {
  diffDocument,
  editableOf,
  parseEditable,
} from "../../src/studio/ui/lib/edit.ts";

const Membership = defineType({
  schema: v.object({
    participantId: refId("participant"),
    organizationId: refId("organization"),
    status: v.picklist(["active", "removed"]),
  }),
});

const Participant = defineType({
  schema: v.object({ name: v.string() }),
  computed: {
    organizationIds: from("membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId),
    membershipCount: from("membership", Membership)
      .by((m) => m.participantId)
      .count(),
  },
});

test("typeFields marks _computed and its revision as system fields", () => {
  const fields = typeFields(Participant);
  const root = fields._computed;
  assert(root !== undefined);
  assertEquals(root.system, "computed");
  assertEquals(fields.name.system, undefined);
  const entries = root.entries ?? {};
  assertEquals(entries._rev.system, "revision");
  assertEquals(entries.organizationIds.system, "computed");
  assertEquals(
    entries.organizationIds.computed,
    "organizationId of membership by participantId, where status",
  );
  assertEquals(
    entries.membershipCount.computed,
    "count of membership by participantId",
  );
});

test("typeFields leaves a type without computed fields untouched", () => {
  const fields = typeFields(Membership);
  assertEquals(fields._computed, undefined);
  assertEquals(Object.keys(fields), [
    "participantId",
    "organizationId",
    "status",
  ]);
});

test("the editor never offers _computed for editing", () => {
  const stored = {
    _id: "participant:1",
    name: "Ada",
    _computed: { organizationIds: ["organization:1"], _rev: 3 },
  };
  assertEquals(editableOf(stored), { name: "Ada" });
  const change = diffDocument(stored, { name: "Ada Lovelace" });
  assertEquals(change.set, { name: "Ada Lovelace" });
  assertEquals(change.unset, []);
  const parsed = parseEditable(
    JSON.stringify({ name: "Ada", _computed: { _rev: 4 } }),
  );
  assertEquals(parsed.ok, false);
});
