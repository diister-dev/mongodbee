import { assertEquals } from "@std/assert";
import { extractIdPrefix } from "../../src/migration/utils/seed-id.ts";
import { notPersonal, personal, personId } from "../../src/privacy/mod.ts";
import { dbId, refId } from "../../src/ids.ts";

Deno.test("extractIdPrefix reads the space through privacy wrappers on a refId or dbId", () => {
  assertEquals(extractIdPrefix(refId("exposition")), "exposition");
  assertEquals(
    extractIdPrefix(notPersonal(refId("exposition"), "event")),
    "exposition",
  );
  assertEquals(
    extractIdPrefix(personal(refId("business_scan"), { of: "participant" })),
    "business_scan",
  );
  assertEquals(
    extractIdPrefix(personal(dbId("badge"), { of: "participantId" })),
    "badge",
  );
  assertEquals(extractIdPrefix(personId("user")), "user");
  assertEquals(
    extractIdPrefix(personId("participant", { of: ["user"] })),
    "participant",
  );
});
