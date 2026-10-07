/**
 * Tests for the shared migration reference resolver.
 *
 * Both `baseline` and `migrate --target` write to a real database at the point
 * the reference designates, so the case worth guarding hardest is the ambiguous
 * one: silently picking a best match would aim the command at the wrong
 * migration without anybody being told.
 *
 * @module
 */

import { test } from "../../+harness.ts";
import {
  assertEquals,
  assertStringIncludes,
  assertThrows,
} from "../../+assert.ts";
import { resolveMigrationRef } from "../../../src/migration/cli/utils/resolve-ref.ts";

const CHAIN = [
  { id: "2026_05_24_1922_DQ176MQ77M", name: "init" },
  { id: "2026_08_15_1256_BDGEDIT01", name: "badge_editor_shapes_and_layers" },
  { id: "2026_08_17_2121_8SKVBG31DA", name: "add_webauthn_credentials" },
];

test("resolveMigrationRef: an exact id wins", () => {
  assertEquals(
    resolveMigrationRef(CHAIN, "2026_08_15_1256_BDGEDIT01"),
    CHAIN[1],
  );
});

test("resolveMigrationRef: an exact name is accepted too", () => {
  assertEquals(
    resolveMigrationRef(CHAIN, "add_webauthn_credentials"),
    CHAIN[2],
  );
});

test("resolveMigrationRef: an unambiguous substring is enough", () => {
  // The point of the whole function: nobody retypes a full id by hand.
  assertEquals(resolveMigrationRef(CHAIN, "BDGEDIT"), CHAIN[1]);
  assertEquals(resolveMigrationRef(CHAIN, "webauthn"), CHAIN[2]);
  assertEquals(resolveMigrationRef(CHAIN, "bdgedit01"), CHAIN[1]);
});

test("resolveMigrationRef: an ambiguous reference is refused, never guessed", () => {
  // "2026_08" matches two migrations. Returning the first would aim a write at
  // a different point of the chain than the operator named.
  const error = assertThrows(
    () => resolveMigrationRef(CHAIN, "2026_08"),
    Error,
  );
  assertStringIncludes(error.message, "ambiguous");
  assertStringIncludes(error.message, "2026_08_15_1256_BDGEDIT01");
  assertStringIncludes(error.message, "2026_08_17_2121_8SKVBG31DA");
});

test("resolveMigrationRef: an exact match beats a broader substring", () => {
  // A name that is also a substring of another entry must still resolve, or an
  // exactly-named migration would become unreachable as the chain grows.
  const overlapping = [
    { id: "001", name: "auth" },
    { id: "002", name: "auth_webauthn" },
  ];
  assertEquals(resolveMigrationRef(overlapping, "auth"), overlapping[0]);
});

test("resolveMigrationRef: an unknown reference lists what exists", () => {
  const error = assertThrows(() => resolveMigrationRef(CHAIN, "nope"), Error);
  assertStringIncludes(error.message, "No migration matches");
  assertStringIncludes(error.message, "init");
});

test("resolveMigrationRef: an empty reference is refused", () => {
  assertThrows(() => resolveMigrationRef(CHAIN, "   "), Error);
});
