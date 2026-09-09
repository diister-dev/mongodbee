/**
 * Tests for paginate with pipeline support in simple collections (collection.ts)
 */

import { test } from "./+harness.ts";
import { assertEquals, assertExists } from "./+assert.ts";
import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";

test("Collection paginate with pipeline - addFields", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
      departmentId: v.optional(v.string()),
    };

    const users = await collection(db, "users", UserSchema);

    // Insert test data
    await users.insertOne({ name: "Alice", age: 30 });
    await users.insertOne({ name: "Bob", age: 25 });
    await users.insertOne({ name: "Charlie", age: 35 });

    const result = await users.paginate(
      {},
      {
        pipeline: (stage) => [
          stage.addFields({ isAdult: { $gte: ["$age", 18] } }),
        ],
      },
    );

    assertEquals(result.total, 3);
    assertEquals(result.data.length, 3);
    assertEquals((result.data[0] as any).isAdult, true);
    assertEquals((result.data[1] as any).isAdult, true);
    assertEquals((result.data[2] as any).isAdult, true);
  });
});

test("Collection paginate with pipeline - self lookup", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Insert test data with manager reference
    const ManagerSchema = {
      name: v.string(),
      age: v.number(),
      managerId: v.optional(v.string()),
    };

    const employees = await collection(db, "employees", ManagerSchema);

    const managerId = await employees.insertOne({ name: "Manager", age: 45 });
    await employees.insertOne({
      name: "Employee1",
      age: 28,
      managerId: managerId.toString(),
    });
    await employees.insertOne({
      name: "Employee2",
      age: 32,
      managerId: managerId.toString(),
    });

    const result = await employees.paginate(
      { managerId: { $exists: true } },
      {
        pipeline: (stage) => [stage.lookup("managerId", "_id", "manager")],
      },
    );

    assertEquals(result.total, 2);
    assertEquals(result.data.length, 2);
    // Note: lookup returns an array
    assertExists((result.data[0] as any).manager);
    assertExists((result.data[1] as any).manager);
  });
});

test("Collection paginate with pipeline - externalLookup", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
      departmentId: v.optional(v.string()),
    };

    const DepartmentSchema = {
      name: v.string(),
      budget: v.number(),
    };

    const users = await collection(db, "users", UserSchema);
    const departments = await collection(db, "departments", DepartmentSchema);

    // Insert departments
    const engDeptId = await departments.insertOne({
      name: "Engineering",
      budget: 100000,
    });
    const hrDeptId = await departments.insertOne({ name: "HR", budget: 50000 });

    // Insert users with department references (store ObjectId as string)
    await users.insertOne({
      name: "Alice",
      age: 30,
      departmentId: engDeptId.toString(),
    });
    await users.insertOne({
      name: "Bob",
      age: 25,
      departmentId: engDeptId.toString(),
    });
    await users.insertOne({
      name: "Charlie",
      age: 35,
      departmentId: hrDeptId.toString(),
    });

    // Use addFields to convert string to ObjectId for lookup match
    const result = await users.paginate(
      {},
      {
        pipeline: (stage) => [
          stage.addFields({ departmentOid: { $toObjectId: "$departmentId" } }),
          stage.externalLookup(
            "departments",
            "departmentOid",
            "_id",
            "department",
          ),
          stage.addFields({
            departmentName: { $arrayElemAt: ["$department.name", 0] },
          }),
        ],
      },
    );

    assertEquals(result.total, 3);
    assertEquals(result.data.length, 3);

    // All users should have their department lookup
    assertExists((result.data[0] as any).department);
    assertExists((result.data[0] as any).departmentName);
  });
});

test("Collection paginate with pipeline - project", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
    };

    const users = await collection(db, "users", UserSchema);

    await users.insertOne({ name: "Alice", age: 30 });
    await users.insertOne({ name: "Bob", age: 25 });

    // Use addFields instead of project to add computed field while keeping original fields
    // This way schema validation still passes
    const result = await users.paginate(
      {},
      {
        pipeline: (stage) => [
          stage.addFields({ isOld: { $gte: ["$age", 30] } }),
        ],
      },
    );

    assertEquals(result.total, 2);
    assertEquals(result.data.length, 2);

    // First doc (Alice, age 30)
    assertEquals((result.data[0] as any).isOld, true);
    // Second doc (Bob, age 25)
    assertEquals((result.data[1] as any).isOld, false);
  });
});

test("Collection paginate with pipeline - cursor pagination afterId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
    };

    const users = await collection(db, "users", UserSchema);

    const id1 = await users.insertOne({ name: "User1", age: 20 });
    await users.insertOne({ name: "User2", age: 25 });
    await users.insertOne({ name: "User3", age: 30 });

    // Get page after first user
    const result = await users.paginate(
      {},
      {
        afterId: id1,
        limit: 2,
        pipeline: (stage) => [
          stage.addFields({
            ageGroup: { $cond: [{ $gte: ["$age", 25] }, "adult", "young"] },
          }),
        ],
      },
    );

    assertEquals(result.total, 3);
    assertEquals(result.position, 1); // After first element
    assertEquals(result.data.length, 2);
    assertEquals(result.data[0].name, "User2");
    assertEquals((result.data[0] as any).ageGroup, "adult");
  });
});

test("Collection paginate with pipeline - cursor pagination beforeId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
    };

    const users = await collection(db, "users", UserSchema);

    await users.insertOne({ name: "User1", age: 20 });
    await users.insertOne({ name: "User2", age: 25 });
    const id3 = await users.insertOne({ name: "User3", age: 30 });

    // Get page before last user
    // With beforeId, items are returned in reverse order (closest to anchor first)
    const result = await users.paginate(
      {},
      {
        beforeId: id3,
        limit: 2,
        pipeline: (stage) => [
          stage.addFields({
            decade: { $multiply: [{ $floor: { $divide: ["$age", 10] } }, 10] },
          }),
        ],
      },
    );

    assertEquals(result.total, 3);
    assertEquals(result.data.length, 2);
    // Items are returned in the SAME order as forward pagination
    assertEquals(result.data[0].name, "User1");
    assertEquals((result.data[0] as any).decade, 20);
    assertEquals(result.data[1].name, "User2");
    assertEquals((result.data[1] as any).decade, 20);
  });
});

test("Collection paginate with pipeline - combined with MongoDB filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
    };

    const users = await collection(db, "users", UserSchema);

    await users.insertOne({ name: "Alice", age: 30 });
    await users.insertOne({ name: "Bob", age: 25 });
    await users.insertOne({ name: "Charlie", age: 35 });
    await users.insertOne({ name: "Diana", age: 20 });

    const result = await users.paginate(
      { age: { $gte: 25 } },
      {
        pipeline: (stage) => [stage.addFields({ category: "mature" })],
      },
    );

    assertEquals(result.total, 3); // Alice, Bob, Charlie (age >= 25)
    assertEquals(result.data.length, 3);
    assertEquals(
      result.data.every((u: any) => u.category === "mature"),
      true,
    );
  });
});

test("Collection paginate with pipeline - prepare filter format", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
    };

    const users = await collection(db, "users", UserSchema);

    await users.insertOne({ name: "Alice", age: 30 });
    await users.insertOne({ name: "Bob", age: 25 });
    await users.insertOne({ name: "Charlie", age: 35 });

    type UserWithScore = { name: string; score: number };

    const result = await users.paginate<
      UserWithScore,
      { displayName: string; finalScore: number }
    >(
      {},
      {
        pipeline: (stage) => [
          stage.addFields({ score: { $multiply: ["$age", 10] } }),
        ],
        prepare: (doc) =>
          ({ name: doc.name, score: (doc as any).score }) as UserWithScore,
        filter: (doc) => doc.score > 260, // Filter out Bob (250)
        format: (doc) => ({
          displayName: `User: ${doc.name}`,
          finalScore: doc.score,
        }),
      },
    );

    assertEquals(result.total, 3);
    assertEquals(result.data.length, 2); // Alice and Charlie (filtered out Bob)
    assertEquals(result.data[0].displayName, "User: Alice");
    assertEquals(result.data[0].finalScore, 300);
  });
});

test("Collection paginate - backwards compatibility without pipeline", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
    };

    const users = await collection(db, "users", UserSchema);

    await users.insertOne({ name: "Alice", age: 30 });
    await users.insertOne({ name: "Bob", age: 25 });
    await users.insertOne({ name: "Charlie", age: 35 });

    // Test without pipeline - should work as before
    const result = await users.paginate(
      {},
      {
        limit: 2,
        format: (doc) => ({ n: doc.name }),
      },
    );

    assertEquals(result.total, 3);
    assertEquals(result.data.length, 2);
    assertEquals(result.data[0], { n: "Alice" });
    assertEquals(result.data[1], { n: "Bob" });
  });
});

test("Collection paginate with pipeline - match stage", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
    };

    const users = await collection(db, "users", UserSchema);

    await users.insertOne({ name: "Alice", age: 30 });
    await users.insertOne({ name: "Bob", age: 25 });
    await users.insertOne({ name: "Charlie", age: 35 });

    const result = await users.paginate(
      {},
      {
        pipeline: (stage) => [
          stage.addFields({
            ageCategory: {
              $cond: [{ $gte: ["$age", 30] }, "senior", "junior"],
            },
          }),
          stage.match({ ageCategory: "senior" }),
        ],
      },
    );

    assertEquals(result.total, 2);
    assertEquals(result.data.length, 2);
    assertEquals(result.data[0].name, "Alice");
    assertEquals(result.data[1].name, "Charlie");
  });
});

test("Collection paginate with pipeline - externalLookup with advanced options", async (t) => {
  await withDatabase(t.name, async (db) => {
    const UserSchema = {
      name: v.string(),
      age: v.number(),
      departmentId: v.optional(v.string()),
    };

    const DepartmentSchema = {
      name: v.string(),
      budget: v.number(),
    };

    const users = await collection(db, "users", UserSchema);
    const departments = await collection(db, "departments", DepartmentSchema);

    const engDeptId = await departments.insertOne({
      name: "Engineering",
      budget: 100000,
    });
    await departments.insertOne({ name: "HR", budget: 50000 });

    await users.insertOne({
      name: "Alice",
      age: 30,
      departmentId: engDeptId.toString(),
    });

    // Convert string to ObjectId for lookup, use advanced pipeline options
    const result = await users.paginate(
      {},
      {
        pipeline: (stage) => [
          stage.addFields({ departmentOid: { $toObjectId: "$departmentId" } }),
          stage.externalLookup("departments", "departmentOid", "_id", {
            as: "dept",
            pipeline: [{ $project: { name: 1, _id: 0 } }],
          }),
        ],
      },
    );

    assertEquals(result.total, 1);
    assertEquals(result.data.length, 1);
    assertExists((result.data[0] as any).dept);
    // The lookup should only have 'name' due to projection
    const dept = (result.data[0] as any).dept[0];
    assertEquals(dept.name, "Engineering");
    assertEquals(dept.budget, undefined);
    assertEquals(dept._id, undefined);
  });
});
