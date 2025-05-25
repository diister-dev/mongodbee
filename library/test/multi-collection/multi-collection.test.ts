import * as v from "../../src/schema.ts";
import { assertEquals, assertRejects } from "jsr:@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";

Deno.test("Basic test", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
            }
        });

        const userA = await collection.insertOne("user", {
            name: "John",
            mail: "john@doe.d"
        });

        const userB = await collection.insertOne("user", {
            name: "Jane",
            mail: "jane@doe.d"
        });

        await collection.insertOne("group", {
            name: "John",
            members: [userA]
        });

        const users = await collection.find("user");
        assertEquals(users.length, 2);

        const groups = await collection.find("group");
        assertEquals(groups.length, 1);

        const findUserB = await collection.findOne("user", { _id: userB });
        assertEquals(findUserB, {
            _id: userB,
            _type: "user",
            name: "Jane",
            mail: "jane@doe.d"
        });

        const deleteUserB = await collection.deleteId("user", userB);
        assertEquals(deleteUserB, 1);
    });
});

Deno.test("FindOne: Ensure find correct type", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
            }
        });

        const usersId = await collection.insertMany("user", [{
            name: "John",
            mail: "john@doe.d"
        }, {
            name: "Jane",
            mail: "jane@doe.d"
        }]);

        const groupsId = await collection.insertMany("group", [{
            name: "John",
            members: [usersId[0]]
        }, {
            name: "Jane",
            members: [usersId[1]]
        }]);

        await assertRejects(async () => {
            await collection.findOne("user", { _id: groupsId[0] });
        });

        await assertRejects(async () => {
            await collection.findOne("group", { _id: usersId[0] });
        });

        await assertRejects(async () => {
            await collection.findOne("group", { _id: "group-invalid:id" });
        });

        const findUserB = await collection.findOne("user", { _id: usersId[1] });
        assertEquals(findUserB, {
            _id: usersId[1],
            _type: "user",
            name: "Jane",
            mail: "jane@doe.d"
        });

        const findGroupB = await collection.findOne("group", { _id: groupsId[1] });
        assertEquals(findGroupB, {
            _id: groupsId[1],
            _type: "group",
            name: "Jane",
            members: [usersId[1]]
        });
    });
});

Deno.test("find: Ensure find correct type", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
            }
        });

        const usersId = await collection.insertMany("user", [{
            name: "John",
            mail: "john@doe.d"
        }, {
            name: "Jane",
            mail: "jane@doe.d"
        }]);

        const groupsId = await collection.insertMany("group", [{
            name: "John",
            members: [usersId[0]]
        }, {
            name: "Jane",
            members: [usersId[1]]
        }]);

        {
            const count = await collection.find("user", { _id: groupsId[0] });
            assertEquals(count.length, 0);
        }

        {
            const count = await collection.find("group", { _id: usersId[0] });
            assertEquals(count.length, 0);
        }

        {
            const count = await collection.find("group", { _id: groupsId[0] });
            assertEquals(count.length, 1);
        }

        {
            const count = await collection.find("user", { _id: usersId[0] });
            assertEquals(count.length, 1);
        }
        
        {
            const count = await collection.find("group", { _id: "group-invalid:id" });
            assertEquals(count.length, 0); 
        }
        
        const users = await collection.find("user", {});
        assertEquals(users.length, 2);

        const groups = await collection.find("group", {});
        assertEquals(groups.length, 2);
    });
});

Deno.test("DeleteId: Ensure delete correct type", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
            }
        });

        const usersId = await collection.insertMany("user", [{
            name: "John",
            mail: "john@doe.d"
        }, {
            name: "Jane",
            mail: "jane@doe.d"
        }]);

        const groupsId = await collection.insertMany("group", [{
            name: "John",
            members: [usersId[0]]
        }, {
            name: "Jane",
            members: [usersId[1]]
        }]);

        await assertRejects(async () => {
            await collection.deleteId("user", groupsId[0]);
        });

        await assertRejects(async () => {
            await collection.deleteId("group", usersId[0]);
        });

        await assertRejects(async () => {
            await collection.deleteId("group", "group-invalid:id");
        });

        const deleteUserB = await collection.deleteId("user", usersId[1]);
        assertEquals(deleteUserB, 1);

        const deleteGroupB = await collection.deleteId("group", groupsId[1]);
        assertEquals(deleteGroupB, 1);
    });
});

Deno.test("DeleteIds: Ensure delete correct type", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
            }
        });

        const usersId = await collection.insertMany("user", [{
            name: "John",
            mail: "john@doe.d"
        }, {
            name: "Jane",
            mail: "jane@doe.d"
        }]);

        const groupsId = await collection.insertMany("group", [{
            name: "John",
            members: [usersId[0]]
        }, {
            name: "Jane",
            members: [usersId[1]]
        }]);

        await assertRejects(async () => {
            await collection.deleteIds("user", groupsId);
        });

        await assertRejects(async () => {
            await collection.deleteIds("group", usersId);
        });

        await assertRejects(async () => {
            await collection.deleteIds("group", [usersId[0], groupsId[0]]);
        });

        await assertRejects(async () => {
            await collection.deleteIds("group", ["group-invalid:id"]);
        });

        const deleteUserB = await collection.deleteIds("user", usersId.slice(1));
        assertEquals(deleteUserB, 1);

        const deleteGroupB = await collection.deleteIds("group", groupsId);
        assertEquals(deleteGroupB, 2);
    });
});

Deno.test("RANDOM TEST - TO DELETE", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
                age: v.number(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
            }
        });

        const usersId = await collection.insertMany("user", [{
            name: "John",
            mail: "john@doe.d",
            age: 20,
        }, {
            name: "Jane",
            mail: "jane@doe.d",
            age: 25,
        }, {
            name: "Jack",
            mail: "jack@doe.d",
            age: 30,
        }, {
            name: "Jill",
            mail: "jill@doe.d",
            age: 30,
        }]);

        await collection.insertMany("group", [{
            name: "John",
            members: [usersId[0], usersId[1], usersId[3]]
        }, {
            name: "Jane",
            members: [usersId[1]]
        }, {
            name: "Jack",
            members: [usersId[2]]
        }, {
            name: "Jill",
            members: [usersId[3]]
        }]);

        await collection.aggregate((stage) => [
            stage.match("group", {}),
            stage.unwind("group", "members"),
            stage.lookup("group", "members", "_id"),
        ]);
    });
});