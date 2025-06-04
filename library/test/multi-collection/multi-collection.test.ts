import * as v from "../../src/schema.ts";
import { assertEquals, assertRejects, assert } from "jsr:@std/assert";
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

Deno.test("Date fields: Insert and query with dates", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            event: {
                name: v.string(),
                startDate: v.date(),
                endDate: v.date(),
            },
            user: {
                name: v.string(),
                birthDate: v.date(),
                lastLogin: v.date(),
            }
        });

        const now = new Date();
        const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);
        const birthDate = new Date("1990-05-15");

        const eventId = await collection.insertOne("event", {
            name: "Conference",
            startDate: now,
            endDate: tomorrow,
        });

        const userId = await collection.insertOne("user", {
            name: "Alice",
            birthDate: birthDate,
            lastLogin: yesterday,
        });

        // Test finding by date
        const events = await collection.find("event", {
            startDate: { $lte: now }
        });
        assertEquals(events.length, 1);
        assertEquals(events[0].name, "Conference");

        // Test finding users by date range
        const users = await collection.find("user", {
            lastLogin: { $gte: yesterday }
        });
        assertEquals(users.length, 1);
        assertEquals(users[0].name, "Alice");

        // Verify date fields are preserved correctly
        const foundEvent = await collection.findOne("event", { _id: eventId });
        assertEquals(foundEvent?.startDate.getTime(), now.getTime());
        assertEquals(foundEvent?.endDate.getTime(), tomorrow.getTime());
    });
});

Deno.test("Date fields: Date comparisons and sorting", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            task: {
                title: v.string(),
                dueDate: v.date(),
                createdAt: v.date(),
            }
        });

        const baseDate = new Date("2025-01-01");
        const tasks = [
            {
                title: "Task 1",
                dueDate: new Date(baseDate.getTime() + 1 * 24 * 60 * 60 * 1000),
                createdAt: baseDate,
            },
            {
                title: "Task 2", 
                dueDate: new Date(baseDate.getTime() + 3 * 24 * 60 * 60 * 1000),
                createdAt: new Date(baseDate.getTime() + 1 * 24 * 60 * 60 * 1000),
            },
            {
                title: "Task 3",
                dueDate: new Date(baseDate.getTime() + 2 * 24 * 60 * 60 * 1000),
                createdAt: new Date(baseDate.getTime() + 2 * 24 * 60 * 60 * 1000),
            }
        ];

        await collection.insertMany("task", tasks);

        // Test date range queries
        const urgentTasks = await collection.find("task", {
            dueDate: { 
                $lte: new Date(baseDate.getTime() + 2 * 24 * 60 * 60 * 1000) 
            }
        });
        assertEquals(urgentTasks.length, 2);

        // Test finding tasks created after a specific date
        const recentTasks = await collection.find("task", {
            createdAt: { $gt: baseDate }
        });
        assertEquals(recentTasks.length, 2);

        // Test date between range
        const midRangeTasks = await collection.find("task", {
            dueDate: {
                $gte: new Date(baseDate.getTime() + 1 * 24 * 60 * 60 * 1000),
                $lte: new Date(baseDate.getTime() + 2 * 24 * 60 * 60 * 1000)
            }
        });
        assertEquals(midRangeTasks.length, 2);
    });
});

Deno.test("Date fields: Current date and date updates", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            document: {
                title: v.string(),
                createdAt: v.date(),
                updatedAt: v.date(),
            }
        });

        const startTime = new Date();
        
        const docId = await collection.insertOne("document", {
            title: "My Document",
            createdAt: startTime,
            updatedAt: startTime,
        });

        // Wait a bit to ensure different timestamps
        await new Promise(resolve => setTimeout(resolve, 10));
        
        const updateTime = new Date();
        
        await collection.updateOne("document", docId, { title: "Updated Document", updatedAt: updateTime });

        const updatedDoc = await collection.findOne("document", { _id: docId });
        
        assertEquals(updatedDoc?.title, "Updated Document");
        assertEquals(updatedDoc?.createdAt.getTime(), startTime.getTime());
        assertEquals(updatedDoc?.updatedAt.getTime(), updateTime.getTime());
        
        // Verify updatedAt is after createdAt
        assert(updatedDoc?.updatedAt >= updatedDoc?.createdAt);
    });
});

Deno.test("Date fields: Date edge cases", async (t) => {
    await withDatabase(t.name, async (db) => {        const collection = await multiCollection(db, "test", {
            appointment: {
                title: v.string(),
                scheduledFor: v.date(),
                reminderDate: v.optional(v.date()),
            }
        });

        // Test with very old date
        const oldDate = new Date("1900-01-01");
        // Test with future date
        const futureDate = new Date("2030-12-31");
        
        const appointment1 = await collection.insertOne("appointment", {
            title: "Historical Event",
            scheduledFor: oldDate,
        });

        const appointment2 = await collection.insertOne("appointment", {
            title: "Future Meeting",
            scheduledFor: futureDate,
            reminderDate: new Date("2030-12-30"),
        });

        // Find appointments by date range
        const oldAppointments = await collection.find("appointment", {
            scheduledFor: { $lt: new Date("2000-01-01") }
        });
        assertEquals(oldAppointments.length, 1);
        assertEquals(oldAppointments[0].title, "Historical Event");

        const futureAppointments = await collection.find("appointment", {
            scheduledFor: { $gt: new Date("2025-01-01") },
        });
        assertEquals(futureAppointments.length, 1);
        assertEquals(futureAppointments[0].title, "Future Meeting");

        // Test optional date field
        const withReminder = await collection.find("appointment", {
            reminderDate: { $exists: true }
        });
        assertEquals(withReminder.length, 1);
    });
});