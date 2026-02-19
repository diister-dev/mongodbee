import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

const multiSchema = {
  user: {
    name: v.string(),
    mail: v.string(),
  },
  group: {
    name: v.string(),
    members: v.array(v.string()),
  },
};

const userGroupModel = defineModel("test", {
  schema: multiSchema,
});

test("Basic test", async () => {
  await withDatabase("Basic test", async (db) => {
    const collection = await multiCollection(db, "test", userGroupModel);

    const userA = await collection.insertOne("user", {
      name: "John",
      mail: "john@doe.d",
    });

    const userB = await collection.insertOne("user", {
      name: "Jane",
      mail: "jane@doe.d",
    });

    await collection.insertOne("group", {
      name: "John",
      members: [userA],
    });

    const users = await collection.find("user").toArray();
    expect(users.length).toEqual(2);

    const groups = await collection.find("group").toArray();
    expect(groups.length).toEqual(1);

    const findUserB = await collection.findOne("user", { _id: userB });
    expect(findUserB).toEqual({
      _id: userB,
      _type: "user",
      name: "Jane",
      mail: "jane@doe.d",
    });

    const deleteUserB = await collection.deleteById("user", userB);
    expect(deleteUserB).toEqual(1);
  });
});

test("FindOne: Ensure find correct type", async () => {
  await withDatabase("FindOne: Ensure find correct type", async (db) => {
    const collection = await multiCollection(db, "test", userGroupModel);

    const usersId = await collection.insertMany("user", [{
      name: "John",
      mail: "john@doe.d",
    }, {
      name: "Jane",
      mail: "jane@doe.d",
    }]);

    const groupsId = await collection.insertMany("group", [{
      name: "John",
      members: [usersId[0]],
    }, {
      name: "Jane",
      members: [usersId[1]],
    }]);

    const notFoundUser = await collection.findOne("user", { _id: groupsId[0] });
    expect(notFoundUser === null).toBeTruthy();

    const notFoundGroup = await collection.findOne("group", {
      _id: usersId[0],
    });
    expect(notFoundGroup === null).toBeTruthy();

    const notFoundGroup2 = await collection.findOne("group", {
      _id: "group-invalid:id",
    });
    expect(notFoundGroup2 === null).toBeTruthy();

    const findUserB = await collection.findOne("user", { _id: usersId[1] });
    expect(findUserB).toEqual({
      _id: usersId[1],
      _type: "user",
      name: "Jane",
      mail: "jane@doe.d",
    });

    const findGroupB = await collection.findOne("group", { _id: groupsId[1] });
    expect(findGroupB).toEqual({
      _id: groupsId[1],
      _type: "group",
      name: "Jane",
      members: [usersId[1]],
    });
  });
});

test("find: Ensure find correct type", async () => {
  await withDatabase("find: Ensure find correct type", async (db) => {
    const collection = await multiCollection(db, "test", userGroupModel);

    const usersId = await collection.insertMany("user", [{
      name: "John",
      mail: "john@doe.d",
    }, {
      name: "Jane",
      mail: "jane@doe.d",
    }]);

    const groupsId = await collection.insertMany("group", [{
      name: "John",
      members: [usersId[0]],
    }, {
      name: "Jane",
      members: [usersId[1]],
    }]);

    {
      const count = await collection.find("user", { _id: groupsId[0] }).toArray();
      expect(count.length).toEqual(0);
    }

    {
      const count = await collection.find("group", { _id: usersId[0] }).toArray();
      expect(count.length).toEqual(0);
    }

    {
      const count = await collection.find("group", { _id: groupsId[0] }).toArray();
      expect(count.length).toEqual(1);
    }

    {
      const count = await collection.find("user", { _id: usersId[0] }).toArray();
      expect(count.length).toEqual(1);
    }

    {
      const count = await collection.find("group", { _id: "group-invalid:id" }).toArray();
      expect(count.length).toEqual(0);
    }

    const users = await collection.find("user", {}).toArray();
    expect(users.length).toEqual(2);

    const groups = await collection.find("group", {}).toArray();
    expect(groups.length).toEqual(2);
  });
});

test("DeleteId: Ensure delete correct type", async () => {
  await withDatabase("DeleteId: Ensure delete correct type", async (db) => {
    const collection = await multiCollection(db, "test", userGroupModel);

    const usersId = await collection.insertMany("user", [{
      name: "John",
      mail: "john@doe.d",
    }, {
      name: "Jane",
      mail: "jane@doe.d",
    }]);

    const groupsId = await collection.insertMany("group", [{
      name: "John",
      members: [usersId[0]],
    }, {
      name: "Jane",
      members: [usersId[1]],
    }]);

    await expect(async () => {
      await collection.deleteById("user", groupsId[0]);
    }).rejects.toThrow();

    await expect(async () => {
      await collection.deleteById("group", usersId[0]);
    }).rejects.toThrow();

    await expect(async () => {
      await collection.deleteById("group", "group-invalid:id");
    }).rejects.toThrow();

    const deleteUserB = await collection.deleteById("user", usersId[1]);
    expect(deleteUserB).toEqual(1);

    const deleteGroupB = await collection.deleteById("group", groupsId[1]);
    expect(deleteGroupB).toEqual(1);
  });
});

test("DeleteIds: Ensure delete correct type", async () => {
  await withDatabase("DeleteIds: Ensure delete correct type", async (db) => {
    const collection = await multiCollection(db, "test", userGroupModel);

    const usersId = await collection.insertMany("user", [{
      name: "John",
      mail: "john@doe.d",
    }, {
      name: "Jane",
      mail: "jane@doe.d",
    }]);

    const groupsId = await collection.insertMany("group", [{
      name: "John",
      members: [usersId[0]],
    }, {
      name: "Jane",
      members: [usersId[1]],
    }]);

    await expect(async () => {
      await collection.deleteByIds("user", groupsId);
    }).rejects.toThrow();

    await expect(async () => {
      await collection.deleteByIds("group", usersId);
    }).rejects.toThrow();

    await expect(async () => {
      await collection.deleteByIds("group", [usersId[0], groupsId[0]]);
    }).rejects.toThrow();

    await expect(async () => {
      await collection.deleteByIds("group", ["group-invalid:id"]);
    }).rejects.toThrow();

    const deleteUserB = await collection.deleteByIds("user", usersId.slice(1));
    expect(deleteUserB).toEqual(1);

    const deleteGroupB = await collection.deleteByIds("group", groupsId);
    expect(deleteGroupB).toEqual(2);
  });
});

test("Aggregate: lookup with string 'as' parameter", async () => {
  await withDatabase("Aggregate: lookup with string 'as' parameter", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
        },
        group: {
          name: v.string(),
          members: v.array(v.string()),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    const usersId = await collection.insertMany("user", [{
      name: "Alice",
      age: 25,
    }, {
      name: "Bob",
      age: 30,
    }]);

    await collection.insertMany("group", [{
      name: "Team A",
      members: [usersId[0], usersId[1]],
    }]);

    const results = await collection.aggregate((stage) => [
      stage.match("group", {}),
      stage.unwind("group", "members"),
      stage.lookup("user", "members", "_id", "userDetails"),
    ]);

    expect(results.length).toEqual(2);
    expect(results[0].userDetails).toBeTruthy();
  });
});

test("Aggregate: lookup with pipeline for filtering", async () => {
  await withDatabase("Aggregate: lookup with pipeline for filtering", async (db) => {
    const model = defineModel("test", {
      schema: {
        invitation: {
          type: v.string(),
          exhibitorId: v.string(),
        },
        visitor: {
          invitationId: v.string(),
          status: v.string(),
        },
        exhibitor: {
          company: v.string(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Create exhibitors
    const exhibitorId1 = await collection.insertOne("exhibitor", {
      company: "Tech Corp",
    });
    const exhibitorId2 = await collection.insertOne("exhibitor", {
      company: "Innovation Ltd",
    });

    // Create invitations
    const invitation1 = await collection.insertOne("invitation", {
      type: "exhibitor@visitors_invitation",
      exhibitorId: exhibitorId1,
    });
    const invitation2 = await collection.insertOne("invitation", {
      type: "exhibitor@visitors_invitation",
      exhibitorId: exhibitorId2,
    });

    // Create visitors (some accepted, some pending)
    await collection.insertMany("visitor", [
      { invitationId: invitation1, status: "accepted" },
      { invitationId: invitation1, status: "accepted" },
      { invitationId: invitation1, status: "pending" },
      { invitationId: invitation2, status: "accepted" },
    ]);

    // Aggregate: count accepted visitors per exhibitor
    const topExhibitors = await collection.aggregate((stage) => [
      // Match all invitations from exhibitors
      stage.match("invitation", {
        type: "exhibitor@visitors_invitation",
      }),
      // Lookup visitors with pipeline filter
      stage.lookup("visitor", "_id", "invitationId", {
        as: "visitors",
        pipeline: (stage) => [
          stage.match("visitor", { status: "accepted" }),
        ],
      }),
      // Add visitor count
      stage.addFields({
        visitorCount: { $size: "$visitors" },
      }),
      // Lookup exhibitor details
      stage.lookup("exhibitor", "exhibitorId", "_id", "exhibitorDetails"),
      // Unwind exhibitor array
      stage.unwind("exhibitor", "exhibitorDetails"),
      // Sort by visitor count
      stage.sort({ visitorCount: -1 }),
      // Project final format
      stage.project({
        _id: 1,
        exhibitorId: 1,
        company: "$exhibitorDetails.company",
        visitorCount: 1,
      }),
    ]);

    expect(topExhibitors.length).toEqual(2);
    expect(topExhibitors[0].company).toEqual("Tech Corp");
    expect(topExhibitors[0].visitorCount).toEqual(2);
    expect(topExhibitors[1].company).toEqual("Innovation Ltd");
    expect(topExhibitors[1].visitorCount).toEqual(1);
  });
});

test("Date fields: Insert and query with dates", async () => {
  await withDatabase("Date fields: Insert and query with dates", async (db) => {
    const model = defineModel("test", {
      schema: {
        event: {
          name: v.string(),
          startDate: v.date(),
          endDate: v.date(),
        },
        user: {
          name: v.string(),
          birthDate: v.date(),
          lastLogin: v.date(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    const now = new Date();
    const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);
    const birthDate = new Date("1990-05-15");

    const eventId = await collection.insertOne("event", {
      name: "Conference",
      startDate: now,
      endDate: tomorrow,
    });

    const _userId = await collection.insertOne("user", {
      name: "Alice",
      birthDate: birthDate,
      lastLogin: yesterday,
    });

    // Test finding by date
    const events = await collection.find("event", {
      startDate: { $lte: now },
    }).toArray();
    expect(events.length).toEqual(1);
    expect(events[0].name).toEqual("Conference");

    // Test finding users by date range
    const users = await collection.find("user", {
      lastLogin: { $gte: yesterday },
    }).toArray();
    expect(users.length).toEqual(1);
    expect(users[0].name).toEqual("Alice");

    // Verify date fields are preserved correctly
    const foundEvent = await collection.findOne("event", { _id: eventId });
    expect(foundEvent?.startDate.getTime()).toEqual(now.getTime());
    expect(foundEvent?.endDate.getTime()).toEqual(tomorrow.getTime());
  });
});

test("Date fields: Date comparisons and sorting", async () => {
  await withDatabase("Date fields: Date comparisons and sorting", async (db) => {
    const model = defineModel("test", {
      schema: {
        task: {
          title: v.string(),
          dueDate: v.date(),
          createdAt: v.date(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

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
      },
    ];

    await collection.insertMany("task", tasks);

    // Test date range queries
    const urgentTasks = await collection.find("task", {
      dueDate: {
        $lte: new Date(baseDate.getTime() + 2 * 24 * 60 * 60 * 1000),
      },
    }).toArray();
    expect(urgentTasks.length).toEqual(2);

    // Test finding tasks created after a specific date
    const recentTasks = await collection.find("task", {
      createdAt: { $gt: baseDate },
    }).toArray();
    expect(recentTasks.length).toEqual(2);

    // Test date between range
    const midRangeTasks = await collection.find("task", {
      dueDate: {
        $gte: new Date(baseDate.getTime() + 1 * 24 * 60 * 60 * 1000),
        $lte: new Date(baseDate.getTime() + 2 * 24 * 60 * 60 * 1000),
      },
    }).toArray();
    expect(midRangeTasks.length).toEqual(2);
  });
});

test("Date fields: Current date and date updates", async () => {
  await withDatabase("Date fields: Current date and date updates", async (db) => {
    const model = defineModel("test", {
      schema: {
        document: {
          title: v.string(),
          createdAt: v.date(),
          updatedAt: v.date(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    const startTime = new Date();

    const docId = await collection.insertOne("document", {
      title: "My Document",
      createdAt: startTime,
      updatedAt: startTime,
    });

    // Wait a bit to ensure different timestamps
    await new Promise((resolve) => setTimeout(resolve, 10));

    const updateTime = new Date();

    await collection.updateById("document", docId, {
      title: "Updated Document",
      updatedAt: updateTime,
    });

    const updatedDoc = await collection.findOne("document", { _id: docId });
    expect(updatedDoc).not.toBeNull();
    expect(updatedDoc?.title).toEqual("Updated Document");
    expect(updatedDoc?.createdAt.getTime()).toEqual(startTime.getTime());
    expect(updatedDoc?.updatedAt.getTime()).toEqual(updateTime.getTime());

    // Verify updatedAt is after createdAt
    expect(updatedDoc?.updatedAt >= updatedDoc?.createdAt).toBeTruthy();
  });
});

test("Date fields: Date edge cases", async () => {
  await withDatabase("Date fields: Date edge cases", async (db) => {
    const model = defineModel("test", {
      schema: {
        appointment: {
          title: v.string(),
          scheduledFor: v.date(),
          reminderDate: v.optional(v.date()),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Test with very old date
    const oldDate = new Date("1900-01-01");
    // Test with future date
    const futureDate = new Date("2030-12-31");

    const _appointment1 = await collection.insertOne("appointment", {
      title: "Historical Event",
      scheduledFor: oldDate,
    });

    const _appointment2 = await collection.insertOne("appointment", {
      title: "Future Meeting",
      scheduledFor: futureDate,
      reminderDate: new Date("2030-12-30"),
    });

    // Find appointments by date range
    const oldAppointments = await collection.find("appointment", {
      scheduledFor: { $lt: new Date("2000-01-01") },
    }).toArray();
    expect(oldAppointments.length).toEqual(1);
    expect(oldAppointments[0].title).toEqual("Historical Event");

    const futureAppointments = await collection.find("appointment", {
      scheduledFor: { $gt: new Date("2025-01-01") },
    }).toArray();
    expect(futureAppointments.length).toEqual(1);
    expect(futureAppointments[0].title).toEqual("Future Meeting");

    // Test optional date field
    const withReminder = await collection.find("appointment", {
      reminderDate: { $exists: true },
    }).toArray();
    expect(withReminder.length).toEqual(1);
  });
});

test("Literal id must have valid type", async () => {
  await withDatabase("Literal id must have valid type", async (db) => {
    const model = defineModel("test", {
      schema: {
        item: {
          _id: v.literal("item-id"),
          name: v.string(),
          createdAt: v.date(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert an item with a valid _id
    const itemId = await collection.insertOne("item", {
      _id: "item-id",
      name: "Test Item",
      createdAt: new Date(),
    });

    // Attempt to find the item using a literal id
    const foundItem = await collection.findOne("item", {
      _id: "item-id",
    });
    expect(foundItem?._id).toEqual(itemId);
  });
});

test("getById: retrieve by id and error cases", async () => {
  await withDatabase("getById: retrieve by id and error cases", async (db) => {
    const collection = await multiCollection(db, "test", userGroupModel);

    const userId = await collection.insertOne("user", {
      name: "John",
      mail: "john@doe.d",
    });

    const groupId = await collection.insertOne("group", {
      name: "Team",
      members: [userId],
    });

    // Successful retrieval
    const foundUser = await collection.getById("user", userId);
    expect(foundUser).toEqual({
      _id: userId,
      _type: "user",
      name: "John",
      mail: "john@doe.d",
    });

    // Trying to get a group id as a user should fail
    await expect(async () => {
      await collection.getById("user", groupId);
    }).rejects.toThrow();

    // Nonexistent id should also throw
    await expect(async () => {
      await collection.getById("user", "user:nonexistent-id");
    }).rejects.toThrow();
  });
});
