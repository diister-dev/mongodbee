import { assertEquals, assertExists } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import * as v from "../../src/schema.ts";
import { defineModel } from "../../src/multi-collection-model.ts";
import { ObjectId } from "mongodb";

/**
 * Tests for paginate with pipeline (lookup) support
 * This tests the ability to perform server-side lookups during pagination
 */

// Schema for registration system with polymorphic references
const registrationModel = defineModel("registration", {
  schema: {
    collaborator: {
      name: v.string(),
      email: v.string(),
      role: v.string(),
    },
    visitor: {
      name: v.string(),
      email: v.string(),
      company: v.string(),
    },
    registration: {
      eventName: v.string(),
      registeredBy: v.optional(v.string()), // "collaborator:xxx" or "visitor:xxx"
      status: v.string(),
    },
  },
});

Deno.test("Paginate with lookup: Basic lookup to same collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create collaborators
    const collab1 = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });
    const collab2 = await mc.insertOne("collaborator", {
      name: "Bob Manager",
      email: "bob@company.com",
      role: "manager",
    });

    // Create registrations linked to collaborators
    await mc.insertOne("registration", {
      eventName: "Conference 2024",
      registeredBy: collab1,
      status: "confirmed",
    });
    await mc.insertOne("registration", {
      eventName: "Workshop AI",
      registeredBy: collab2,
      status: "pending",
    });
    await mc.insertOne("registration", {
      eventName: "Meetup JS",
      registeredBy: collab1,
      status: "confirmed",
    });

    // Paginate registrations with lookup to get collaborator details
    const results = await mc.paginate("registration", {}, {
      limit: 10,
      pipeline: (stage) => [
        stage.lookup("collaborator", "registeredBy", "_id", "collaboratorDetails"),
      ],
      format: (doc) => ({
        event: doc.eventName,
        status: doc.status,
        // deno-lint-ignore no-explicit-any
        registeredByName: (doc as any).collaboratorDetails?.[0]?.name || "Unknown",
      }),
    });

    assertEquals(results.data.length, 3);
    assertEquals(results.total, 3);
    
    // Check that lookups worked
    const aliceEvents = results.data.filter(r => r.registeredByName === "Alice Admin");
    assertEquals(aliceEvents.length, 2);
    
    const bobEvents = results.data.filter(r => r.registeredByName === "Bob Manager");
    assertEquals(bobEvents.length, 1);
  });
});

Deno.test("Paginate with lookup: Polymorphic lookup (collaborator OR visitor)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create a collaborator
    const collabId = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });

    // Create a visitor
    const visitorId = await mc.insertOne("visitor", {
      name: "John Guest",
      email: "john@external.com",
      company: "External Corp",
    });

    // Create registrations - some by collaborator, some by visitor
    await mc.insertOne("registration", {
      eventName: "Conference 2024",
      registeredBy: collabId,
      status: "confirmed",
    });
    await mc.insertOne("registration", {
      eventName: "Workshop AI",
      registeredBy: visitorId,
      status: "pending",
    });

    // Paginate with multiple lookups for polymorphic references
    const results = await mc.paginate("registration", {}, {
      limit: 10,
      pipeline: (stage) => [
        // Lookup from collaborators
        stage.lookup("collaborator", "registeredBy", "_id", "collaboratorDocs"),
        // Lookup from visitors
        stage.lookup("visitor", "registeredBy", "_id", "visitorDocs"),
      ],
      format: (doc) => {
        // deno-lint-ignore no-explicit-any
        const anyDoc = doc as any;
        const collaborator = anyDoc.collaboratorDocs?.[0];
        const visitor = anyDoc.visitorDocs?.[0];
        
        return {
          event: doc.eventName,
          status: doc.status,
          registeredByType: collaborator ? "collaborator" : visitor ? "visitor" : "unknown",
          registeredByName: collaborator?.name || visitor?.name || "Unknown",
          registeredByEmail: collaborator?.email || visitor?.email || null,
        };
      },
    });

    assertEquals(results.data.length, 2);
    
    // Find registration by collaborator
    const collabReg = results.data.find(r => r.registeredByType === "collaborator");
    assertExists(collabReg);
    assertEquals(collabReg.registeredByName, "Alice Admin");
    assertEquals(collabReg.event, "Conference 2024");
    
    // Find registration by visitor
    const visitorReg = results.data.find(r => r.registeredByType === "visitor");
    assertExists(visitorReg);
    assertEquals(visitorReg.registeredByName, "John Guest");
    assertEquals(visitorReg.event, "Workshop AI");
  });
});

Deno.test("Paginate with anyLookup: Polymorphic lookup without type constraint", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create a collaborator
    const collabId = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });

    // Create a visitor
    const visitorId = await mc.insertOne("visitor", {
      name: "John Guest",
      email: "john@external.com",
      company: "External Corp",
    });

    // Create registrations - some by collaborator, some by visitor
    await mc.insertOne("registration", {
      eventName: "Conference 2024",
      registeredBy: collabId,
      status: "confirmed",
    });
    await mc.insertOne("registration", {
      eventName: "Workshop AI",
      registeredBy: visitorId,
      status: "pending",
    });

    // Use anyLookup - single lookup that matches ANY type by ID
    const results = await mc.paginate("registration", {}, {
      limit: 10,
      pipeline: (stage) => [
        // Single anyLookup that works for both collaborator and visitor IDs
        stage.anyLookup("registeredBy", "_id", "registrant"),
      ],
      format: (doc) => {
        // deno-lint-ignore no-explicit-any
        const anyDoc = doc as any;
        const registrant = anyDoc.registrant?.[0];
        
        return {
          event: doc.eventName,
          status: doc.status,
          // The _type field tells us what type was matched
          registeredByType: registrant?._type || "unknown",
          registeredByName: registrant?.name || "Unknown",
          registeredByEmail: registrant?.email || null,
        };
      },
    });

    assertEquals(results.data.length, 2);
    
    // Find registration by collaborator
    const collabReg = results.data.find(r => r.registeredByType === "collaborator");
    assertExists(collabReg);
    assertEquals(collabReg.registeredByName, "Alice Admin");
    assertEquals(collabReg.event, "Conference 2024");
    
    // Find registration by visitor
    const visitorReg = results.data.find(r => r.registeredByType === "visitor");
    assertExists(visitorReg);
    assertEquals(visitorReg.registeredByName, "John Guest");
    assertEquals(visitorReg.event, "Workshop AI");
  });
});

Deno.test("Paginate with lookup: Using addFields for computed values", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create collaborators
    const collab1 = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });

    // Create multiple registrations
    await mc.insertMany("registration", [
      { eventName: "Event A", registeredBy: collab1, status: "confirmed" },
      { eventName: "Event B", registeredBy: collab1, status: "pending" },
      { eventName: "Event C", registeredBy: undefined, status: "draft" },
    ]);

    // Use addFields to compute values before lookup
    const results = await mc.paginate("registration", {}, {
      limit: 10,
      pipeline: (stage) => [
        // Add computed field - check if registeredBy exists and is not null/empty
        stage.addFields({
          hasRegistrant: { 
            $and: [
              { $ne: ["$registeredBy", null] },
              { $ne: [{ $ifNull: ["$registeredBy", ""] }, ""] }
            ]
          },
          statusUpper: { $toUpper: "$status" },
        }),
        // Lookup collaborator
        stage.lookup("collaborator", "registeredBy", "_id", "collaboratorDocs"),
      ],
      format: (doc) => {
        // deno-lint-ignore no-explicit-any
        const anyDoc = doc as any;
        return {
          event: doc.eventName,
          status: anyDoc.statusUpper,
          hasRegistrant: anyDoc.hasRegistrant,
          registrantName: anyDoc.collaboratorDocs?.[0]?.name || null,
        };
      },
    });

    assertEquals(results.data.length, 3);
    
    // Check computed fields
    const eventA = results.data.find(r => r.event === "Event A");
    assertExists(eventA);
    assertEquals(eventA.status, "CONFIRMED");
    assertEquals(eventA.hasRegistrant, true);
    assertEquals(eventA.registrantName, "Alice Admin");
    
    const eventC = results.data.find(r => r.event === "Event C");
    assertExists(eventC);
    assertEquals(eventC.status, "DRAFT");
    assertEquals(eventC.hasRegistrant, false);
    assertEquals(eventC.registrantName, null);
  });
});

Deno.test("Paginate with lookup: Combined with MongoDB filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create collaborators with different roles
    const admin = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });
    const user = await mc.insertOne("collaborator", {
      name: "Bob User",
      email: "bob@company.com",
      role: "user",
    });

    // Create registrations with different statuses
    await mc.insertMany("registration", [
      { eventName: "Event 1", registeredBy: admin, status: "confirmed" },
      { eventName: "Event 2", registeredBy: admin, status: "pending" },
      { eventName: "Event 3", registeredBy: user, status: "confirmed" },
      { eventName: "Event 4", registeredBy: user, status: "cancelled" },
    ]);

    // Paginate only confirmed registrations with lookup
    const results = await mc.paginate("registration", { status: "confirmed" }, {
      limit: 10,
      pipeline: (stage) => [
        stage.lookup("collaborator", "registeredBy", "_id", "collaboratorDocs"),
      ],
      format: (doc) => ({
        event: doc.eventName,
        // deno-lint-ignore no-explicit-any
        registrantName: (doc as any).collaboratorDocs?.[0]?.name || null,
      }),
    });

    assertEquals(results.data.length, 2);
    assertEquals(results.total, 2); // Total should only count confirmed
    
    // Both confirmed events should be returned
    const events = results.data.map(r => r.event).sort();
    assertEquals(events, ["Event 1", "Event 3"]);
  });
});

Deno.test("Paginate with lookup: With cursor pagination (afterId)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create a collaborator
    const collab = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });

    // Create 10 registrations
    const regIds: string[] = [];
    for (let i = 1; i <= 10; i++) {
      const id = await mc.insertOne("registration", {
        eventName: `Event ${i}`,
        registeredBy: collab,
        status: i % 2 === 0 ? "confirmed" : "pending",
      });
      regIds.push(id);
    }

    // First page with lookup
    const page1 = await mc.paginate("registration", {}, {
      limit: 3,
      pipeline: (stage) => [
        stage.lookup("collaborator", "registeredBy", "_id", "collaboratorDocs"),
      ],
      format: (doc) => ({
        id: doc._id,
        event: doc.eventName,
        // deno-lint-ignore no-explicit-any
        registrant: (doc as any).collaboratorDocs?.[0]?.name,
      }),
    });

    assertEquals(page1.data.length, 3);
    assertEquals(page1.total, 10);
    assertEquals(page1.position, 0);

    // Second page using afterId
    const page2 = await mc.paginate("registration", {}, {
      limit: 3,
      afterId: page1.data[page1.data.length - 1].id,
      pipeline: (stage) => [
        stage.lookup("collaborator", "registeredBy", "_id", "collaboratorDocs"),
      ],
      format: (doc) => ({
        id: doc._id,
        event: doc.eventName,
        // deno-lint-ignore no-explicit-any
        registrant: (doc as any).collaboratorDocs?.[0]?.name,
      }),
    });

    assertEquals(page2.data.length, 3);
    assertEquals(page2.total, 10);
    assertEquals(page2.position, 3);

    // Verify no overlap between pages
    const page1Ids = new Set(page1.data.map(r => r.id));
    const page2Ids = new Set(page2.data.map(r => r.id));
    for (const id of page2Ids) {
      assertEquals(page1Ids.has(id), false, "Pages should not overlap");
    }

    // All should have lookup data
    for (const reg of [...page1.data, ...page2.data]) {
      assertEquals(reg.registrant, "Alice Admin");
    }
  });
});

Deno.test("Paginate with lookup: Combined with prepare/filter/format pipeline", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create collaborators with different roles
    const admin = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });
    const user = await mc.insertOne("collaborator", {
      name: "Bob User",
      email: "bob@company.com",
      role: "user",
    });

    // Create registrations
    await mc.insertMany("registration", [
      { eventName: "Important Event", registeredBy: admin, status: "confirmed" },
      { eventName: "Regular Event", registeredBy: user, status: "confirmed" },
      { eventName: "Another Event", registeredBy: admin, status: "confirmed" },
    ]);

    // Use both pipeline (server-side lookup) AND prepare/filter (client-side processing)
    const results = await mc.paginate("registration", {}, {
      limit: 10,
      // Server-side: MongoDB lookup
      pipeline: (stage) => [
        stage.lookup("collaborator", "registeredBy", "_id", "collaboratorDocs"),
      ],
      // Client-side: Enrich with computed data
      prepare: (doc) => {
        // deno-lint-ignore no-explicit-any
        const anyDoc = doc as any;
        const collaborator = anyDoc.collaboratorDocs?.[0];
        return {
          ...doc,
          isAdminRegistration: collaborator?.role === "admin",
          collaborator,
        };
      },
      // Client-side: Filter only admin registrations
      filter: (enriched) => enriched.isAdminRegistration,
      // Client-side: Format final output
      format: (enriched) => ({
        event: enriched.eventName,
        registrantName: enriched.collaborator?.name,
        registrantRole: enriched.collaborator?.role,
      }),
    });

    // Should only return registrations by admin
    assertEquals(results.data.length, 2);
    
    for (const reg of results.data) {
      assertEquals(reg.registrantRole, "admin");
      assertEquals(reg.registrantName, "Alice Admin");
    }
  });
});

Deno.test("Paginate with lookup: Lookup with nested pipeline filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create collaborators with different roles
    const admin = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });
    const user = await mc.insertOne("collaborator", {
      name: "Bob User",
      email: "bob@company.com",
      role: "user",
    });

    // Create registrations
    await mc.insertMany("registration", [
      { eventName: "Event 1", registeredBy: admin, status: "confirmed" },
      { eventName: "Event 2", registeredBy: user, status: "confirmed" },
    ]);

    // Use lookup with nested pipeline to filter only admins
    const results = await mc.paginate("registration", {}, {
      limit: 10,
      pipeline: (stage) => [
        stage.lookup("collaborator", "registeredBy", "_id", {
          as: "adminCollaborators",
          pipeline: (nestedStage) => [
            nestedStage.match("collaborator", { role: "admin" }),
          ],
        }),
      ],
      format: (doc) => {
        // deno-lint-ignore no-explicit-any
        const anyDoc = doc as any;
        return {
          event: doc.eventName,
          adminRegistrant: anyDoc.adminCollaborators?.[0]?.name || null,
          hasAdminRegistrant: (anyDoc.adminCollaborators?.length || 0) > 0,
        };
      },
    });

    assertEquals(results.data.length, 2);
    
    // Event 1 should have admin registrant
    const event1 = results.data.find(r => r.event === "Event 1");
    assertExists(event1);
    assertEquals(event1.adminRegistrant, "Alice Admin");
    assertEquals(event1.hasAdminRegistrant, true);
    
    // Event 2 should NOT have admin registrant (Bob is user)
    const event2 = results.data.find(r => r.event === "Event 2");
    assertExists(event2);
    assertEquals(event2.adminRegistrant, null);
    assertEquals(event2.hasAdminRegistrant, false);
  });
});

Deno.test("Paginate without pipeline: Backwards compatibility", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    await mc.insertMany("registration", [
      { eventName: "Event 1", registeredBy: undefined, status: "confirmed" },
      { eventName: "Event 2", registeredBy: undefined, status: "pending" },
      { eventName: "Event 3", registeredBy: undefined, status: "confirmed" },
    ]);

    // Standard paginate without pipeline should still work
    const results = await mc.paginate("registration", { status: "confirmed" }, {
      limit: 10,
      format: (doc) => ({
        event: doc.eventName,
        status: doc.status,
      }),
    });

    assertEquals(results.data.length, 2);
    assertEquals(results.total, 2);
    
    for (const reg of results.data) {
      assertEquals(reg.status, "confirmed");
    }
  });
});

Deno.test("Paginate with lookup: Project to reduce data transfer", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    const collab = await mc.insertOne("collaborator", {
      name: "Alice Admin",
      email: "alice@company.com",
      role: "admin",
    });

    await mc.insertOne("registration", {
      eventName: "Event 1",
      registeredBy: collab,
      status: "confirmed",
    });

    // Use project to limit fields returned
    const results = await mc.paginate("registration", {}, {
      limit: 10,
      pipeline: (stage) => [
        stage.lookup("collaborator", "registeredBy", "_id", "collaboratorDocs"),
        stage.project({
          _id: 1,
          _type: 1,
          eventName: 1,
          status: 1,
          registrantName: { $arrayElemAt: ["$collaboratorDocs.name", 0] },
          // Don't include collaboratorDocs array
        }),
      ],
      format: (doc) => {
        // deno-lint-ignore no-explicit-any
        const anyDoc = doc as any;
        return {
          event: doc.eventName,
          registrant: anyDoc.registrantName,
        };
      },
    });

    assertEquals(results.data.length, 1);
    assertEquals(results.data[0].event, "Event 1");
    assertEquals(results.data[0].registrant, "Alice Admin");
  });
});

Deno.test("Paginate with externalLookup: Join with external collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registration", registrationModel);

    // Create an external collection for "events" (not part of multi-collection)
    const eventsCollection = db.collection("external_events");
    await eventsCollection.insertMany([
      { _id: "evt1" as unknown as ObjectId, title: "Tech Conference 2024", location: "Paris", capacity: 500 },
      { _id: "evt2" as unknown as ObjectId, title: "AI Workshop", location: "London", capacity: 50 },
    ]);

    // Create registrations that reference external events
    await mc.insertOne("registration", {
      eventName: "evt1", // References external event
      registeredBy: undefined,
      status: "confirmed",
    });
    await mc.insertOne("registration", {
      eventName: "evt2",
      registeredBy: undefined,
      status: "pending",
    });

    // Use externalLookup to join with external collection
    const results = await mc.paginate("registration", {}, {
      limit: 10,
      pipeline: (stage) => [
        stage.externalLookup("external_events", "eventName", "_id", "eventDetails"),
      ],
      format: (doc) => {
        // deno-lint-ignore no-explicit-any
        const anyDoc = doc as any;
        const event = anyDoc.eventDetails?.[0];
        return {
          eventId: doc.eventName,
          status: doc.status,
          eventTitle: event?.title || "Unknown",
          eventLocation: event?.location || null,
          eventCapacity: event?.capacity || 0,
        };
      },
    });

    assertEquals(results.data.length, 2);
    
    const conf = results.data.find(r => r.eventId === "evt1");
    assertExists(conf);
    assertEquals(conf.eventTitle, "Tech Conference 2024");
    assertEquals(conf.eventLocation, "Paris");
    assertEquals(conf.eventCapacity, 500);
    
    const workshop = results.data.find(r => r.eventId === "evt2");
    assertExists(workshop);
    assertEquals(workshop.eventTitle, "AI Workshop");
    assertEquals(workshop.eventLocation, "London");
  });
});

Deno.test("Paginate with externalLookup: Join with another multi-collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create first multi-collection for registrations
    const registrations = await multiCollection(db, "registrations", registrationModel);

    // Create second multi-collection for a different domain
    const venueModel = defineModel("venue", {
      schema: {
        venue: {
          name: v.string(),
          city: v.string(),
          capacity: v.number(),
        },
        room: {
          name: v.string(),
          venueId: v.string(),
          floor: v.number(),
        },
      },
    });
    const venues = await multiCollection(db, "venues", venueModel);

    // Create venues
    const venue1 = await venues.insertOne("venue", {
      name: "Convention Center",
      city: "Paris",
      capacity: 5000,
    });
    const venue2 = await venues.insertOne("venue", {
      name: "Tech Hub",
      city: "London",
      capacity: 200,
    });

    // Create registrations that reference venues from another multi-collection
    await registrations.insertOne("registration", {
      eventName: "Conference",
      registeredBy: venue1, // Store venue ID as registeredBy for this test
      status: "confirmed",
    });
    await registrations.insertOne("registration", {
      eventName: "Workshop",
      registeredBy: venue2,
      status: "pending",
    });

    // Use externalLookup to join with another multi-collection
    const results = await registrations.paginate("registration", {}, {
      limit: 10,
      pipeline: (stage) => [
        // Join with the "venues" multi-collection
        stage.externalLookup("venues", "registeredBy", "_id", "venueDetails"),
      ],
      format: (doc) => {
        // deno-lint-ignore no-explicit-any
        const anyDoc = doc as any;
        const venue = anyDoc.venueDetails?.[0];
        return {
          event: doc.eventName,
          status: doc.status,
          venueName: venue?.name || "Unknown",
          venueCity: venue?.city || null,
          venueType: venue?._type || null, // Should be "venue"
        };
      },
    });

    assertEquals(results.data.length, 2);
    
    const conf = results.data.find(r => r.event === "Conference");
    assertExists(conf);
    assertEquals(conf.venueName, "Convention Center");
    assertEquals(conf.venueCity, "Paris");
    assertEquals(conf.venueType, "venue");
    
    const workshop = results.data.find(r => r.event === "Workshop");
    assertExists(workshop);
    assertEquals(workshop.venueName, "Tech Hub");
    assertEquals(workshop.venueCity, "London");
  });
});
