import { test, expect } from "vitest";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";

// Test schema for pagination
const userSchema = {
  name: v.string(),
  age: v.number(),
  email: v.string(),
  isActive: v.boolean(),
} as const;

test("Basic prepare → filter → format pipeline", async () => {
  await withDatabase("Basic prepare → filter → format pipeline", async (db) => {
    const users = await collection(db, "users", userSchema);

    // Insert test data
    await users.insertOne({
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await users.insertOne({
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });
    await users.insertOne({
      name: "Charlie",
      age: 35,
      email: "charlie@test.com",
      isActive: true,
    });

    const { data: results } = await users.paginate({}, {
      // Step 1: Prepare (enrich with computed field)
      prepare: async (user) => ({
        ...user,
        ageGroup: user.age < 30 ? "young" : "adult",
        emailDomain: user.email.split("@")[1],
      }),

      // Step 2: Filter (only active users)
      filter: (enrichedUser) => enrichedUser.isActive,

      // Step 3: Format (return simplified format)
      format: async (enrichedUser) => ({
        displayName: enrichedUser.name,
        category: enrichedUser.ageGroup,
        domain: enrichedUser.emailDomain,
      }),
    });

    expect(results.length === 2).toBeTruthy();
    expect(results[0].displayName === "Alice").toBeTruthy();
    expect(results[0].category === "young").toBeTruthy();
    expect(results[0].domain === "test.com").toBeTruthy();
    expect(
      results[1].displayName === "Charlie",
    ).toBeTruthy();
    expect(results[1].category === "adult").toBeTruthy();
  });
});

test("Only prepare stage (no filter/format)", async () => {
  await withDatabase("Only prepare stage (no filter/format)", async (db) => {
    const users = await collection(db, "users", userSchema);

    await users.insertOne({
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await users.insertOne({
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });

    const { data: results } = await users.paginate({}, {
      prepare: async (user) => ({
        ...user,
        ageGroup: user.age < 30 ? "young" : "adult",
        canVote: user.age >= 18,
      }),
    });

    expect(results.length === 2).toBeTruthy();
    expect(results[0].ageGroup === "young").toBeTruthy();
    expect(results[0].canVote === true).toBeTruthy();
    expect(results[1].ageGroup === "adult").toBeTruthy();
    expect(results[1].canVote === true).toBeTruthy();
  });
});

test("Only filter stage (no prepare/format)", async () => {
  await withDatabase("Only filter stage (no prepare/format)", async (db) => {
    const users = await collection(db, "users", userSchema);

    await users.insertOne({
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await users.insertOne({
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });
    await users.insertOne({
      name: "Charlie",
      age: 35,
      email: "charlie@test.com",
      isActive: true,
    });

    const { data: results } = await users.paginate({}, {
      filter: (user) => user.age >= 30,
    });

    expect(results.length === 2).toBeTruthy();
    expect(results[0].name === "Bob").toBeTruthy();
    expect(results[1].name === "Charlie").toBeTruthy();
  });
});

test("Only format stage (no prepare/filter)", async () => {
  await withDatabase("Only format stage (no prepare/filter)", async (db) => {
    const users = await collection(db, "users", userSchema);

    await users.insertOne({
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await users.insertOne({
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });

    const { data: results } = await users.paginate({}, {
      format: async (user) => ({
        id: user._id,
        fullName: user.name,
        contact: user.email,
      }),
    });

    expect(results.length === 2).toBeTruthy();
    expect(results[0].fullName === "Alice").toBeTruthy();
    expect(results[0].contact === "alice@test.com").toBeTruthy();
    expect(results[1].fullName === "Bob").toBeTruthy();
  });
});

test("Async external API simulation", async () => {
  await withDatabase("Async external API simulation", async (db) => {
    const users = await collection(db, "users", userSchema);

    await users.insertOne({
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await users.insertOne({
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });

    // Simulate external API calls
    const mockExternalAPI = {
      async getUserProfile(email: string) {
        // Simulate API delay
        await new Promise((resolve) => setTimeout(resolve, 10));
        return {
          reputation: email.includes("alice") ? 100 : 50,
          verified: email.includes("alice") ? true : false,
          badges: email.includes("alice") ? ["premium"] : ["basic"],
        };
      },

      async getPreferences(_userId: string) {
        await new Promise((resolve) => setTimeout(resolve, 5));
        return {
          theme: "dark",
          notifications: true,
          language: "en",
        };
      },
    };

    const { data: results } = await users.paginate({}, {
      // Step 1: Prepare - fetch external data
      prepare: async (user) => {
        const profile = await mockExternalAPI.getUserProfile(user.email);
        const preferences = await mockExternalAPI.getPreferences(
          user._id.toString(),
        );

        return {
          ...user,
          profile,
          preferences,
          enrichedAt: new Date(),
        };
      },

      // Step 2: Filter - only verified users
      filter: (enrichedUser) => enrichedUser.profile.verified,

      // Step 3: Format - create final API response
      format: async (enrichedUser) => ({
        user: {
          id: enrichedUser._id,
          name: enrichedUser.name,
          email: enrichedUser.email,
        },
        profile: {
          reputation: enrichedUser.profile.reputation,
          badges: enrichedUser.profile.badges,
        },
        settings: enrichedUser.preferences,
        meta: {
          enrichedAt: enrichedUser.enrichedAt,
        },
      }),
    });

    expect(results.length === 1).toBeTruthy();
    expect(results[0].user.name === "Alice").toBeTruthy();
    expect(results[0].profile.reputation === 100).toBeTruthy();
    expect(
      results[0].profile.badges.includes("premium"),
    ).toBeTruthy();
    expect(results[0].settings.theme === "dark").toBeTruthy();
    expect(
      results[0].meta.enrichedAt instanceof Date,
    ).toBeTruthy();
  });
});

test("Error handling in pipeline stages", async () => {
  await withDatabase("Error handling in pipeline stages", async (db) => {
    const users = await collection(db, "users", userSchema);

    await users.insertOne({
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await users.insertOne({
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });

    // Test error in prepare
    try {
      await users.paginate({}, {
        prepare: async (user) => {
          if (user.name === "Bob") {
            throw new Error("Simulated prepare error");
          }
          return { ...user, processed: true };
        },
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(
        (error as Error).message === "Simulated prepare error",
      ).toBeTruthy();
    }

    // Test error in filter
    try {
      await users.paginate({}, {
        filter: (user) => {
          if (user.name === "Bob") {
            throw new Error("Simulated filter error");
          }
          return true;
        },
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(
        (error as Error).message === "Simulated filter error",
      ).toBeTruthy();
    }

    // Test error in format
    try {
      await users.paginate({}, {
        format: async (user) => {
          if (user.name === "Bob") {
            throw new Error("Simulated format error");
          }
          return { processed: user.name };
        },
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(
        (error as Error).message === "Simulated format error",
      ).toBeTruthy();
    }
  });
});

test("Type safety verification", async () => {
  await withDatabase("Type safety verification", async (db) => {
    const users = await collection(db, "users", userSchema);

    await users.insertOne({
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });

    // Test type transformations
    const { data: results } = await users.paginate({}, {
      prepare: async (user) => {
        // user should be WithId<User>
        expect(typeof user.name === "string").toBeTruthy();
        expect(typeof user.age === "number").toBeTruthy();
        expect(typeof user._id !== "undefined").toBeTruthy();

        return {
          ...user,
          computedField: "computed",
        };
      },

      filter: (enrichedUser) => {
        // enrichedUser should have computedField
        expect(
          enrichedUser.computedField === "computed",
        ).toBeTruthy();
        return true;
      },

      format: async (enrichedUser) => {
        // enrichedUser should still have computedField
        expect(
          enrichedUser.computedField === "computed",
        ).toBeTruthy();

        return {
          finalField: enrichedUser.name,
        };
      },
    });

    expect(results.length === 1).toBeTruthy();
    expect(results[0].finalField === "Alice").toBeTruthy();
  });
});
