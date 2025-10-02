import { assert } from "@std/assert";
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

Deno.test("Basic prepare → filter → format pipeline", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        // Insert test data
        await users.insertOne({ name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await users.insertOne({ name: "Bob", age: 30, email: "bob@test.com", isActive: false });
        await users.insertOne({ name: "Charlie", age: 35, email: "charlie@test.com", isActive: true });

        const { data: results} = await users.paginate({}, {
            // Step 1: Prepare (enrich with computed field)
            prepare: async (user) => ({
                ...user,
                ageGroup: user.age < 30 ? "young" : "adult",
                emailDomain: user.email.split("@")[1]
            }),
            
            // Step 2: Filter (only active users)
            filter: (enrichedUser) => enrichedUser.isActive,
            
            // Step 3: Format (return simplified format)
            format: async (enrichedUser) => ({
                displayName: enrichedUser.name,
                category: enrichedUser.ageGroup,
                domain: enrichedUser.emailDomain
            })
        });

        assert(results.length === 2, "Should return 2 active users");
        assert(results[0].displayName === "Alice", "First user should be Alice");
        assert(results[0].category === "young", "Alice should be young");
        assert(results[0].domain === "test.com", "Domain should be test.com");
        assert(results[1].displayName === "Charlie", "Second user should be Charlie");
        assert(results[1].category === "adult", "Charlie should be adult");
    });
});

Deno.test("Only prepare stage (no filter/format)", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        await users.insertOne({ name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await users.insertOne({ name: "Bob", age: 30, email: "bob@test.com", isActive: false });

        const { data: results } = await users.paginate({}, {
            prepare: async (user) => ({
                ...user,
                ageGroup: user.age < 30 ? "young" : "adult",
                canVote: user.age >= 18
            })
        });

        assert(results.length === 2, "Should return all users");
        assert(results[0].ageGroup === "young", "Alice should be young");
        assert(results[0].canVote === true, "Alice can vote");
        assert(results[1].ageGroup === "adult", "Bob should be adult");
        assert(results[1].canVote === true, "Bob can vote");
    });
});

Deno.test("Only filter stage (no prepare/format)", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        await users.insertOne({ name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await users.insertOne({ name: "Bob", age: 30, email: "bob@test.com", isActive: false });
        await users.insertOne({ name: "Charlie", age: 35, email: "charlie@test.com", isActive: true });

        const {data: results} = await users.paginate({}, {
            filter: (user) => user.age >= 30
        });

        assert(results.length === 2, "Should return 2 users >= 30");
        assert(results[0].name === "Bob", "First should be Bob");
        assert(results[1].name === "Charlie", "Second should be Charlie");
    });
});

Deno.test("Only format stage (no prepare/filter)", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        await users.insertOne({ name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await users.insertOne({ name: "Bob", age: 30, email: "bob@test.com", isActive: false });

        const {data: results} = await users.paginate({}, {
            format: async (user) => ({
                id: user._id,
                fullName: user.name,
                contact: user.email
            })
        });

        assert(results.length === 2, "Should return all users");
        assert(results[0].fullName === "Alice", "First should be Alice");
        assert(results[0].contact === "alice@test.com", "Should have email");
        assert(results[1].fullName === "Bob", "Second should be Bob");
    });
});

Deno.test("Async external API simulation", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        await users.insertOne({ name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await users.insertOne({ name: "Bob", age: 30, email: "bob@test.com", isActive: false });

        // Simulate external API calls
        const mockExternalAPI = {
            async getUserProfile(email: string) {
                // Simulate API delay
                await new Promise(resolve => setTimeout(resolve, 10));
                return {
                    reputation: email.includes("alice") ? 100 : 50,
                    verified: email.includes("alice") ? true : false,
                    badges: email.includes("alice") ? ["premium"] : ["basic"]
                };
            },
            
            async getPreferences(_userId: string) {
                await new Promise(resolve => setTimeout(resolve, 5));
                return {
                    theme: "dark",
                    notifications: true,
                    language: "en"
                };
            }
        };

        const {data: results} = await users.paginate({}, {
            // Step 1: Prepare - fetch external data
            prepare: async (user) => {
                const profile = await mockExternalAPI.getUserProfile(user.email);
                const preferences = await mockExternalAPI.getPreferences(user._id.toString());
                
                return {
                    ...user,
                    profile,
                    preferences,
                    enrichedAt: new Date()
                };
            },
            
            // Step 2: Filter - only verified users
            filter: (enrichedUser) => enrichedUser.profile.verified,
            
            // Step 3: Format - create final API response
            format: async (enrichedUser) => ({
                user: {
                    id: enrichedUser._id,
                    name: enrichedUser.name,
                    email: enrichedUser.email
                },
                profile: {
                    reputation: enrichedUser.profile.reputation,
                    badges: enrichedUser.profile.badges
                },
                settings: enrichedUser.preferences,
                meta: {
                    enrichedAt: enrichedUser.enrichedAt
                }
            })
        });

        assert(results.length === 1, "Should return 1 verified user");
        assert(results[0].user.name === "Alice", "Should be Alice");
        assert(results[0].profile.reputation === 100, "Should have reputation");
        assert(results[0].profile.badges.includes("premium"), "Should have premium badge");
        assert(results[0].settings.theme === "dark", "Should have preferences");
        assert(results[0].meta.enrichedAt instanceof Date, "Should have enrichment timestamp");
    });
});

Deno.test("Error handling in pipeline stages", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        await users.insertOne({ name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await users.insertOne({ name: "Bob", age: 30, email: "bob@test.com", isActive: false });

        // Test error in prepare
        try {
            await users.paginate({}, {
                prepare: async (user) => {
                    if (user.name === "Bob") {
                        throw new Error("Simulated prepare error");
                    }
                    return { ...user, processed: true };
                }
            });
            assert(false, "Should have thrown error");
        } catch (error) {
            assert((error as Error).message === "Simulated prepare error", "Should catch prepare error");
        }

        // Test error in filter
        try {
            await users.paginate({}, {
                filter: (user) => {
                    if (user.name === "Bob") {
                        throw new Error("Simulated filter error");
                    }
                    return true;
                }
            });
            assert(false, "Should have thrown error");
        } catch (error) {
            assert((error as Error).message === "Simulated filter error", "Should catch filter error");
        }

        // Test error in format
        try {
            await users.paginate({}, {
                format: async (user) => {
                    if (user.name === "Bob") {
                        throw new Error("Simulated format error");
                    }
                    return { processed: user.name };
                }
            });
            assert(false, "Should have thrown error");
        } catch (error) {
            assert((error as Error).message === "Simulated format error", "Should catch format error");
        }
    });
});

Deno.test("Type safety verification", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        await users.insertOne({ name: "Alice", age: 25, email: "alice@test.com", isActive: true });

        // Test type transformations
        const {data: results} = await users.paginate({}, {
            prepare: async (user) => {
                // user should be WithId<User>
                assert(typeof user.name === "string", "Should have name");
                assert(typeof user.age === "number", "Should have age");
                assert(typeof user._id !== "undefined", "Should have _id");
                
                return {
                    ...user,
                    computedField: "computed"
                };
            },
            
            filter: (enrichedUser) => {
                // enrichedUser should have computedField
                assert(enrichedUser.computedField === "computed", "Should have computed field");
                return true;
            },
            
            format: async (enrichedUser) => {
                // enrichedUser should still have computedField
                assert(enrichedUser.computedField === "computed", "Should still have computed field");
                
                return {
                    finalField: enrichedUser.name
                };
            }
        });

        assert(results.length === 1, "Should return 1 result");
        assert(results[0].finalField === "Alice", "Should have final field");
    });
});
