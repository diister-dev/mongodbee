import { assert } from "jsr:@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import * as v from "../../src/schema.ts";

// Test schemas for multi-collection
const userSchema = {
    name: v.string(),
    age: v.number(),
    email: v.string(),
    isActive: v.boolean(),
} as const;

const productSchema = {
    name: v.string(),
    price: v.number(),
    category: v.string(),
    inStock: v.boolean(),
} as const;

const collectionSchema = {
    users: userSchema,
    products: productSchema,
} as const;

Deno.test("Multi-Collection: Basic prepare → filter → format", async (t) => {
    await withDatabase(t.name, async (db) => {
        const mc = await multiCollection(db, "multi_test", collectionSchema);
        
        // Insert test data
        await mc.insertOne("users", { name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await mc.insertOne("users", { name: "Bob", age: 30, email: "bob@test.com", isActive: false });
        await mc.insertOne("users", { name: "Charlie", age: 35, email: "charlie@test.com", isActive: true });

        const results = await mc.paginate("users", {}, {
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
                domain: enrichedUser.emailDomain,
                type: enrichedUser._type
            })
        });

        assert(results.data.length === 2, "Should return 2 active users");
        assert(results.data[0].displayName === "Alice", "First user should be Alice");
        assert(results.data[0].category === "young", "Alice should be young");
        assert(results.data[0].domain === "test.com", "Domain should be test.com");
        assert(results.data[0].type === "users", "Type should be users");
        assert(results.data[1].displayName === "Charlie", "Second user should be Charlie");
        assert(results.data[1].category === "adult", "Charlie should be adult");
    });
});

Deno.test("Multi-Collection: Products with pricing logic", async (t) => {
    await withDatabase(t.name, async (db) => {
        const mc = await multiCollection(db, "multi_test", collectionSchema);
        
        // Insert test products
        await mc.insertOne("products", { name: "Laptop", price: 999, category: "electronics", inStock: true });
        await mc.insertOne("products", { name: "Mouse", price: 25, category: "electronics", inStock: false });
        await mc.insertOne("products", { name: "Book", price: 15, category: "books", inStock: true });

        const results = await mc.paginate("products", {}, {
            // Step 1: Prepare (enrich with pricing tiers)
            prepare: async (product) => ({
                ...product,
                priceRange: product.price < 50 ? "budget" : product.price < 500 ? "mid" : "premium",
                discountEligible: product.price > 100 && product.inStock
            }),
            
            // Step 2: Filter (only in-stock products)
            filter: (enrichedProduct) => enrichedProduct.inStock,
            
            // Step 3: Format (create catalog format)
            format: async (enrichedProduct) => ({
                productName: enrichedProduct.name,
                displayPrice: `$${enrichedProduct.price}`,
                tier: enrichedProduct.priceRange,
                canDiscount: enrichedProduct.discountEligible,
                categoryTag: enrichedProduct.category.toUpperCase()
            })
        });

        assert(results.data.length === 2, "Should return 2 in-stock products");
        assert(results.data[0].productName === "Laptop", "First product should be Laptop");
        assert(results.data[0].tier === "premium", "Laptop should be premium");
        assert(results.data[0].canDiscount === true, "Laptop should be discount eligible");
        assert(results.data[1].productName === "Book", "Second product should be Book");
        assert(results.data[1].tier === "budget", "Book should be budget");
        assert(results.data[1].canDiscount === false, "Book should not be discount eligible");
    });
});

Deno.test("Multi-Collection: Cross-type isolation", async (t) => {
    await withDatabase(t.name, async (db) => {
        const mc = await multiCollection(db, "multi_test", collectionSchema);
        
        // Insert mixed data
        await mc.insertOne("users", { name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await mc.insertOne("products", { name: "Laptop", price: 999, category: "electronics", inStock: true });
        await mc.insertOne("users", { name: "Bob", age: 30, email: "bob@test.com", isActive: false });

        const userResults = await mc.paginate("users", {}, {
            prepare: async (user) => ({
                ...user,
                type: "user-record"
            }),
            format: async (enrichedUser) => ({
                name: enrichedUser.name,
                recordType: enrichedUser.type
            })
        });

        const productResults = await mc.paginate("products", {}, {
            prepare: async (product) => ({
                ...product,
                type: "product-record"
            }),
            format: async (enrichedProduct) => ({
                name: enrichedProduct.name,
                recordType: enrichedProduct.type
            })
        });

        assert(userResults.data.length === 2, "Should return 2 users");
        assert(userResults.data.every(r => r.recordType === "user-record"), "All should be user records");
        assert(productResults.data.length === 1, "Should return 1 product");
        assert(productResults.data.every(r => r.recordType === "product-record"), "All should be product records");
    });
});

Deno.test("Multi-Collection: External API enrichment", async (t) => {
    await withDatabase(t.name, async (db) => {
        const mc = await multiCollection(db, "multi_test", collectionSchema);
        
        await mc.insertOne("users", { name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await mc.insertOne("products", { name: "Laptop", price: 999, category: "electronics", inStock: true });

        // Mock external services
        const mockServices = {
            async getUserReputation(email: string) {
                await new Promise(resolve => setTimeout(resolve, 5));
                return email.includes("alice") ? 95 : 50;
            },
            
            async getProductReviews(productName: string) {
                await new Promise(resolve => setTimeout(resolve, 10));
                return productName === "Laptop" ? { rating: 4.5, count: 142 } : { rating: 3.0, count: 5 };
            }
        };

        const userResults = await mc.paginate("users", {}, {
            prepare: async (user) => {
                const reputation = await mockServices.getUserReputation(user.email);
                return {
                    ...user,
                    reputation,
                    trustLevel: reputation > 80 ? "high" : "medium"
                };
            },
            
            filter: (enrichedUser) => enrichedUser.isActive,
            
            format: async (enrichedUser) => ({
                userName: enrichedUser.name,
                trust: enrichedUser.trustLevel,
                score: enrichedUser.reputation
            })
        });

        const productResults = await mc.paginate("products", {}, {
            prepare: async (product) => {
                const reviews = await mockServices.getProductReviews(product.name);
                return {
                    ...product,
                    reviews,
                    isPopular: reviews.count > 100
                };
            },
            
            filter: (enrichedProduct) => enrichedProduct.inStock,
            
            format: async (enrichedProduct) => ({
                productName: enrichedProduct.name,
                rating: enrichedProduct.reviews.rating,
                popularity: enrichedProduct.isPopular ? "popular" : "niche"
            })
        });

        assert(userResults.data.length === 1, "Should return 1 active user");
        assert(userResults.data[0].trust === "high", "Alice should have high trust");
        assert(userResults.data[0].score === 95, "Alice should have score 95");
        
        assert(productResults.data.length === 1, "Should return 1 in-stock product");
        assert(productResults.data[0].rating === 4.5, "Laptop should have 4.5 rating");
        assert(productResults.data[0].popularity === "popular", "Laptop should be popular");
    });
});

Deno.test("Multi-Collection: Type safety with generics", async (t) => {
    await withDatabase(t.name, async (db) => {
        const mc = await multiCollection(db, "multi_test", collectionSchema);
        
        await mc.insertOne("users", { name: "Alice", age: 25, email: "alice@test.com", isActive: true });

        // Test type transformations maintain type safety
        const results = await mc.paginate("users", {}, {
            prepare: async (user) => {
                // user should be User with _id and _type
                assert(typeof user.name === "string", "Should have name");
                assert(typeof user.age === "number", "Should have age");
                assert(typeof user._id === "string", "Should have string _id");
                assert(user._type === "users", "Should have correct _type");
                
                return {
                    ...user,
                    enrichedField: "test-value"
                };
            },
            
            filter: (enrichedUser) => {
                // enrichedUser should have enrichedField
                assert(enrichedUser.enrichedField === "test-value", "Should have enriched field");
                return true;
            },
            
            format: async (enrichedUser) => {
                // enrichedUser should still have all fields
                assert(enrichedUser.name === "Alice", "Should have original name");
                assert(enrichedUser.enrichedField === "test-value", "Should have enriched field");
                
                return {
                    finalName: enrichedUser.name,
                    finalValue: enrichedUser.enrichedField
                };
            }
        });

        assert(results.data.length === 1, "Should return 1 result");
        assert(results.data[0].finalName === "Alice", "Should have final name");
        assert(results.data[0].finalValue === "test-value", "Should have final value");
    });
});

Deno.test("Multi-Collection: Error handling", async (t) => {
    await withDatabase(t.name, async (db) => {
        const mc = await multiCollection(db, "multi_test", collectionSchema);
        
        await mc.insertOne("users", { name: "Alice", age: 25, email: "alice@test.com", isActive: true });
        await mc.insertOne("users", { name: "Bob", age: 30, email: "bob@test.com", isActive: false });

        // Test error in prepare
        try {
            await mc.paginate("users", {}, {
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
            await mc.paginate("users", {}, {
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
            await mc.paginate("users", {}, {
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
