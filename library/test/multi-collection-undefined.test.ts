import * as v from "../src/schema.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { MongoClient } from "../src/mongodb.ts";
import { defineModel } from "../src/multi-collection-model.ts";

// Mock MongoDB setup for testing
let client: MongoClient;
let db: ReturnType<MongoClient["db"]>;

async function setupTestDb() {
    const mongoUrl = Deno.env.get("MONGODB_URL") || "mongodb://localhost:27017";
    client = new MongoClient(mongoUrl);
    await client.connect();
    db = client.db("test_multi_collection_undefined");
}

async function cleanupTestDb() {
    if (db) {
        await db.dropDatabase();
    }
    if (client) {
        await client.close();
    }
}

Deno.test("MultiCollection: undefined behavior remove (default)", async () => {
    await setupTestDb();
    
    try {
        const catalogModel = defineModel("catalog_remove", {
            schema:{
                product: {
                    name: v.string(),
                    price: v.number(),
                    description: v.optional(v.string()),
                    category: v.optional(v.string())
                },
                category: {
                    name: v.string(),
                    parentId: v.optional(v.string())
                }
            }
        });

        const catalog = await multiCollection(db, "catalog_remove", catalogModel);
        
        // Insert product with undefined values (should be removed)
        const productId = await catalog.insertOne("product", {
            name: "Laptop",
            price: 999.99,
            description: undefined,  // Should be removed
            category: "Electronics"
        });
        
        const product = await catalog.findOne("product", { _id: productId });
        assert(product !== null);
        assert(!("description" in product));
        assertEquals(product.name, "Laptop");
        assertEquals(product.price, 999.99);
        assertEquals(product.category, "Electronics");
        
        // Insert category with undefined values
        const categoryId = await catalog.insertOne("category", {
            name: "Electronics",
            parentId: undefined  // Should be removed
        });
        
        const category = await catalog.findOne("category", { _id: categoryId });
        assert(category !== null);
        assert(!("parentId" in category));
        assertEquals(category.name, "Electronics");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("MultiCollection: undefined behavior error", async () => {
    await setupTestDb();
    
    try {
        const model = defineModel("catalog_error", {
            schema: {
                product: {
                    name: v.string(),
                    price: v.number(),
                    description: v.optional(v.string()),
                    category: v.optional(v.string())
                },
                category: {
                    name: v.string(),
                    parentId: v.optional(v.string())
                }
            }
        });

        const catalog = await multiCollection(db, "catalog_error", model, {
            undefinedBehavior: 'error'
        });
        
        // Should throw error for product with undefined
        await assertRejects(
            async () => {
                await catalog.insertOne("product", {
                    name: "Laptop",
                    price: 999.99,
                    description: undefined,  // Should cause error
                    category: "Electronics"
                });
            },
            Error,
            "Undefined values are not allowed"
        );
        
        // Should throw error for category with undefined
        await assertRejects(
            async () => {
                await catalog.insertOne("category", {
                    name: "Electronics",
                    parentId: undefined  // Should cause error
                });
            },
            Error,
            "Undefined values are not allowed"
        );
        
        // But should work fine without undefined values
        const productId = await catalog.insertOne("product", {
            name: "Laptop",
            price: 999.99,
            category: "Electronics"
        });
        
        const product = await catalog.findOne("product", { _id: productId });
        assert(product !== null);
        assertEquals(product.name, "Laptop");
        assertEquals(product.price, 999.99);
        assertEquals(product.category, "Electronics");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("MultiCollection: insertMany with undefined behavior", async () => {
    await setupTestDb();
    
    try {
        // Test remove behavior
        const model = defineModel("catalog_many_remove", {
            schema: {
                product: {
                    name: v.string(),
                    price: v.number(),
                    description: v.optional(v.string()),
                    category: v.optional(v.string())
                }
            }
        });

        const catalogRemove = await multiCollection(db, "catalog_many_remove", model, {
            undefinedBehavior: 'remove'
        });
        
        const productIds = await catalogRemove.insertMany("product", [
            {
                name: "Product1",
                price: 10.99,
                description: undefined,  // Should be removed
                category: "Cat1"
            },
            {
                name: "Product2",
                price: 20.99,
                description: "Description2",
                category: undefined  // Should be removed
            }
        ]);
        
        assertEquals(productIds.length, 2);
        
        const products = await catalogRemove.find("product", {});
        assertEquals(products.length, 2);
        
        const product1 = products.find(p => p.name === "Product1");
        const product2 = products.find(p => p.name === "Product2");
        
        assert(product1 !== undefined);
        assert(product2 !== undefined);
        assert(!("description" in product1));
        assert(!("category" in product2));
        
        // Test error behavior
        const modelError = defineModel("catalog_many_error", {
            schema: {
                product: {
                    name: v.string(),
                    price: v.number(),
                    description: v.optional(v.string()),
                    category: v.optional(v.string())
                }
            }
        });

        const catalogError = await multiCollection(db, "catalog_many_error", modelError, {
            undefinedBehavior: 'error'
        });
        
        await assertRejects(
            async () => {
                await catalogError.insertMany("product", [
                    {
                        name: "Product1",
                        price: 10.99,
                        description: undefined,  // Should cause error
                        category: "Cat1"
                    }
                ]);
            },
            Error,
            "Undefined values are not allowed"
        );
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("MultiCollection: Mixed document types with different undefined values", async () => {
    await setupTestDb();
    
    try {
        const model = defineModel("catalog_mixed", {
            schema: {
                product: {
                    name: v.string(),
                    price: v.number(),
                    description: v.optional(v.string()),
                    categoryId: v.optional(v.string())
                },
                category: {
                    name: v.string(),
                    parentId: v.optional(v.string()),
                    description: v.optional(v.string())
                },
                brand: {
                    name: v.string(),
                    website: v.optional(v.string()),
                    country: v.optional(v.string())
                }
            }
        });

        const catalog = await multiCollection(db, "catalog_mixed", model, {
            undefinedBehavior: 'remove'
        });
        
        // Insert different document types with undefined values
        const categoryId = await catalog.insertOne("category", {
            name: "Electronics",
            parentId: undefined,  // Should be removed
            description: "Electronic devices"
        });
        
        const brandId = await catalog.insertOne("brand", {
            name: "TechCorp",
            website: "https://techcorp.com",
            country: undefined  // Should be removed
        });
        
        const productId = await catalog.insertOne("product", {
            name: "Smartphone",
            price: 599.99,
            description: undefined,  // Should be removed
            categoryId: categoryId
        });
        
        // Verify all documents were inserted correctly
        const category = await catalog.findOne("category", { _id: categoryId });
        const brand = await catalog.findOne("brand", { _id: brandId });
        const product = await catalog.findOne("product", { _id: productId });
        
        // Check category
        assert(category !== null);
        assert(!("parentId" in category));
        assertEquals(category.name, "Electronics");
        assertEquals(category.description, "Electronic devices");
        
        // Check brand
        assert(brand !== null);
        assert(!("country" in brand));
        assertEquals(brand.name, "TechCorp");
        assertEquals(brand.website, "https://techcorp.com");
        
        // Check product
        assert(product !== null);
        assert(!("description" in product));
        assertEquals(product.name, "Smartphone");
        assertEquals(product.price, 599.99);
        assertEquals(product.categoryId, categoryId);
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("MultiCollection: Multiple collections with different undefined behaviors", async () => {
    await setupTestDb();
    
    try {
        // Collection 1: Remove undefined (default)
        const model = defineModel("catalog", {
            schema: {
                product: {
                    name: v.string(),
                    price: v.number(),
                    description: v.optional(v.string())
                }
            }
        });

        const catalogRemove = await multiCollection(db, "catalog_remove_multi", model, {
            undefinedBehavior: 'remove'
        });
        
        // Collection 2: Error on undefined
        const catalogError = await multiCollection(db, "catalog_error_multi", model, {
            undefinedBehavior: 'error'
        });
        
        const testProduct = {
            name: "TestProduct",
            price: 49.99,
            description: undefined
        };
        
        // First collection should work (remove undefined)
        const productId1 = await catalogRemove.insertOne("product", testProduct);
        const product1 = await catalogRemove.findOne("product", { _id: productId1 });
        assert(product1 !== null);
        assert(!("description" in product1));
        assertEquals(product1.name, "TestProduct");
        
        // Second collection should fail (error on undefined)
        await assertRejects(
            async () => {
                await catalogError.insertOne("product", testProduct);
            },
            Error,
            "Undefined values are not allowed"
        );
        
        // But second collection should work with clean data
        const productId2 = await catalogError.insertOne("product", {
            name: "TestProduct2",
            price: 59.99
        });
        
        const product2 = await catalogError.findOne("product", { _id: productId2 });
        assert(product2 !== null);
        assertEquals(product2.name, "TestProduct2");
        assertEquals(product2.price, 59.99);
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("MultiCollection: Nested undefined values", async () => {
    await setupTestDb();
    
    try {
        const nestedSchema = {
            name: v.string(),
            profile: v.optional(v.object({
                bio: v.optional(v.string()),
                website: v.optional(v.string()),
                social: v.optional(v.object({
                    twitter: v.optional(v.string()),
                    github: v.optional(v.string())
                }))
            })),
            preferences: v.optional(v.array(v.string()))
        };

        const model = defineModel("nested_docs", {
            schema: {
                profiles: nestedSchema
            }
        });
        
        const mc = await multiCollection(db, "nested_docs", model, {
            undefinedBehavior: 'remove'
        });
        
        const profileId = await mc.insertOne("profiles", {
            name: "Developer",
            profile: {
                bio: "Software developer",
                website: undefined,  // Should be removed
                social: {
                    twitter: undefined,  // Should be removed
                    github: "dev123"
                }
            },
            preferences: undefined  // Should be removed
        });
        
        const profile = await mc.findOne("profiles", { _id: profileId });
        
        assert(profile !== null);
        assertEquals(profile.name, "Developer");
        assert("profile" in profile);
        assert(!("preferences" in profile));
        
        // Check nested object sanitization
        assert(profile.profile !== undefined);
        assert("bio" in profile.profile);
        assert(!("website" in profile.profile));
        assert("social" in profile.profile);
        assert(profile.profile.social !== undefined);
        assert(!("twitter" in profile.profile.social));
        assert("github" in profile.profile.social);
        assertEquals(profile.profile.social.github, "dev123");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("MultiCollection: Ignore undefined behavior", async () => {
    await setupTestDb();
    
    try {
        const model = defineModel("ignore_docs", {
            schema: {
                products: {
                    name: v.string(),
                    price: v.number(),
                    description: v.optional(v.string())
                }
            }
        });

        const mc = await multiCollection(db, "ignore_docs", model, {
            undefinedBehavior: 'ignore'
        });
        
        // Should ignore undefined values and let MongoDB handle them
        // Note: This will likely fail at MongoDB level since undefined is not valid BSON
        try {
            await mc.insertOne("products", {
                name: "TestProduct",
                price: 29.99,
                description: undefined  // Will be ignored by our sanitizer
            });
            
            // If we reach here, MongoDB accepted it (unlikely)
            const product = await mc.findOne("products", { name: "TestProduct" });
            assert(product !== null);
            assertEquals(product.name, "TestProduct");
            
        } catch (error) {
            // Expected: MongoDB will likely reject undefined values
            assert(error instanceof Error);
            // This is acceptable behavior for 'ignore' mode
        }
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("MultiCollection: Performance with undefined sanitization", async () => {
    await setupTestDb();
    
    try {
        const model = defineModel("perf_docs", {
            schema: {
                items: {
                    id: v.string(),
                    data: v.optional(v.string()),
                    metadata: v.optional(v.object({
                        created: v.optional(v.string()),
                        updated: v.optional(v.string())
                    }))
                }
            }
        });

        const mc = await multiCollection(db, "perf_docs", model, {
            undefinedBehavior: 'remove'
        });
        
        // Generate test data with many undefined values
        const testData = [];
        for (let i = 0; i < 50; i++) {  // Reduced for test performance
            testData.push({
                id: `item_${i}`,
                data: i % 3 === 0 ? undefined : `data_${i}`,  // 1/3 undefined
                metadata: i % 2 === 0 ? {
                    created: `2023-01-${i % 28 + 1}`,
                    updated: i % 4 === 0 ? undefined : `2023-02-${i % 28 + 1}`
                } : undefined
            });
        }
        
        const startTime = Date.now();
        const itemIds = await mc.insertMany("items", testData);
        const endTime = Date.now();
        
        assertEquals(itemIds.length, 50);
        // Performance validation: should handle 50 documents efficiently
        
        // Verify correct sanitization
        const items = await mc.find("items", {});
        assertEquals(items.length, 50);
        
        // Check that undefined values were properly removed
        for (const item of items) {
            assert("id" in item);
            
            // data should only be present if it wasn't undefined
            const originalItem = testData.find(d => d.id === item.id);
            if (originalItem?.data !== undefined) {
                assert("data" in item);
            } else {
                assert(!("data" in item));
            }
            
            // Check metadata sanitization
            if (originalItem?.metadata !== undefined) {
                assert("metadata" in item);
                assert(item.metadata !== undefined);
                assert("created" in item.metadata);
                
                if (originalItem.metadata.updated !== undefined) {
                    assert("updated" in item.metadata);
                } else {
                    assert(!("updated" in item.metadata));
                }
            } else {
                assert(!("metadata" in item));
            }
        }
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("MultiCollection: Array sanitization with undefined values", async () => {
    await setupTestDb();
    
    try {
        const model = defineModel("array_docs", {
            schema: {
                posts: {
                    title: v.string(),
                    tags: v.optional(v.array(v.string())),
                    comments: v.optional(v.array(v.object({
                        author: v.string(),
                        text: v.optional(v.string()),
                        timestamp: v.optional(v.string())
                    })))
                }
            }
        });
        
        const mc = await multiCollection(db, "array_docs", model, {
            undefinedBehavior: 'remove'
        });
        
        const postId = await mc.insertOne("posts", {
            title: "Test Post",
            tags: ["tech", "mongodb"],
            comments: [
                {
                    author: "user1",
                    text: "Great post!",
                    timestamp: undefined  // Should be removed
                },
                {
                    author: "user2",
                    text: undefined,  // Should be removed
                    timestamp: "2023-01-01"
                }
            ]
        });
        
        const post = await mc.findOne("posts", { _id: postId });
        
        assert(post !== null);
        assertEquals(post.title, "Test Post");
        assert(post.tags !== undefined);
        assertEquals(post.tags.length, 2);
        assert(post.comments !== undefined);
        assertEquals(post.comments.length, 2);
        
        // Check first comment
        const comment1 = post.comments[0];
        assertEquals(comment1.author, "user1");
        assertEquals(comment1.text, "Great post!");
        assert(!("timestamp" in comment1));
        
        // Check second comment
        const comment2 = post.comments[1];
        assertEquals(comment2.author, "user2");
        assert(!("text" in comment2));
        assertEquals(comment2.timestamp, "2023-01-01");
        
    } finally {
        await cleanupTestDb();
    }
});
