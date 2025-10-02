import { assert } from "@std/assert";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";

Deno.test("nonEmpty validation for strings", async (t) => {
    await withDatabase(t.name, async (db) => {
        const schema = {
            name: v.pipe(v.string(), v.nonEmpty("Name cannot be empty")),
            description: v.string(),
        } as const;

        const users = await collection(db, "users", schema);
        
        // Test valid non-empty string
        const userId = await users.insertOne({ 
            name: "John", 
            description: "A valid user"
        });
        assert(userId, "Should insert valid document");

        // Test empty string - should be rejected by MongoDB validator
        try {
            await users.insertOne({ 
                name: "", 
                description: "Invalid user"
            });
            assert(false, "Should have failed validation for empty string");
        } catch (error) {
            // Expected to fail due to MongoDB validator
            assert(error, "Should throw validation error");
        }

        // Test that we can retrieve the valid document
        const user = await users.getById(userId);
        assert(user.name === "John", "Should retrieve the valid user");
    });
});

Deno.test("nonEmpty validation for arrays", async (t) => {
    await withDatabase(t.name, async (db) => {
        const schema = {
            tags: v.pipe(v.array(v.string()), v.nonEmpty("Tags cannot be empty")),
            optionalTags: v.array(v.string()),
        } as const;

        const documents = await collection(db, "documents", schema);
        
        // Test valid non-empty array
        const docId = await documents.insertOne({ 
            tags: ["javascript", "mongodb"], 
            optionalTags: []
        });
        assert(docId, "Should insert valid document");

        // Test empty array - should be rejected by MongoDB validator
        try {
            await documents.insertOne({ 
                tags: [], 
                optionalTags: ["optional"]
            });
            assert(false, "Should have failed validation for empty array");
        } catch (error) {
            // Expected to fail due to MongoDB validator
            assert(error, "Should throw validation error");
        }

        // Test that we can retrieve the valid document
        const doc = await documents.getById(docId);
        assert(doc.tags.length === 2, "Should retrieve the valid document");
        assert(doc.tags.includes("javascript"), "Should have javascript tag");
    });
});

Deno.test("nonEmpty with other validations", async (t) => {
    await withDatabase(t.name, async (db) => {
        const schema = {
            title: v.pipe(
                v.string(), 
                v.nonEmpty("Title cannot be empty"),
                v.minLength(3, "Title must be at least 3 characters"),
                v.maxLength(100, "Title cannot exceed 100 characters")
            ),
            keywords: v.pipe(
                v.array(v.string()), 
                v.nonEmpty("Keywords cannot be empty"),
                v.maxLength(10, "Cannot have more than 10 keywords")
            ),
        } as const;

        const articles = await collection(db, "articles", schema);
        
        // Test valid document
        const articleId = await articles.insertOne({ 
            title: "Valid Article Title", 
            keywords: ["tech", "javascript"]
        });
        assert(articleId, "Should insert valid document");

        // Test empty title
        try {
            await articles.insertOne({ 
                title: "", 
                keywords: ["tech"]
            });
            assert(false, "Should have failed for empty title");
        } catch (error) {
            assert(error, "Should throw validation error for empty title");
        }

        // Test title too short (should pass nonEmpty but fail minLength)
        try {
            await articles.insertOne({ 
                title: "Hi", // 2 characters, fails minLength(3)
                keywords: ["tech"]
            });
            assert(false, "Should have failed for title too short");
        } catch (error) {
            assert(error, "Should throw validation error for short title");
        }

        // Test empty keywords array
        try {
            await articles.insertOne({ 
                title: "Valid Title", 
                keywords: []
            });
            assert(false, "Should have failed for empty keywords");
        } catch (error) {
            assert(error, "Should throw validation error for empty keywords");
        }

        // Verify the valid document
        const article = await articles.getById(articleId);
        assert(article.title === "Valid Article Title", "Should have correct title");
        assert(article.keywords.length === 2, "Should have 2 keywords");
    });
});
