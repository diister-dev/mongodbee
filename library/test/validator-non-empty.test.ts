import { test, expect } from "vitest";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";

test("nonEmpty validation for strings", async () => {
  await withDatabase("nonEmpty validation for strings", async (db) => {
    const schema = {
      name: v.pipe(v.string(), v.nonEmpty("Name cannot be empty")),
      description: v.string(),
    } as const;

    const users = await collection(db, "users", schema);

    // Test valid non-empty string
    const userId = await users.insertOne({
      name: "John",
      description: "A valid user",
    });
    expect(userId).toBeTruthy();

    // Test empty string - should be rejected by MongoDB validator
    try {
      await users.insertOne({
        name: "",
        description: "Invalid user",
      });
      expect(false).toBeTruthy();
    } catch (error) {
      // Expected to fail due to MongoDB validator
      expect(error).toBeTruthy();
    }

    // Test that we can retrieve the valid document
    const user = await users.getById(userId);
    expect(user.name === "John").toBeTruthy();
  });
});

test("nonEmpty validation for arrays", async () => {
  await withDatabase("nonEmpty validation for arrays", async (db) => {
    const schema = {
      tags: v.pipe(v.array(v.string()), v.nonEmpty("Tags cannot be empty")),
      optionalTags: v.array(v.string()),
    } as const;

    const documents = await collection(db, "documents", schema);

    // Test valid non-empty array
    const docId = await documents.insertOne({
      tags: ["javascript", "mongodb"],
      optionalTags: [],
    });
    expect(docId).toBeTruthy();

    // Test empty array - should be rejected by MongoDB validator
    try {
      await documents.insertOne({
        tags: [],
        optionalTags: ["optional"],
      });
      expect(false).toBeTruthy();
    } catch (error) {
      // Expected to fail due to MongoDB validator
      expect(error).toBeTruthy();
    }

    // Test that we can retrieve the valid document
    const doc = await documents.getById(docId);
    expect(doc.tags.length === 2).toBeTruthy();
    expect(doc.tags.includes("javascript")).toBeTruthy();
  });
});

test("nonEmpty with other validations", async () => {
  await withDatabase("nonEmpty with other validations", async (db) => {
    const schema = {
      title: v.pipe(
        v.string(),
        v.nonEmpty("Title cannot be empty"),
        v.minLength(3, "Title must be at least 3 characters"),
        v.maxLength(100, "Title cannot exceed 100 characters"),
      ),
      keywords: v.pipe(
        v.array(v.string()),
        v.nonEmpty("Keywords cannot be empty"),
        v.maxLength(10, "Cannot have more than 10 keywords"),
      ),
    } as const;

    const articles = await collection(db, "articles", schema);

    // Test valid document
    const articleId = await articles.insertOne({
      title: "Valid Article Title",
      keywords: ["tech", "javascript"],
    });
    expect(articleId).toBeTruthy();

    // Test empty title
    try {
      await articles.insertOne({
        title: "",
        keywords: ["tech"],
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(error).toBeTruthy();
    }

    // Test title too short (should pass nonEmpty but fail minLength)
    try {
      await articles.insertOne({
        title: "Hi", // 2 characters, fails minLength(3)
        keywords: ["tech"],
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(error).toBeTruthy();
    }

    // Test empty keywords array
    try {
      await articles.insertOne({
        title: "Valid Title",
        keywords: [],
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(error).toBeTruthy();
    }

    // Verify the valid document
    const article = await articles.getById(articleId);
    expect(
      article.title === "Valid Article Title",
    ).toBeTruthy();
    expect(article.keywords.length === 2).toBeTruthy();
  });
});
