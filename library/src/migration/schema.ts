/**
 * MongoDBee Schema Validation System
 *
 * Validates that migrations match current schemas and detects schema-migration mismatches
 *
 * @module
 */

import type { MigrationRule } from "./types.ts";
import { Collection, Db, MongoClient } from "mongodb";

/**
 * Schema validation error details
 */
export interface SchemaValidationError {
  collection: string;
  field: string;
  expected: string;
  actual: string;
  severity: "error" | "warning";
}

/**
 * Schema validation result
 */
export interface SchemaValidationResult {
  valid: boolean;
  errors: SchemaValidationError[];
  warnings: SchemaValidationError[];
  collections: {
    name: string;
    exists: boolean;
    documentCount: number;
    sampleFields: Record<string, string>;
  }[];
}

/**
 * Schema field definition
 */
export interface SchemaField {
  type:
    | "string"
    | "number"
    | "boolean"
    | "date"
    | "object"
    | "array"
    | "objectId";
  required?: boolean;
  default?: unknown;
  validation?: {
    min?: number;
    max?: number;
    pattern?: string;
    enum?: unknown[];
  };
}

/**
 * Collection schema definition
 */
export interface CollectionSchema {
  collection: string;
  fields: Record<string, SchemaField>;
  indexes?: {
    fields: Record<string, 1 | -1>;
    options?: {
      unique?: boolean;
      sparse?: boolean;
      name?: string;
    };
  }[];
}

/**
 * Complete database schema
 */
export interface DatabaseSchema {
  collections: CollectionSchema[];
  version?: string;
  description?: string;
}

/**
 * Infers schema from a MongoDB collection by sampling documents
 */
export async function inferCollectionSchema(
  collection: Collection,
  sampleSize: number = 100,
): Promise<CollectionSchema> {
  const collectionName = collection.collectionName;
  const documents = await collection.aggregate([
    { $sample: { size: sampleSize } },
  ]).toArray();

  const fieldTypes: Record<string, Set<string>> = {};
  const fieldCounts: Record<string, number> = {};

  // Analyze sample documents
  for (const doc of documents) {
    analyzeDocument(doc, fieldTypes, fieldCounts, "");
  }

  // Build schema fields
  const fields: Record<string, SchemaField> = {};
  const totalDocs = documents.length;

  for (const [fieldPath, types] of Object.entries(fieldTypes)) {
    const count = fieldCounts[fieldPath] || 0;
    const mostCommonType = Array.from(types)[0]; // Simplified - could be more sophisticated

    fields[fieldPath] = {
      type: mostCommonType as SchemaField["type"],
      required: count / totalDocs > 0.9, // Required if present in >90% of documents
    };
  }

  // Get indexes
  const indexes = await collection.listIndexes().toArray();
  const schemaIndexes = indexes
    .filter((idx) => idx.name !== "_id_") // Skip default _id index
    .map((idx) => ({
      fields: idx.key,
      options: {
        unique: idx.unique || false,
        sparse: idx.sparse || false,
        name: idx.name,
      },
    }));

  return {
    collection: collectionName,
    fields,
    indexes: schemaIndexes,
  };
}

/**
 * Recursively analyze a document to extract field types
 */
function analyzeDocument(
  obj: unknown,
  fieldTypes: Record<string, Set<string>>,
  fieldCounts: Record<string, number>,
  prefix: string,
): void {
  if (obj === null || obj === undefined) return;

  if (typeof obj !== "object") {
    const type = getFieldType(obj);
    if (!fieldTypes[prefix]) fieldTypes[prefix] = new Set();
    fieldTypes[prefix].add(type);
    fieldCounts[prefix] = (fieldCounts[prefix] || 0) + 1;
    return;
  }

  if (Array.isArray(obj)) {
    const type = "array";
    if (!fieldTypes[prefix]) fieldTypes[prefix] = new Set();
    fieldTypes[prefix].add(type);
    fieldCounts[prefix] = (fieldCounts[prefix] || 0) + 1;

    // Analyze array elements (just first few for performance)
    for (let i = 0; i < Math.min(obj.length, 3); i++) {
      analyzeDocument(obj[i], fieldTypes, fieldCounts, `${prefix}[${i}]`);
    }
    return;
  }

  // Handle objects
  for (const [key, value] of Object.entries(obj)) {
    const fieldPath = prefix ? `${prefix}.${key}` : key;
    analyzeDocument(value, fieldTypes, fieldCounts, fieldPath);
  }
}

/**
 * Determine the MongoDB field type for a value
 */
function getFieldType(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (typeof value === "string") return "string";
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  if (value instanceof Date) return "date";
  if (Array.isArray(value)) return "array";
  if (
    value && typeof value === "object" && value.constructor?.name === "ObjectId"
  ) return "objectId";
  if (typeof value === "object") return "object";
  return "unknown";
}

/**
 * Validates that migration rules are compatible with the current database schema
 */
export async function validateMigrationAgainstSchema(
  migration: MigrationRule[],
  schema: DatabaseSchema,
): Promise<SchemaValidationResult> {
  const errors: SchemaValidationError[] = [];
  const warnings: SchemaValidationError[] = [];

  for (const rule of migration) {
    // Handle different types of migration rules
    if (rule.type === "create_collection") {
      const collectionSchema = schema.collections.find((c) =>
        c.collection === rule.collectionName
      );

      if (collectionSchema) {
        warnings.push({
          collection: rule.collectionName,
          field: "",
          expected: "Collection to not exist",
          actual: "Collection already exists",
          severity: "warning",
        });
      }
    }

    if (rule.type === "seed_collection") {
      const collectionSchema = schema.collections.find((c) =>
        c.collection === rule.collectionName
      );

      if (!collectionSchema) {
        errors.push({
          collection: rule.collectionName,
          field: "",
          expected: "Collection to exist for seeding",
          actual: "Collection not found",
          severity: "error",
        });
      } else if (rule.documents.length > 0) {
        // Validate document structure against schema
        const sampleDoc = rule.documents[0] as Record<string, unknown>;
        for (
          const [fieldName, fieldSchema] of Object.entries(
            collectionSchema.fields,
          )
        ) {
          if (fieldSchema.required && !(fieldName in sampleDoc)) {
            warnings.push({
              collection: rule.collectionName,
              field: fieldName,
              expected: `Required field of type ${fieldSchema.type}`,
              actual: "Field missing in seed data",
              severity: "warning",
            });
          }
        }
      }
    }

    if (rule.type === "transform_collection") {
      const collectionSchema = schema.collections.find((c) =>
        c.collection === rule.collectionName
      );

      if (!collectionSchema) {
        errors.push({
          collection: rule.collectionName,
          field: "",
          expected: "Collection to exist for transformation",
          actual: "Collection not found",
          severity: "error",
        });
      }
      // Note: We can't validate the actual transformation logic statically
      // This would require runtime analysis or more sophisticated type checking
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    collections: schema.collections.map((c) => ({
      name: c.collection,
      exists: true,
      documentCount: 0, // Would need to be passed in or queried
      sampleFields: Object.fromEntries(
        Object.entries(c.fields).slice(0, 5).map(([k, v]) => [k, v.type]),
      ),
    })),
  };
}

/**
 * Generates a complete database schema by connecting to MongoDB and sampling collections
 */
export async function generateDatabaseSchema(
  mongoUri: string,
  databaseName: string,
  sampleSize: number = 100,
): Promise<DatabaseSchema> {
  const client = new MongoClient(mongoUri);

  try {
    await client.connect();
    const db = client.db(databaseName);

    // Get all collections
    const collections = await db.listCollections().toArray();
    const schemaCollections: CollectionSchema[] = [];

    for (const collectionInfo of collections) {
      const collection = db.collection(collectionInfo.name);
      const schema = await inferCollectionSchema(collection, sampleSize);
      schemaCollections.push(schema);
    }

    return {
      collections: schemaCollections,
      version: new Date().toISOString(),
      description: `Generated schema for database: ${databaseName}`,
    };
  } finally {
    await client.close();
  }
}

/**
 * Compares two schemas and returns differences
 */
export function compareSchemas(
  currentSchema: DatabaseSchema,
  targetSchema: DatabaseSchema,
): SchemaValidationResult {
  const errors: SchemaValidationError[] = [];
  const warnings: SchemaValidationError[] = [];

  // Check for missing collections
  for (const targetCollection of targetSchema.collections) {
    const currentCollection = currentSchema.collections.find(
      (c) => c.collection === targetCollection.collection,
    );

    if (!currentCollection) {
      warnings.push({
        collection: targetCollection.collection,
        field: "",
        expected: "Collection to exist",
        actual: "Collection missing",
        severity: "warning",
      });
      continue;
    }

    // Check fields
    for (
      const [fieldName, targetField] of Object.entries(targetCollection.fields)
    ) {
      const currentField = currentCollection.fields[fieldName];

      if (!currentField) {
        if (targetField.required) {
          errors.push({
            collection: targetCollection.collection,
            field: fieldName,
            expected: `Required field of type ${targetField.type}`,
            actual: "Field missing",
            severity: "error",
          });
        } else {
          warnings.push({
            collection: targetCollection.collection,
            field: fieldName,
            expected: `Optional field of type ${targetField.type}`,
            actual: "Field missing",
            severity: "warning",
          });
        }
      } else if (currentField.type !== targetField.type) {
        errors.push({
          collection: targetCollection.collection,
          field: fieldName,
          expected: targetField.type,
          actual: currentField.type,
          severity: "error",
        });
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    collections: currentSchema.collections.map((c) => ({
      name: c.collection,
      exists: true,
      documentCount: 0,
      sampleFields: Object.fromEntries(
        Object.entries(c.fields).slice(0, 5).map(([k, v]) => [k, v.type]),
      ),
    })),
  };
}

/**
 * Utility to save schema to file
 */
export async function saveSchemaToFile(
  schema: DatabaseSchema,
  filePath: string,
): Promise<void> {
  const content = JSON.stringify(schema, null, 2);
  await Deno.writeTextFile(filePath, content);
}

/**
 * Utility to load schema from file
 */
export async function loadSchemaFromFile(
  filePath: string,
): Promise<DatabaseSchema> {
  const content = await Deno.readTextFile(filePath);
  return JSON.parse(content);
}
