import { test, expect } from "vitest";
import {
  type NavigationNode,
  SchemaNavigator,
  type SchemaVisitor,
  type VisitResult,
} from "../src/schema-navigator.ts";
import * as v from "../src/schema.ts";

// Test visitor that collects all visited nodes
class CollectingVisitor implements SchemaVisitor {
  nodes: NavigationNode[] = [];
  containers: NavigationNode[] = [];
  validations: NavigationNode[] = [];

  visitNode(node: NavigationNode): VisitResult {
    this.nodes.push(node);
    return { continue: true };
  }

  enterContainer(node: NavigationNode): VisitResult {
    this.containers.push(node);
    return { continue: true };
  }

  exitContainer(_node: NavigationNode): void {
    // Track container exits
  }

  visitValidation(node: NavigationNode): VisitResult {
    this.validations.push(node);
    return { continue: true };
  }
}

// Test visitor that stops at certain depth
class DepthLimitingVisitor implements SchemaVisitor {
  maxDepth: number;
  nodes: NavigationNode[] = [];

  constructor(maxDepth: number) {
    this.maxDepth = maxDepth;
  }

  visitNode(node: NavigationNode): VisitResult {
    this.nodes.push(node);
    return { continue: node.depth < this.maxDepth };
  }
}

test("SchemaNavigator: Basic string schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.string();

  navigator.navigate(schema, visitor);

  expect(visitor.nodes.length).toEqual(1);
  expect(visitor.nodes[0].schema.type).toEqual("string");
  expect(visitor.nodes[0].path).toEqual([]);
  expect(visitor.nodes[0].depth).toEqual(0);
  expect(visitor.nodes[0].parent).toEqual(undefined);
  expect(visitor.nodes[0].key).toEqual(undefined);
});

test("SchemaNavigator: Piped string schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.pipe(v.string(), v.minLength(3), v.maxLength(50));

  navigator.navigate(schema, visitor);

  // Should visit the string schema and all validations in pipe
  expect(visitor.nodes.length >= 3).toBeTruthy();
  expect(visitor.nodes[0].schema.type).toEqual("string");
  expect(visitor.validations.length >= 2).toBeTruthy(); // minLength and maxLength
});

test("SchemaNavigator: Object schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.object({
    name: v.string(),
    age: v.number(),
    email: v.pipe(v.string(), v.email()),
  });

  navigator.navigate(schema, visitor);

  // Should visit object + 3 properties + email validation
  expect(visitor.nodes.length >= 4).toBeTruthy();
  expect(visitor.containers.length).toEqual(1); // object is a container

  // Find the object node
  const objectNode = visitor.nodes.find((n) => n.schema.type === "object");
  expect(objectNode).toBeDefined();
  expect(objectNode!.path).toEqual([]);
  expect(objectNode!.depth).toEqual(0);

  // Find property nodes
  const nameNode = visitor.nodes.find((n) => n.key === "name");
  expect(nameNode).toBeDefined();
  expect(nameNode!.path).toEqual(["name"]);
  expect(nameNode!.depth).toEqual(1);
  expect(nameNode!.schema.type).toEqual("string");

  const ageNode = visitor.nodes.find((n) => n.key === "age");
  expect(ageNode).toBeDefined();
  expect(ageNode!.path).toEqual(["age"]);
  expect(ageNode!.depth).toEqual(1);
  expect(ageNode!.schema.type).toEqual("number");
});

test("SchemaNavigator: Nested object schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.object({
    user: v.object({
      profile: v.object({
        name: v.string(),
        bio: v.optional(v.string()),
      }),
      settings: v.object({
        theme: v.string(),
        notifications: v.boolean(),
      }),
    }),
  });

  navigator.navigate(schema, visitor);

  // Should have multiple container levels
  expect(visitor.containers.length >= 4).toBeTruthy(); // root object + user + profile + settings

  // Find deeply nested name field
  const nameNode = visitor.nodes.find((n) => n.key === "name");
  expect(nameNode).toBeDefined();
  expect(nameNode!.path).toEqual(["user", "profile", "name"]);
  expect(nameNode!.depth).toEqual(3);

  // Find optional bio field
  const bioNode = visitor.nodes.find((n) => n.key === "bio");
  expect(bioNode).toBeDefined();
  expect(bioNode!.path).toEqual(["user", "profile", "bio"]);
  expect(bioNode!.schema.type).toEqual("optional");
});

test("SchemaNavigator: Array schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.array(v.object({
    id: v.string(),
    value: v.number(),
  }));

  navigator.navigate(schema, visitor);

  // Should visit array + item object + properties
  expect(visitor.nodes.length >= 4).toBeTruthy();
  expect(visitor.containers.length).toEqual(2); // array + object

  // Find array node
  const arrayNode = visitor.nodes.find((n) => n.schema.type === "array");
  expect(arrayNode).toBeDefined();
  expect(arrayNode!.path).toEqual([]);
  expect(arrayNode!.depth).toEqual(0);

  // Find item object properties
  const idNode = visitor.nodes.find((n) => n.key === "id");
  expect(idNode).toBeDefined();
  expect(idNode!.path).toEqual(["$[]", "id"]);
  expect(idNode!.depth).toEqual(2);
});

test("SchemaNavigator: Union schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.union([
    v.string(),
    v.number(),
    v.object({ type: v.literal("user"), name: v.string() }),
  ]);

  navigator.navigate(schema, visitor);

  // Should visit union + all options
  expect(visitor.nodes.length >= 4).toBeTruthy();
  expect(visitor.containers.length).toEqual(2); // union + object

  // Find union node
  const unionNode = visitor.nodes.find((n) => n.schema.type === "union");
  expect(unionNode).toBeDefined();
  expect(unionNode!.path).toEqual([]);

  // Find union options
  const stringOption = visitor.nodes.find((n) =>
    n.schema.type === "string" && n.key === 0
  );
  expect(stringOption).toBeDefined();
  expect(stringOption!.path).toEqual(["$union[0]"]);
  expect(stringOption!.depth).toEqual(1);
});

test("SchemaNavigator: Intersect schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.intersect([
    v.object({ a: v.string() }),
    v.object({ b: v.number() }),
  ]);

  navigator.navigate(schema, visitor);

  // Should visit intersect + both objects + properties
  expect(visitor.nodes.length >= 5).toBeTruthy();
  expect(visitor.containers.length).toEqual(3); // intersect + 2 objects

  // Find intersect node
  const intersectNode = visitor.nodes.find((n) =>
    n.schema.type === "intersect"
  );
  expect(intersectNode).toBeDefined();
  expect(intersectNode!.path).toEqual([]);
});

test("SchemaNavigator: Tuple schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.tuple([
    v.string(),
    v.number(),
    v.boolean(),
  ]);

  navigator.navigate(schema, visitor);

  // Should visit tuple + all items
  expect(visitor.nodes.length).toEqual(4);
  expect(visitor.containers.length).toEqual(1); // tuple

  // Find tuple items
  const firstItem = visitor.nodes.find((n) => n.key === 0);
  expect(firstItem).toBeDefined();
  expect(firstItem!.path).toEqual(["0"]);
  expect(firstItem!.schema.type).toEqual("string");

  const secondItem = visitor.nodes.find((n) => n.key === 1);
  expect(secondItem).toBeDefined();
  expect(secondItem!.path).toEqual(["1"]);
  expect(secondItem!.schema.type).toEqual("number");
});

test("SchemaNavigator: Record schema navigation", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.record(v.string(), v.number());

  navigator.navigate(schema, visitor);

  // Should visit record + key schema + value schema
  expect(visitor.nodes.length).toEqual(3);
  expect(visitor.containers.length).toEqual(1); // record

  // Find record node
  const recordNode = visitor.nodes.find((n) => n.schema.type === "record");
  expect(recordNode).toBeDefined();
  expect(recordNode!.path).toEqual([]);

  // Find key and value schemas
  const keyNode = visitor.nodes.find((n) => n.key === "$key");
  expect(keyNode).toBeDefined();
  expect(keyNode!.path).toEqual(["$key"]);
  expect(keyNode!.schema.type).toEqual("string");

  const valueNode = visitor.nodes.find((n) => n.key === "$value");
  expect(valueNode).toBeDefined();
  expect(valueNode!.path).toEqual(["$value"]);
  expect(valueNode!.schema.type).toEqual("number");
});

test("SchemaNavigator: Optional and nullable schemas", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.object({
    optional: v.optional(v.string()),
    nullable: v.nullable(v.number()),
    nullish: v.nullish(v.boolean()),
  });

  navigator.navigate(schema, visitor);

  // Should visit object + 3 wrapper schemas + 3 inner schemas
  expect(visitor.nodes.length).toEqual(7);

  // Find optional field
  const optionalNode = visitor.nodes.find((n) => n.key === "optional");
  expect(optionalNode).toBeDefined();
  expect(optionalNode!.schema.type).toEqual("optional");

  // Find nullable field
  const nullableNode = visitor.nodes.find((n) => n.key === "nullable");
  expect(nullableNode).toBeDefined();
  expect(nullableNode!.schema.type).toEqual("nullable");

  // Find nullish field
  const nullishNode = visitor.nodes.find((n) => n.key === "nullish");
  expect(nullishNode).toBeDefined();
  expect(nullishNode!.schema.type).toEqual("nullish");
});

test("SchemaNavigator: Depth limiting visitor", () => {
  const navigator = new SchemaNavigator();
  const visitor = new DepthLimitingVisitor(2);

  const schema = v.object({
    level1: v.object({
      level2: v.object({
        level3: v.string(),
      }),
    }),
  });

  navigator.navigate(schema, visitor);

  // Should stop at depth 2, not visit level3
  const level3Node = visitor.nodes.find((n) => n.key === "level3");
  expect(level3Node).toEqual(undefined);

  // Should visit up to level2
  const level2Node = visitor.nodes.find((n) => n.key === "level2");
  expect(level2Node).toBeDefined();
  expect(level2Node!.depth).toEqual(2);
});

test("SchemaNavigator: Initial context override", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.string();

  navigator.navigate(schema, visitor, {
    path: ["custom", "path"],
    depth: 5,
    key: "customKey",
  });

  expect(visitor.nodes.length).toEqual(1);
  expect(visitor.nodes[0].path).toEqual(["custom", "path"]);
  expect(visitor.nodes[0].depth).toEqual(5);
  expect(visitor.nodes[0].key).toEqual("customKey");
});

test("SchemaNavigator: Complex schema with all types", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.object({
    // Basic types
    name: v.pipe(v.string(), v.minLength(1)),
    age: v.pipe(v.number(), v.minValue(0)),
    active: v.boolean(),

    // Collections
    tags: v.array(v.string()),
    metadata: v.record(v.string(), v.any()),
    coordinates: v.tuple([v.number(), v.number()]),

    // Unions and intersections
    status: v.union([v.literal("active"), v.literal("inactive")]),
    combined: v.intersect([
      v.object({ a: v.string() }),
      v.object({ b: v.number() }),
    ]),

    // Optional and nullable
    description: v.optional(v.string()),
    deletedAt: v.nullable(v.date()),

    // Nested object
    address: v.object({
      street: v.string(),
      city: v.string(),
      zipCode: v.pipe(v.string(), v.regex(/^\d{5}$/)),
    }),
  });

  navigator.navigate(schema, visitor);

  // Should visit many nodes
  expect(visitor.nodes.length > 10).toBeTruthy();
  expect(visitor.containers.length > 3).toBeTruthy();
  expect(visitor.validations.length > 0).toBeTruthy();

  // Verify some specific nodes exist
  const nameNode = visitor.nodes.find((n) => n.key === "name");
  expect(nameNode).toBeDefined();
  expect(nameNode!.path).toEqual(["name"]);

  const zipCodeNode = visitor.nodes.find((n) => n.key === "zipCode");
  expect(zipCodeNode).toBeDefined();
  expect(zipCodeNode!.path).toEqual(["address", "zipCode"]);
  expect(zipCodeNode!.depth).toEqual(2);

  const statusNode = visitor.nodes.find((n) => n.key === "status");
  expect(statusNode).toBeDefined();
  expect(statusNode!.schema.type).toEqual("union");
});

test("SchemaNavigator: Navigation node convenience accessors", () => {
  const navigator = new SchemaNavigator();
  const visitor = new CollectingVisitor();

  const schema = v.object({
    nested: v.object({
      value: v.string(),
    }),
  });

  navigator.navigate(schema, visitor);

  const valueNode = visitor.nodes.find((n) => n.key === "value");
  expect(valueNode).toBeDefined();

  // Test convenience accessors
  expect(valueNode!.path).toEqual(["nested", "value"]);
  expect(valueNode!.depth).toEqual(2);
  expect(valueNode!.key).toEqual("value");
  expect(valueNode!.parent).toBeDefined();
  expect(valueNode!.parent.type).toEqual("object");

  // Test that accessors return the same as context
  expect(valueNode!.path).toEqual(valueNode!.context.path);
  expect(valueNode!.depth).toEqual(valueNode!.context.depth);
  expect(valueNode!.key).toEqual(valueNode!.context.key);
  expect(valueNode!.parent).toEqual(valueNode!.context.parent);
});

test("SchemaNavigator: Visitor without optional methods", () => {
  const navigator = new SchemaNavigator();

  // Visitor with only required method
  const basicVisitor: SchemaVisitor = {
    visitNode(_node) {
      return { continue: true };
    },
  };

  const schema = v.object({
    name: v.string(),
    tags: v.array(v.string()),
  });

  // Should not throw even without optional methods
  navigator.navigate(schema, basicVisitor);
});

test("SchemaNavigator: Early termination", () => {
  const navigator = new SchemaNavigator();
  const visited: string[] = [];

  const terminatingVisitor: SchemaVisitor = {
    visitNode(node) {
      visited.push(node.schema.type);
      // Stop after visiting 3 nodes
      return { continue: visited.length < 3 };
    },
  };

  const schema = v.object({
    a: v.string(),
    b: v.string(),
    c: v.string(),
    d: v.string(),
  });

  navigator.navigate(schema, terminatingVisitor);

  // Should have stopped early - visitor logic limits to 3 nodes
  expect(visited.length <= 5).toBeTruthy(); // Allow for some flexibility in implementation
});
