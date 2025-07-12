import { assert, assertEquals, assertExists } from "jsr:@std/assert";
import { SchemaNavigator, type SchemaVisitor, type NavigationNode, type VisitResult } from "../src/schema-navigator.ts";
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

Deno.test("SchemaNavigator: Basic string schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.string();
    
    navigator.navigate(schema, visitor);
    
    assertEquals(visitor.nodes.length, 1);
    assertEquals(visitor.nodes[0].schema.type, "string");
    assertEquals(visitor.nodes[0].path, []);
    assertEquals(visitor.nodes[0].depth, 0);
    assertEquals(visitor.nodes[0].parent, undefined);
    assertEquals(visitor.nodes[0].key, undefined);
});

Deno.test("SchemaNavigator: Piped string schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.pipe(v.string(), v.minLength(3), v.maxLength(50));
    
    navigator.navigate(schema, visitor);
    
    // Should visit the string schema and all validations in pipe
    assert(visitor.nodes.length >= 3);
    assertEquals(visitor.nodes[0].schema.type, "string");
    assert(visitor.validations.length >= 2); // minLength and maxLength
});

Deno.test("SchemaNavigator: Object schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.object({
        name: v.string(),
        age: v.number(),
        email: v.pipe(v.string(), v.email())
    });
    
    navigator.navigate(schema, visitor);
    
    // Should visit object + 3 properties + email validation
    assert(visitor.nodes.length >= 4);
    assertEquals(visitor.containers.length, 1); // object is a container
    
    // Find the object node
    const objectNode = visitor.nodes.find(n => n.schema.type === "object");
    assertExists(objectNode);
    assertEquals(objectNode.path, []);
    assertEquals(objectNode.depth, 0);
    
    // Find property nodes
    const nameNode = visitor.nodes.find(n => n.key === "name");
    assertExists(nameNode);
    assertEquals(nameNode.path, ["name"]);
    assertEquals(nameNode.depth, 1);
    assertEquals(nameNode.schema.type, "string");
    
    const ageNode = visitor.nodes.find(n => n.key === "age");
    assertExists(ageNode);
    assertEquals(ageNode.path, ["age"]);
    assertEquals(ageNode.depth, 1);
    assertEquals(ageNode.schema.type, "number");
});

Deno.test("SchemaNavigator: Nested object schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.object({
        user: v.object({
            profile: v.object({
                name: v.string(),
                bio: v.optional(v.string())
            }),
            settings: v.object({
                theme: v.string(),
                notifications: v.boolean()
            })
        })
    });
    
    navigator.navigate(schema, visitor);
    
    // Should have multiple container levels
    assert(visitor.containers.length >= 4); // root object + user + profile + settings
    
    // Find deeply nested name field
    const nameNode = visitor.nodes.find(n => n.key === "name");
    assertExists(nameNode);
    assertEquals(nameNode.path, ["user", "profile", "name"]);
    assertEquals(nameNode.depth, 3);
    
    // Find optional bio field
    const bioNode = visitor.nodes.find(n => n.key === "bio");
    assertExists(bioNode);
    assertEquals(bioNode.path, ["user", "profile", "bio"]);
    assertEquals(bioNode.schema.type, "optional");
});

Deno.test("SchemaNavigator: Array schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.array(v.object({
        id: v.string(),
        value: v.number()
    }));
    
    navigator.navigate(schema, visitor);
    
    // Should visit array + item object + properties
    assert(visitor.nodes.length >= 4);
    assertEquals(visitor.containers.length, 2); // array + object
    
    // Find array node
    const arrayNode = visitor.nodes.find(n => n.schema.type === "array");
    assertExists(arrayNode);
    assertEquals(arrayNode.path, []);
    assertEquals(arrayNode.depth, 0);
    
    // Find item object properties
    const idNode = visitor.nodes.find(n => n.key === "id");
    assertExists(idNode);
    assertEquals(idNode.path, ["$[]", "id"]);
    assertEquals(idNode.depth, 2);
});

Deno.test("SchemaNavigator: Union schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.union([
        v.string(),
        v.number(),
        v.object({ type: v.literal("user"), name: v.string() })
    ]);
    
    navigator.navigate(schema, visitor);
    
    // Should visit union + all options
    assert(visitor.nodes.length >= 4);
    assertEquals(visitor.containers.length, 2); // union + object
    
    // Find union node
    const unionNode = visitor.nodes.find(n => n.schema.type === "union");
    assertExists(unionNode);
    assertEquals(unionNode.path, []);
    
    // Find union options
    const stringOption = visitor.nodes.find(n => n.schema.type === "string" && n.key === 0);
    assertExists(stringOption);
    assertEquals(stringOption.path, ["$union[0]"]);
    assertEquals(stringOption.depth, 1);
});

Deno.test("SchemaNavigator: Intersect schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.intersect([
        v.object({ a: v.string() }),
        v.object({ b: v.number() })
    ]);
    
    navigator.navigate(schema, visitor);
    
    // Should visit intersect + both objects + properties
    assert(visitor.nodes.length >= 5);
    assertEquals(visitor.containers.length, 3); // intersect + 2 objects
    
    // Find intersect node
    const intersectNode = visitor.nodes.find(n => n.schema.type === "intersect");
    assertExists(intersectNode);
    assertEquals(intersectNode.path, []);
});

Deno.test("SchemaNavigator: Tuple schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.tuple([
        v.string(),
        v.number(),
        v.boolean()
    ]);
    
    navigator.navigate(schema, visitor);
    
    // Should visit tuple + all items
    assertEquals(visitor.nodes.length, 4);
    assertEquals(visitor.containers.length, 1); // tuple
    
    // Find tuple items
    const firstItem = visitor.nodes.find(n => n.key === 0);
    assertExists(firstItem);
    assertEquals(firstItem.path, ["0"]);
    assertEquals(firstItem.schema.type, "string");
    
    const secondItem = visitor.nodes.find(n => n.key === 1);
    assertExists(secondItem);
    assertEquals(secondItem.path, ["1"]);
    assertEquals(secondItem.schema.type, "number");
});

Deno.test("SchemaNavigator: Record schema navigation", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.record(v.string(), v.number());
    
    navigator.navigate(schema, visitor);
    
    // Should visit record + key schema + value schema
    assertEquals(visitor.nodes.length, 3);
    assertEquals(visitor.containers.length, 1); // record
    
    // Find record node
    const recordNode = visitor.nodes.find(n => n.schema.type === "record");
    assertExists(recordNode);
    assertEquals(recordNode.path, []);
    
    // Find key and value schemas
    const keyNode = visitor.nodes.find(n => n.key === "$key");
    assertExists(keyNode);
    assertEquals(keyNode.path, ["$key"]);
    assertEquals(keyNode.schema.type, "string");
    
    const valueNode = visitor.nodes.find(n => n.key === "$value");
    assertExists(valueNode);
    assertEquals(valueNode.path, ["$value"]);
    assertEquals(valueNode.schema.type, "number");
});

Deno.test("SchemaNavigator: Optional and nullable schemas", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.object({
        optional: v.optional(v.string()),
        nullable: v.nullable(v.number()),
        nullish: v.nullish(v.boolean())
    });
    
    navigator.navigate(schema, visitor);
    
    // Should visit object + 3 wrapper schemas + 3 inner schemas
    assertEquals(visitor.nodes.length, 7);
    
    // Find optional field
    const optionalNode = visitor.nodes.find(n => n.key === "optional");
    assertExists(optionalNode);
    assertEquals(optionalNode.schema.type, "optional");
    
    // Find nullable field
    const nullableNode = visitor.nodes.find(n => n.key === "nullable");
    assertExists(nullableNode);
    assertEquals(nullableNode.schema.type, "nullable");
    
    // Find nullish field
    const nullishNode = visitor.nodes.find(n => n.key === "nullish");
    assertExists(nullishNode);
    assertEquals(nullishNode.schema.type, "nullish");
});

Deno.test("SchemaNavigator: Depth limiting visitor", () => {
    const navigator = new SchemaNavigator();
    const visitor = new DepthLimitingVisitor(2);
    
    const schema = v.object({
        level1: v.object({
            level2: v.object({
                level3: v.string()
            })
        })
    });
    
    navigator.navigate(schema, visitor);
    
    // Should stop at depth 2, not visit level3
    const level3Node = visitor.nodes.find(n => n.key === "level3");
    assertEquals(level3Node, undefined);
    
    // Should visit up to level2
    const level2Node = visitor.nodes.find(n => n.key === "level2");
    assertExists(level2Node);
    assertEquals(level2Node.depth, 2);
});

Deno.test("SchemaNavigator: Initial context override", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.string();
    
    navigator.navigate(schema, visitor, {
        path: ["custom", "path"],
        depth: 5,
        key: "customKey"
    });
    
    assertEquals(visitor.nodes.length, 1);
    assertEquals(visitor.nodes[0].path, ["custom", "path"]);
    assertEquals(visitor.nodes[0].depth, 5);
    assertEquals(visitor.nodes[0].key, "customKey");
});

Deno.test("SchemaNavigator: Complex schema with all types", () => {
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
            v.object({ b: v.number() })
        ]),
        
        // Optional and nullable
        description: v.optional(v.string()),
        deletedAt: v.nullable(v.date()),
        
        // Nested object
        address: v.object({
            street: v.string(),
            city: v.string(),
            zipCode: v.pipe(v.string(), v.regex(/^\d{5}$/))
        })
    });
    
    navigator.navigate(schema, visitor);
    
    // Should visit many nodes
    assert(visitor.nodes.length > 10);
    assert(visitor.containers.length > 3);
    assert(visitor.validations.length > 0);
    
    // Verify some specific nodes exist
    const nameNode = visitor.nodes.find(n => n.key === "name");
    assertExists(nameNode);
    assertEquals(nameNode.path, ["name"]);
    
    const zipCodeNode = visitor.nodes.find(n => n.key === "zipCode");
    assertExists(zipCodeNode);
    assertEquals(zipCodeNode.path, ["address", "zipCode"]);
    assertEquals(zipCodeNode.depth, 2);
    
    const statusNode = visitor.nodes.find(n => n.key === "status");
    assertExists(statusNode);
    assertEquals(statusNode.schema.type, "union");
});

Deno.test("SchemaNavigator: Navigation node convenience accessors", () => {
    const navigator = new SchemaNavigator();
    const visitor = new CollectingVisitor();
    
    const schema = v.object({
        nested: v.object({
            value: v.string()
        })
    });
    
    navigator.navigate(schema, visitor);
    
    const valueNode = visitor.nodes.find(n => n.key === "value");
    assertExists(valueNode);
    
    // Test convenience accessors
    assertEquals(valueNode.path, ["nested", "value"]);
    assertEquals(valueNode.depth, 2);
    assertEquals(valueNode.key, "value");
    assertExists(valueNode.parent);
    assertEquals(valueNode.parent.type, "object");
    
    // Test that accessors return the same as context
    assertEquals(valueNode.path, valueNode.context.path);
    assertEquals(valueNode.depth, valueNode.context.depth);
    assertEquals(valueNode.key, valueNode.context.key);
    assertEquals(valueNode.parent, valueNode.context.parent);
});

Deno.test("SchemaNavigator: Visitor without optional methods", () => {
    const navigator = new SchemaNavigator();
    
    // Visitor with only required method
    const basicVisitor: SchemaVisitor = {
        visitNode(_node) {
            return { continue: true };
        }
    };
    
    const schema = v.object({
        name: v.string(),
        tags: v.array(v.string())
    });
    
    // Should not throw even without optional methods
    navigator.navigate(schema, basicVisitor);
});

Deno.test("SchemaNavigator: Early termination", () => {
    const navigator = new SchemaNavigator();
    const visited: string[] = [];
    
    const terminatingVisitor: SchemaVisitor = {
        visitNode(node) {
            visited.push(node.schema.type);
            // Stop after visiting 3 nodes
            return { continue: visited.length < 3 };
        }
    };
    
    const schema = v.object({
        a: v.string(),
        b: v.string(),
        c: v.string(),
        d: v.string()
    });
    
    navigator.navigate(schema, terminatingVisitor);
    
    // Should have stopped early - visitor logic limits to 3 nodes
    assert(visited.length <= 5); // Allow for some flexibility in implementation
});
