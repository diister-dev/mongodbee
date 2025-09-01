import { assertEquals } from "jsr:@std/assert";
import * as v from '../src/schema.ts';
import { toMongoValidator } from '../src/validator.ts';

Deno.test("Conflict between non_empty and min_length", () => {
    // Test case 1: non_empty (minLength: 1) + min_length(5) - should be min_length(5)
    const schema1 = v.object({
        field: v.pipe(v.string(), v.nonEmpty(), v.minLength(5))
    });

    const validator1 = toMongoValidator(schema1);
    const jsonSchema1 = validator1.$jsonSchema!;
    
    // nonEmpty gives minLength: 1, minLength(5) gives minLength: 5
    // Maintenant devrait prendre le max (minLength: 5)
    assertEquals(jsonSchema1.properties!.field.minLength, 5);
    assertEquals(jsonSchema1.properties!.field.minItems, 1); // nonEmpty also sets minItems

    // Test case 2: min_length(5) + non_empty - should keep min_length(5)
    const schema2 = v.object({
        field: v.pipe(v.string(), v.minLength(5), v.nonEmpty())
    });

    const validator2 = toMongoValidator(schema2);
    const jsonSchema2 = validator2.$jsonSchema!;
    
    // minLength(5) gives minLength: 5, nonEmpty gives minLength: 1
    // Maintenant devrait prendre le max (minLength: 5) - CORRIGÉ !
    assertEquals(jsonSchema2.properties!.field.minLength, 5); // Devrait être 5 maintenant !
    assertEquals(jsonSchema2.properties!.field.minItems, 1);

    // Test case 3: multiple min_length values
    const schema3 = v.object({
        field: v.pipe(v.string(), v.minLength(3), v.minLength(7))
    });

    const validator3 = toMongoValidator(schema3);
    const jsonSchema3 = validator3.$jsonSchema!;
    
    // Le dernier devrait gagner (minLength: 7)
    assertEquals(jsonSchema3.properties!.field.minLength, 7);
});

Deno.test("Conflict between min_value and max_value", () => {
    // Test conflicting numeric ranges
    const schema = v.object({
        field: v.pipe(v.number(), v.minValue(10), v.maxValue(5)) // Impossible !
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    // Devrait avoir minimum: 10 et maximum: 5 (impossible à satisfaire)
    assertEquals(jsonSchema.properties!.field.minimum, 10);
    assertEquals(jsonSchema.properties!.field.maximum, 5);
});

Deno.test("Smart conflict resolution test", () => {
    // Test que les conflits min/max sont résolus intelligemment
    const smartSchema = v.object({
        // Test résolution minLength intelligent
        stringField: v.pipe(v.string(), v.minLength(3), v.nonEmpty(), v.minLength(5)), // Devrait être 5
        
        // Test résolution maxLength intelligent  
        maxField: v.pipe(v.string(), v.maxLength(10), v.maxLength(7)), // Devrait être 7
        
        // Test résolution minItems/maxItems pour arrays
        arrayField: v.pipe(v.array(v.string()), v.minLength(2), v.maxLength(8), v.nonEmpty()), // min=2, max=8
        
        // Test résolution minimum/maximum pour numbers
        numberField: v.pipe(v.number(), v.minValue(5), v.minValue(10), v.maxValue(100), v.maxValue(50)), // min=10, max=50
    });

    const validator = toMongoValidator(smartSchema);
    const jsonSchema = validator.$jsonSchema!;
    
    // Vérification stringField: max(3, 1, 5) = 5
    assertEquals(jsonSchema.properties!.stringField.minLength, 5);
    
    // Vérification maxField: min(10, 7) = 7  
    assertEquals(jsonSchema.properties!.maxField.maxLength, 7);
    
    // Vérification arrayField: minItems=2, maxItems=8
    assertEquals(jsonSchema.properties!.arrayField.minItems, 2); // max(2, 1) = 2
    assertEquals(jsonSchema.properties!.arrayField.maxItems, 8);
    
    // Vérification numberField: minimum=10, maximum=50
    assertEquals(jsonSchema.properties!.numberField.minimum, 10); // max(5, 10) = 10
    assertEquals(jsonSchema.properties!.numberField.maximum, 50); // min(100, 50) = 50
});
