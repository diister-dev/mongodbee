// async function upgrade(migration: MigrationBuilder) {
//     const userMigration = migration.collection("+users")
//         .seed([
//             { firstname: "John", lastname: "Doe" },
//             { firstname: "Jane", lastname: "Smith" },
//         ])
//         .addField("isActive", (doc) => true)
//         .done();

//     const articleMigration = migration.collection("+articles")
//         .transform({
//             up: (doc) => ({ ...doc, slug: generateSlug(doc.title) }),
//             down: (doc) => {
//                 delete doc[slug];
//                 return doc;
//             }
//         })
//         .done();

//     return migration; // System will call executeAll
// }

type MigrationIrreversible = {
    type: 'irreversible';
}

type MigrationPropertie = MigrationIrreversible;

type CreateCollectionRule = {
    type : 'create_collection';
    collectionName: string;
}

type SeedCollectionRule = {
    type: 'seed_collection';
    collectionName: string;
    documents: readonly unknown[];
}

type TransformCollectionRule = {
    type: 'transform_collection';
    collectionName: string;
    up: (doc: any) => any;
    down: (doc: any) => any;
}

type MigrationRule =
    | CreateCollectionRule
    | SeedCollectionRule
    | TransformCollectionRule;

type TransformRule<T = any, U = any> = {
    readonly up: (doc: T) => U;
    readonly down: (doc: U) => T;
};

type MigrationState = {
    properties: MigrationPropertie[];
    operations: MigrationRule[];
    mark(props: MigrationPropertie): void;
    hasProperty(type: MigrationPropertie['type']): boolean;
}

type MigrationCollectionBuilder = {
    seed(documents: readonly unknown[]): MigrationCollectionBuilder;
    transform(rule: TransformRule): MigrationCollectionBuilder;
    done(): MigrationBuilder;
}

export type MigrationBuilder = {
    createCollection(name: string): MigrationCollectionBuilder;
    collection(name: string): MigrationCollectionBuilder;
    compile(): MigrationState;
}

export function migrationBuilder(initState: MigrationState | undefined = undefined) : MigrationBuilder {
    const state = initState ?? {
        properties: [],
        operations: [],
        mark(props: (MigrationPropertie)) {
            const exists = state!.properties.find(p => p.type === props.type);
            if(!exists) {
                state!.properties.push(props);
            }
        },
        hasProperty(type: MigrationPropertie['type']) : boolean {
            return state!.properties.some(p => p.type === type);
        }
    }

    //#region PRIVATE CONTEXT
    function collectionBuilding(workingState: MigrationState, collectionName: string) {
        function seed(documents: readonly unknown[]) {
            workingState.operations ??= [];
            workingState.operations.push({
                type: 'seed_collection',
                collectionName,
                documents
            });
            return collectionBuilding(workingState, collectionName);
        }

        function transform(rule: TransformRule) {
            workingState.operations ??= [];
            workingState.operations.push({
                type: 'transform_collection',
                collectionName,
                up: rule.up,
                down: rule.down,
            });
            return collectionBuilding(workingState, collectionName);
        }
        
        function done() {
            return migrationBuilder(workingState);
        }

        return {
            seed,
            transform,
            done,
        }
    }
    // #endregion

    // #region PUBLIC CONTEXT
    function createCollection(name: string) {
        state.operations.push({
            type: 'create_collection',
            collectionName: name
        });
        // Any operation after createCollection makes the migration irreversible
        // Because to reverse it, we would need to drop the collection, which would lead to data loss
        state.mark({ type: 'irreversible' });
        return collectionBuilding(state, name);
    }

    function collection(name: string) {
        return collectionBuilding(state, name);
    }
    // #endregion

    function compile() {
        return state;
    }

    return {
        createCollection,
        collection,
        compile,
    }
}

type SchemasDefinition = {
    collections: Record<string, any>;
    multiCollections?: Record<string, any>;
}

type MigrationDefinition<
    Schema extends SchemasDefinition,
    Parent extends null | MigrationDefinition<any, any>
> = {
    id: string;
    name: string;
    parent: Parent;
    schemas: Schema;
    migrate: (migration: MigrationBuilder) => MigrationState;
}

export function migrationDefinition<
    Schema extends SchemasDefinition,
    Parent extends null | MigrationDefinition<any, any>
>(
    id: string,
    name: string,
    options: {
        parent: Parent,
        schemas: Schema,
        migrate: (migration: MigrationBuilder) => MigrationState,
    }
) {
    return {
        id,
        name,
        ...options,
    };
}

// const migration = migrationBuilder()
//     .createCollection("+users")
//         .seed([
//             { firstname: "John", lastname: "Doe" },
//             { firstname: "Jane", lastname: "Smith" },
//         ])
//         .done()
//     .collection("+articles")
//         .transform({
//             up: (doc) => ({
//                 ...doc,
//                 slug: doc.title.toLowerCase()
//             }),
//             down: (doc) => {
//                 const { slug, ...rest } = doc;
//                 return rest;
//             }
//         })
//         .done()

// const compiled = migration.compile();

// console.log(compiled);