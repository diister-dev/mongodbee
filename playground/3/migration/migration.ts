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

type TransformRule<T = unknown, U = unknown> = {
    readonly up: (doc: T) => U;
    readonly down: (doc: U) => T;
};

function migrationBuilder(state: any = undefined) {
    if(state === undefined) {
        state = {
            properties: [],
            collections: {},
            operations: [],
            mark(props: string) {
                if(!state.properties.includes(props)) {
                    state.properties.push(props);
                }
            }
        }
    }

    //#region PRIVATE CONTEXT
    function collectionBuilding(parentState: any, collectionName: string, workingState: any) {
        function seed(documents: readonly unknown[]) {
            console.log(`Seeding ${collectionName} with ${documents.length} documents`);
            workingState.operations ??= [];
            workingState.operations.push({
                type: 'seed',
                payload: documents
            });
            return collectionBuilding(parentState, collectionName, workingState);
        }

        function transform<T, U>(rule: TransformRule<T, U>) {
            console.log(`Adding transform rule for ${collectionName}`);
            workingState.operations ??= [];
            workingState.operations.push({
                type: 'transform',
                payload: rule
            });
            return collectionBuilding(parentState, collectionName, workingState);
        }

        function addField(fieldName: string, valueOrGenerator: unknown | ((doc: unknown) => unknown)) {
            console.log(`Adding field '${fieldName}' to ${collectionName}`);
            workingState.operations ??= [];
            workingState.operations.push({
                type: 'addField',
                payload: { fieldName, valueOrGenerator }
            });
            return collectionBuilding(parentState, collectionName, workingState);
        }

        function removeField(fieldName: string, options?: { backup?: boolean }) {
            console.log(`Removing field '${fieldName}' from ${collectionName}`);
            workingState.operations ??= [];
            workingState.operations.push({
                type: 'removeField',
                payload: { fieldName, options }
            });
            parentState.mark('irreversible');
            return collectionBuilding(parentState, collectionName, workingState);
        }

        function renameField(from: string, to: string) {
            console.log(`Renaming field '${from}' to '${to}' in ${collectionName}`);
            workingState.operations ??= [];
            workingState.operations.push({
                type: 'renameField',
                payload: { from, to }
            });
            return collectionBuilding(parentState, collectionName, workingState);
        }

        // ✨ Le done() magique - retourne au contexte parent
        function done() {
            return migrationBuilder(parentState);
        }

        // Interface collection - seulement les méthodes de collection + done
        return {
            seed,
            transform,
            addField,
            removeField,
            renameField,
            done,
        }
    }
    // #endregion

    // #region PUBLIC CONTEXT
    function collection(name: string) {
        console.log(`Working on collection: ${name}`);        
        if(!state.collections[name]) {
            state.collections[name] = {
                name,
                operations: []
            };
        }
        return collectionBuilding(state, name, state.collections[name]);
    }

    function dropCollection(name: string) {
        console.log(`Dropping collection: ${name}`);
        state.operations.push({
            type: 'dropCollection',
            collection: name
        });
        state.mark('irreversible');
        return migrationBuilder(state);
    }
    // #endregion

    function compile() {
        return state;
    }

    return {
        collection,
        dropCollection,
        compile,
    }
}

const migration = migrationBuilder()
    .collection("+users")
        .seed([
            { firstname: "John", lastname: "Doe" },
            { firstname: "Jane", lastname: "Smith" },
        ])
        .addField("isActive", () => true)
        .addField("uuid", () => crypto.randomUUID())
        .done()
    .collection("+articles")
        .transform({
            up: (doc: any) => ({
                ...doc,
                slug: doc.title.toLowerCase()
            }),
            down: (doc: any) => {
                const { slug, ...rest } = doc;
                return rest;
            }
        })
        .addField("publishedAt", () => new Date())
        .done()
    .dropCollection("temp_collection");

const compiled = migration.compile();

console.log(compiled);