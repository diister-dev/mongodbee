import { ulid } from "@std/ulid/ulid";
import type { DatabaseState, MigrationRule } from "../types.ts"
import * as v from "valibot"

export function createMemoryApplier() {
  const migrations: {
    [K in MigrationRule['type']]: {
      apply: (state: DatabaseState, operation: Extract<MigrationRule, { type: K }>) => DatabaseState | Promise<DatabaseState>,
      reverse: (state: DatabaseState, operation: Extract<MigrationRule, { type: K }>) => DatabaseState | Promise<DatabaseState>,
    }
  } = {
    create_collection: {
      apply: (state, operation) => {
        state.collections[operation.collectionName] = { content: [] };
        return state;
      },
      reverse: (state, operation) => {
        delete state.collections[operation.collectionName];
        return state;
      }
    },
    create_multicollection: {
      apply: (state, operation) => {
        state.multiCollections[operation.collectionName] = { content: [] };
        return state;
      },
      reverse: (state, operation) => {
        delete state.multiCollections[operation.collectionName];
        return state;
      }
    },
    create_multimodel_instance: {
      apply: (state, operation) => {
        state.multiModels[operation.collectionName] ??= {
          modelType: operation.modelType,
          content: [],
        };
        return state;
      },
      reverse: (state, operation) => {
        delete state.multiModels[operation.collectionName];
        return state;
      }
    },
    mark_as_multimodel: {
      apply: (state, operation) => {
        const original = state.collections[operation.collectionName];
        if (original) {
          state.multiModels[operation.collectionName] = {
            modelType: operation.modelType,
            content: original.content,
          };
          delete state.collections[operation.collectionName];
        }
        return state;
      },
      reverse: (state, operation) => {
        const multiModel = state.multiModels[operation.collectionName];
        if (multiModel) {
          state.collections[operation.collectionName] = { content: multiModel.content };
          delete state.multiModels[operation.collectionName];
        }
        return state;
      }
    },
    seed_collection: {
      apply: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if(!collection) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        collection.content.push(...operation.documents.map((doc: unknown) => {
          const typedDoc = doc as Record<string, unknown>;
          return {
            _id: typedDoc._id || (operation.schema._id ? v.getDefault(operation.schema._id) : undefined) || ulid(),
            ...typedDoc,
          };
        }));
        return state;
      },
      reverse: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if(!collection) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        // Reversal by filtering out seeded documents by _id
        collection.content = collection.content.filter(doc => !operation.documents.some((odoc: unknown) => {
          const typedDoc = odoc as Record<string, unknown>;
          return typedDoc._id && typedDoc._id === doc._id;
        }));
        return state;
      }
    },
    seed_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        multiCollection.content.push(...operation.documents.map((doc: unknown) => {
          const typedDoc = doc as Record<string, unknown>;
          return {
            _id: typedDoc._id || (operation.schema._id ? v.getDefault(operation.schema._id) : undefined) || `${operation.documentType}:${ulid()}`,
            ...typedDoc,
            _type: operation.documentType,
          };
        }));
        return state;
      },
      reverse: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        // Reversal by filtering out seeded documents by _id
        multiCollection.content = multiCollection.content.filter(doc => !operation.documents.some((odoc: unknown) => {
          const typedDoc = odoc as Record<string, unknown>;
          return typedDoc._id && typedDoc._id === doc._id;
        }));
        return state;
      }
    },
    seed_multimodel_instance_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }
        multiCollection.content.push(...operation.documents.map((doc: unknown) => {
          const typedDoc = doc as Record<string, unknown>;
          return {
            _id: typedDoc._id || (operation.schema._id ? v.getDefault(operation.schema._id) : undefined) || `${operation.documentType}:${ulid()}`,
            ...typedDoc,
            _type: operation.documentType,
          };
        }));
        return state;
      },
      reverse: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }
        // Reversal by filtering out seeded documents by _id
        multiCollection.content = multiCollection.content.filter(doc => !operation.documents.some((odoc: unknown) => {
          const typedDoc = odoc as Record<string, unknown>;
          return typedDoc._id && typedDoc._id === doc._id;
        }));
        return state;
      }
    },
    seed_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            instance.content.push(...operation.documents.map((doc: unknown) => {
              const typedDoc = doc as Record<string, unknown>;
              return {
                _id: typedDoc._id || (operation.schema._id ? v.getDefault(operation.schema._id) : undefined) || `${operation.documentType}:${ulid()}`,
                ...typedDoc,
                _type: operation.documentType,
              };
            }));
          }
        }
        return state;
      },
      reverse: (state, operation) => {
        const modelType = operation.modelType;
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            // Reversal by filtering out seeded documents by _id
            instance.content = instance.content.filter(doc => !operation.documents.some((odoc: unknown) => {
              const typedDoc = odoc as Record<string, unknown>;
              return typedDoc._id && typedDoc._id === doc._id;
            }));
          }
        }
        return state;
      }
    },
    transform_collection: {
      apply: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if(!collection) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        collection.content = collection.content.map(operation.up as (doc: Record<string, unknown>) => Record<string, unknown>);
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const collection = state.collections[operation.collectionName];
        if(!collection) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        collection.content = collection.content.map(operation.down as (doc: Record<string, unknown>) => Record<string, unknown>);
        return state;
      }
    },
    transform_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        multiCollection.content = multiCollection.content.map(doc => {
          if (doc._type === operation.documentType) {
            return operation.up(doc as Record<string, unknown>);
          }
          return doc;
        });
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const multiCollection = state.multiCollections[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        multiCollection.content = multiCollection.content.map(doc => {
          if (doc._type === operation.documentType) {
            return operation.down(doc as Record<string, unknown>);
          }
          return doc;
        });
        return state;
      }
    },
    transform_multimodel_instance_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }
        multiCollection.content = multiCollection.content.map(doc => {
          if (doc._type === operation.documentType) {
            return operation.up(doc as Record<string, unknown>);
          }
          return doc;
        });
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const multiCollection = state.multiModels[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }
        multiCollection.content = multiCollection.content.map(doc => {
          if (doc._type === operation.documentType) {
            return operation.down(doc as Record<string, unknown>);
          }
          return doc;
        });
        return state;
      }
    },
    transform_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            instance.content = instance.content.map(doc => {
              if (doc._type === operation.documentType) {
                return operation.up(doc as Record<string, unknown>);
              }
              return doc;
            });
          }
        }
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const modelType = operation.modelType;
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            instance.content = instance.content.map(doc => {
              if (doc._type === operation.documentType) {
                return operation.down(doc as Record<string, unknown>);
              }
              return doc;
            });
          }
        }
        return state;
      }
    },
    update_indexes: {
      apply: (state) => {
        // Indexes are not modeled in this in-memory representation
        return state;
      },
      reverse: (state) => {
        // Indexes are not modeled in this in-memory representation
        return state;
      }
    },
  }

  async function applyOperation(
    state: DatabaseState,
    operation: MigrationRule,
  ): Promise<DatabaseState> {
    const handler = migrations[operation.type]?.apply;
    if (!handler) {
      throw new Error(`No handler for operation type: ${operation.type}`)
    }
    // Type assertion is safe here because we're dispatching to the correct handler based on operation.type
    // deno-lint-ignore no-explicit-any
    return await handler(state, operation as any);
  }

  async function reverseOperation(
    state: DatabaseState,
    operation: MigrationRule,
  ): Promise<DatabaseState> {
    const handler = migrations[operation.type]?.reverse;
    if (!handler) {
      throw new Error(`No reverse handler for operation type: ${operation.type}`);
    }
    // Type assertion is safe here because we're dispatching to the correct handler based on operation.type
    // deno-lint-ignore no-explicit-any
    return await handler(state, operation as any);
  }

  return {
    applyOperation,
    reverseOperation,
  }
}