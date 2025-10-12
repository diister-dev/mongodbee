import type { DatabaseState, MigrationRule } from "../types.ts"

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
        state.multiModels[operation.modelType] ??= {};
        state.multiModels[operation.modelType][operation.collectionName] = { content: [] };
        return state;
      },
      reverse: (state, operation) => {
        delete state.multiModels[operation.modelType]?.[operation.collectionName];
        if (Object.keys(state.multiModels[operation.modelType] || {}).length === 0) {
          delete state.multiModels[operation.modelType];
        }
        return state;
      }
    },
    mark_as_multimodel: {
      apply: (state, operation) => {
        const original = state.collections[operation.collectionName];
        if (original) {
          state.multiCollections[operation.collectionName] = original;
          delete state.collections[operation.collectionName];
        }
        return state;
      },
      reverse: (state, operation) => {
        const original = state.multiCollections[operation.collectionName];
        if (original) {
          state.collections[operation.collectionName] = original;
          delete state.multiCollections[operation.collectionName];
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
        collection.content.push(...operation.documents as any[]);
        return state;
      },
      reverse: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if(!collection) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        // Simple reversal by removing the last N documents added
        collection.content.splice(-operation.documents.length);
        return state;
      }
    },
    seed_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        multiCollection.content.push(...operation.documents as any[]);
        return state;
      },
      reverse: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        // Simple reversal by removing the last N documents added
        multiCollection.content.splice(-operation.documents.length);
        return state;
      }
    },
    seed_multimodel_instance_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection type ${operation.collectionName} does not exist`);
        }
        const instance = multiCollection[operation.collectionName];
        if(!instance) {
          throw new Error(`Multi-collection instance ${operation.collectionName} does not exist`);
        }
        instance.content.push(...operation.documents as any[]);
        return state;
      },
      reverse: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection type ${operation.collectionName} does not exist`);
        }
        const instance = multiCollection[operation.collectionName];
        if(!instance) {
          throw new Error(`Multi-collection instance ${operation.collectionName} does not exist`);
        }
        // Simple reversal by removing the last N documents added
        instance.content.splice(-operation.documents.length);
        return state;
      }
    },
    transform_collection: {
      apply: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if(!collection) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        collection.content = collection.content.map(operation.up as any);
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
        collection.content = collection.content.map(operation.down as any);
        return state;
      }
    },
    transform_multicollection_type: {
      apply: (state, operation) => {
        throw new Error("Function not implemented.");
      },
      reverse: (state, operation) => {
        throw new Error("Function not implemented.");
      }
    },
    transform_multimodel_instance_type: {
      apply: (state, operation) => {
        throw new Error("Function not implemented.");
      },
      reverse: (state, operation) => {
        throw new Error("Function not implemented.");
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
    return await handler(state, operation as any);
  }

  return {
    applyOperation,
    reverseOperation,
  }
}