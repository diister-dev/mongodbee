import type { DatabaseState, MigrationBuilder, MigrationRule } from "../types.ts"

export function createMemoryApplier() {
  const migrations: {
    [K in MigrationRule['type']]: (state: DatabaseState, operation: Extract<MigrationRule, { type: K }>) => DatabaseState | Promise<DatabaseState>
  } = {
    create_collection: (state, operation) => {
      state.collections[operation.collectionName] = { content: [] };
      return state;
    },
    create_multicollection: (state, operation) => {
      state.multiCollections[operation.collectionName] = { content: [] };
      return state;
    },
    create_multicollection_instance: (state, operation) => {
      state.multiModels[operation.collectionType] ??= {};
      state.multiModels[operation.collectionType][operation.collectionName] = { content: [] };
      return state;
    },
    // @TODO: Clarify if we need to mark as a collection for multiModels
    mark_as_multicollection: (state, operation) => {
      const original = state.collections[operation.collectionName];
      if (original) {
        state.multiCollections[operation.collectionName] = original;
        delete state.collections[operation.collectionName];
      }
      return state;
    },
    seed_collection: (state, operation) => {
      const collection = state.collections[operation.collectionName];
      if(!collection) {
        throw new Error(`Collection ${operation.collectionName} does not exist`);
      }
      collection.content.push(...operation.documents as any[]);
      return state;
    },
    seed_multicollection_instance: (state, operation) => {
      throw new Error("Function not implemented.");
    },
    transform_collection: (state, operation) => {
      throw new Error("Function not implemented.");
    },
    transform_multicollection_type: (state, operation) => {
      throw new Error("Function not implemented.");
    },
    update_indexes: (state, operation) => {
      throw new Error("Function not implemented.");
    },
  }

  async function applyOperation(
    state: DatabaseState,
    operation: MigrationRule,
  ): Promise<DatabaseState> {
    const handler = migrations[operation.type]
    if (!handler) {
      throw new Error(`No handler for operation type: ${operation.type}`)
    }
    return await handler(state, operation as any);
  }

  return {
    applyOperation
  }
}