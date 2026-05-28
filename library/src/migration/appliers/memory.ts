import type { DatabaseState, MigrationDefinition, MigrationRule } from "../types.ts"
import { flowTargetId, extractIdPrefix, resolveSeedId } from "../utils/seed-id.ts";
import { getIrreversibleOperations } from "../builder.ts";

/** Simple equality matcher for in-memory `where` filters (exact match only). */
function matchesWhere(
  doc: Record<string, unknown>,
  where?: Record<string, unknown>,
): boolean {
  if (!where) return true;
  return Object.entries(where).every(([k, val]) => doc[k] === val);
}

export function createMemoryApplier(migration: MigrationDefinition) {
  const migrationId = migration?.id ?? "unknown";

  /**
   * Resolve the `_id` for a seed document: honour an explicit `_id`,
   * otherwise derive a deterministic one so apply/reverse agree.
   */
  function seedId(
    doc: Record<string, unknown>,
    schemaIdField: unknown,
    fallbackPrefix: string,
    opSignature: string,
    docIndex: number,
  ): string {
    return resolveSeedId(doc, schemaIdField, fallbackPrefix, migrationId, opSignature, docIndex);
  }

  /** Move a physical collection across whichever bucket currently holds it. */
  function renamePhysical(
    state: DatabaseState,
    from: string,
    to: string,
    dropTarget: boolean,
  ): DatabaseState {
    if (dropTarget) {
      delete state.collections[to];
      delete state.multiCollections[to];
      delete state.multiModels[to];
      delete state.scopedMultiCollections[to];
    }
    if (state.collections[from]) {
      state.collections[to] = state.collections[from];
      delete state.collections[from];
    } else if (state.multiCollections[from]) {
      state.multiCollections[to] = state.multiCollections[from];
      delete state.multiCollections[from];
    } else if (state.multiModels[from]) {
      state.multiModels[to] = state.multiModels[from];
      delete state.multiModels[from];
    } else if (state.scopedMultiCollections[from]) {
      state.scopedMultiCollections[to] = state.scopedMultiCollections[from];
      delete state.scopedMultiCollections[from];
    }
    return state;
  }

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
    rename_collection: {
      apply: (state, operation) =>
        renamePhysical(state, operation.from, operation.to, operation.dropTarget ?? false),
      reverse: (state, operation) =>
        renamePhysical(state, operation.to, operation.from, false),
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
        const sig = operation.collectionName;
        collection.content.push(...operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          return {
            ...typedDoc,
            _id: seedId(typedDoc, operation.schema._id, "", sig, i),
          };
        }));
        return state;
      },
      reverse: (state, operation) => {
        const collection = state.collections[operation.collectionName];
        if(!collection) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        const sig = operation.collectionName;
        // Recompute the exact ids that were inserted, then filter them out.
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(doc as Record<string, unknown>, operation.schema._id, "", sig, i)
          ),
        );
        collection.content = collection.content.filter(
          (doc) => !seededIds.has(String(doc._id)),
        );
        return state;
      }
    },
    seed_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        const sig = `${operation.collectionName}:${operation.documentType}`;
        multiCollection.content.push(...operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          return {
            ...typedDoc,
            _id: seedId(typedDoc, operation.schema._id, operation.documentType, sig, i),
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
        const sig = `${operation.collectionName}:${operation.documentType}`;
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(doc as Record<string, unknown>, operation.schema._id, operation.documentType, sig, i)
          ),
        );
        multiCollection.content = multiCollection.content.filter(
          (doc) => !seededIds.has(String(doc._id)),
        );
        return state;
      }
    },
    seed_multimodel_instance_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiModels[operation.collectionName];
        if(!multiCollection) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }
        const sig = `${operation.collectionName}:${operation.modelType}:${operation.documentType}`;
        multiCollection.content.push(...operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          return {
            ...typedDoc,
            _id: seedId(typedDoc, operation.schema._id, operation.documentType, sig, i),
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
        const sig = `${operation.collectionName}:${operation.modelType}:${operation.documentType}`;
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(doc as Record<string, unknown>, operation.schema._id, operation.documentType, sig, i)
          ),
        );
        multiCollection.content = multiCollection.content.filter(
          (doc) => !seededIds.has(String(doc._id)),
        );
        return state;
      }
    },
    seed_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        const sig = `${operation.modelType}:${operation.documentType}`;
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            instance.content.push(...operation.documents.map((doc: unknown, i) => {
              const typedDoc = doc as Record<string, unknown>;
              return {
                ...typedDoc,
                _id: seedId(typedDoc, operation.schema._id, operation.documentType, sig, i),
                _type: operation.documentType,
              };
            }));
          }
        }
        return state;
      },
      reverse: (state, operation) => {
        const modelType = operation.modelType;
        const sig = `${operation.modelType}:${operation.documentType}`;
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(doc as Record<string, unknown>, operation.schema._id, operation.documentType, sig, i)
          ),
        );
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            instance.content = instance.content.filter(
              (doc) => !seededIds.has(String(doc._id)),
            );
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
    flow: {
      apply: (state, operation) => {
        const src = state.collections[operation.from.collection];
        if (!src) {
          throw new Error(`Flow source collection ${operation.from.collection} does not exist`);
        }
        const tgt = state.collections[operation.into.collection];
        if (!tgt) {
          throw new Error(`Flow target collection ${operation.into.collection} does not exist`);
        }

        const prefix = extractIdPrefix(operation.targetIdSchema, "");
        const matched = src.content.filter((doc) =>
          matchesWhere(doc, operation.from.where)
        );

        for (const doc of matched) {
          const mapped = operation.map({ ...doc }) as Record<string, unknown>;
          mapped._id = flowTargetId(
            prefix,
            migrationId,
            operation.from.collection,
            String(doc._id),
          );
          tgt.content.push(mapped);
        }

        if (operation.sourceDisposition === "consume") {
          src.content = src.content.filter(
            (doc) => !matchesWhere(doc, operation.from.where),
          );
        }
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(
            "Flow with source: 'consume' (move) is irreversible — cannot roll back",
          );
        }
        const src = state.collections[operation.from.collection];
        const tgt = state.collections[operation.into.collection];
        if (!src || !tgt) {
          throw new Error(`Flow collections missing for reverse`);
        }
        // Copy reverse: recompute target ids from the still-present source.
        const prefix = extractIdPrefix(operation.targetIdSchema, "");
        const targetIds = new Set(
          src.content
            .filter((doc) => matchesWhere(doc, operation.from.where))
            .map((doc) =>
              flowTargetId(prefix, migrationId, operation.from.collection, String(doc._id))
            ),
        );
        tgt.content = tgt.content.filter(
          (doc) => !targetIds.has(String(doc._id)),
        );
        return state;
      },
    },
    flow_to_scope: {
      apply: (state, operation) => {
        const target = (state.scopedMultiCollections[operation.into.collection] ??= {
          content: [],
        });

        // Gather (doc, ctx, remove) from the source selector.
        const items: {
          doc: Record<string, unknown>;
          ctx: { sourceCollection?: string; instanceName?: string; documentType?: string };
          remove: () => void;
        }[] = [];
        const from = operation.from;
        if (from.kind === "collection") {
          const coll = state.collections[from.name];
          if (coll) {
            for (const doc of [...coll.content]) {
              if (matchesWhere(doc, from.where)) {
                items.push({
                  doc,
                  ctx: { sourceCollection: from.name },
                  remove: () => { coll.content = coll.content.filter((d) => d !== doc); },
                });
              }
            }
          }
        } else if (from.kind === "multiModelInstances") {
          for (const [instanceName, inst] of Object.entries(state.multiModels)) {
            if (inst.modelType !== from.model) continue;
            for (const doc of [...inst.content]) {
              // Skip the multi-collection's internal bookkeeping docs
              // (`_information`/`_migrations`) — mongodbee plumbing, not real
              // sub-documents (mirrors the mongodb applier).
              if (typeof doc._type === "string" && doc._type.startsWith("_")) continue;
              items.push({
                doc,
                ctx: { instanceName },
                remove: () => { inst.content = inst.content.filter((d) => d !== doc); },
              });
            }
          }
        } else {
          const coll = state.multiCollections[from.collectionName];
          if (coll) {
            for (const doc of [...coll.content]) {
              if (doc._type === from.documentType) {
                items.push({
                  doc,
                  ctx: { documentType: from.documentType },
                  remove: () => { coll.content = coll.content.filter((d) => d !== doc); },
                });
              }
            }
          }
        }

        for (const { doc, ctx } of items) {
          const scope = operation.scope(doc, ctx);
          const mapped = operation.map ? operation.map({ ...doc }, ctx) : { ...doc };
          const toType = operation.toType
            ? operation.toType(doc, ctx)
            : (mapped._type ?? doc._type) as string;
          let id = mapped._id;
          if (id === undefined || id === null) {
            id = `${toType}:${crypto.randomUUID().replace(/-/g, "")}`;
          }
          const outDoc = { ...mapped, _id: id, _type: toType, _scope: scope };

          const idx = target.content.findIndex(
            (d) => d._scope === scope && d._type === toType && d._id === id,
          );
          if (idx >= 0) {
            const onConflict = operation.onConflict ?? "error";
            if (onConflict === "error") {
              throw new Error(
                `flow_to_scope: conflict on (${scope}, ${toType}, ${id})`,
              );
            }
            if (onConflict === "skip") continue;
            const merged = operation.merge
              ? operation.merge(target.content[idx], outDoc)
              : { ...target.content[idx], ...outDoc };
            target.content[idx] = { ...merged, _id: id, _type: toType, _scope: scope };
          } else {
            target.content.push(outDoc);
          }
        }

        if (operation.sourceDisposition === "consume") {
          // Fully-consumed sources are removed entirely (key dropped), so a
          // drained collection / multi-model instance no longer "exists".
          if (from.kind === "collection" && !from.where) {
            delete state.collections[from.name];
          } else if (from.kind === "multiModelInstances") {
            for (const [instanceName, inst] of Object.entries(state.multiModels)) {
              if (inst.modelType === from.model) delete state.multiModels[instanceName];
            }
          } else {
            for (const { remove } of items) remove(); // filtered subset only
          }
        }
        return state;
      },
      reverse: (_state, _operation) => {
        throw new Error("flow_to_scope is irreversible — cannot roll back");
      },
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
    delete_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        // Remove all documents of this type
        multiCollection.content = multiCollection.content.filter(
          doc => doc._type !== operation.documentType
        );
        return state;
      },
      reverse: (_state, _operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(`Cannot reverse delete_multicollection_type: operation is irreversible`);
      }
    },
    delete_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            // Remove all documents of this type from this instance
            instance.content = instance.content.filter(
              doc => doc._type !== operation.documentType
            );
          }
        }
        return state;
      },
      reverse: (_state, _operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(`Cannot reverse delete_multimodel_instances_type: operation is irreversible`);
      }
    },
    rename_multicollection_type: {
      apply: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        // Rename all documents from oldTypeName to newTypeName
        multiCollection.content = multiCollection.content.map(doc => {
          if (doc._type === operation.oldTypeName) {
            return { ...doc, _type: operation.newTypeName };
          }
          return doc;
        });
        return state;
      },
      reverse: (state, operation) => {
        const multiCollection = state.multiCollections[operation.collectionName];
        if (!multiCollection) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        // Reverse: rename from newTypeName back to oldTypeName
        multiCollection.content = multiCollection.content.map(doc => {
          if (doc._type === operation.newTypeName) {
            return { ...doc, _type: operation.oldTypeName };
          }
          return doc;
        });
        return state;
      }
    },
    create_scoped_multicollection: {
      apply: (state, operation) => {
        state.scopedMultiCollections[operation.collectionName] = { content: [] };
        return state;
      },
      reverse: (state, operation) => {
        delete state.scopedMultiCollections[operation.collectionName];
        return state;
      },
    },
    seed_scoped_multicollection_type: {
      apply: (state, operation) => {
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(`Scoped multi-collection ${operation.collectionName} does not exist`);
        }
        const sig = `${operation.collectionName}:${operation.scope}:${operation.documentType}`;
        coll.content.push(...operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          return {
            ...typedDoc,
            _id: seedId(typedDoc, operation.schema._id, operation.documentType, sig, i),
            _type: operation.documentType,
            _scope: operation.scope,
          };
        }));
        return state;
      },
      reverse: (state, operation) => {
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(`Scoped multi-collection ${operation.collectionName} does not exist`);
        }
        const sig = `${operation.collectionName}:${operation.scope}:${operation.documentType}`;
        const seededIds = new Set(
          operation.documents.map((doc: unknown, i) =>
            seedId(doc as Record<string, unknown>, operation.schema._id, operation.documentType, sig, i)
          ),
        );
        coll.content = coll.content.filter(
          (doc) => !seededIds.has(String(doc._id)),
        );
        return state;
      },
    },
    transform_scoped_multicollection_type: {
      apply: (state, operation) => {
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(`Scoped multi-collection ${operation.collectionName} does not exist`);
        }
        const scopeSet = operation.scopeFilter && operation.scopeFilter.length > 0
          ? new Set(operation.scopeFilter)
          : null;
        coll.content = coll.content.map((doc) => {
          if (
            doc._type === operation.documentType &&
            (!scopeSet || scopeSet.has(doc._scope as string))
          ) {
            return { ...operation.up(doc as Record<string, unknown>), _type: doc._type, _scope: doc._scope, _id: doc._id };
          }
          return doc;
        });
        return state;
      },
      reverse: (state, operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const coll = state.scopedMultiCollections[operation.collectionName];
        if (!coll) {
          throw new Error(`Scoped multi-collection ${operation.collectionName} does not exist`);
        }
        const scopeSet = operation.scopeFilter && operation.scopeFilter.length > 0
          ? new Set(operation.scopeFilter)
          : null;
        coll.content = coll.content.map((doc) => {
          if (
            doc._type === operation.documentType &&
            (!scopeSet || scopeSet.has(doc._scope as string))
          ) {
            return { ...operation.down(doc as Record<string, unknown>), _type: doc._type, _scope: doc._scope, _id: doc._id };
          }
          return doc;
        });
        return state;
      },
    },
    rename_multimodel_instances_type: {
      apply: (state, operation) => {
        const modelType = operation.modelType;
        const oldTypePrefix = `${operation.oldTypeName}:`;
        const newTypePrefix = `${operation.newTypeName}:`;
        
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            // Rename all documents from oldTypeName to newTypeName
            // Also update _id if it starts with "oldTypeName:"
            instance.content = instance.content.map(doc => {
              if (doc._type === operation.oldTypeName) {
                const oldId = doc._id;
                let newId = oldId;
                
                // If _id is a string starting with "oldTypeName:", replace the prefix
                if (typeof oldId === 'string' && oldId.startsWith(oldTypePrefix)) {
                  newId = newTypePrefix + oldId.slice(oldTypePrefix.length);
                }
                
                return { ...doc, _type: operation.newTypeName, _id: newId };
              }
              return doc;
            });
          }
        }
        return state;
      },
      reverse: (state, operation) => {
        const modelType = operation.modelType;
        const oldTypePrefix = `${operation.oldTypeName}:`;
        const newTypePrefix = `${operation.newTypeName}:`;
        
        for (const [_instanceName, instance] of Object.entries(state.multiModels)) {
          if (instance.modelType === modelType) {
            // Reverse: rename from newTypeName back to oldTypeName
            // Also restore _id prefix if it starts with "newTypeName:"
            instance.content = instance.content.map(doc => {
              if (doc._type === operation.newTypeName) {
                const currentId = doc._id;
                let restoredId = currentId;
                
                // If _id is a string starting with "newTypeName:", replace the prefix back
                if (typeof currentId === 'string' && currentId.startsWith(newTypePrefix)) {
                  restoredId = oldTypePrefix + currentId.slice(newTypePrefix.length);
                }
                
                return { ...doc, _type: operation.oldTypeName, _id: restoredId };
              }
              return doc;
            });
          }
        }
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

  /**
   * Applies a complete migration (all operations)
   * 
   * For memory applier, there's no schema synchronization needed since
   * this is just an in-memory simulation.
   * 
   * @param state - Current database state
   * @param operations - Array of migration operations to apply
   * @param direction - 'up' for forward migration, 'down' for rollback
   * @returns Updated database state
   */
  async function applyMigration(
    state: DatabaseState,
    operations: MigrationRule[],
    direction: 'up' | 'down',
  ): Promise<DatabaseState> {
    let currentState = state;

    // Pre-scan: refuse to roll back if any operation is irreversible, BEFORE
    // mutating anything — otherwise we'd leave a partial rollback behind.
    if (direction === "down") {
      const irreversible = getIrreversibleOperations(operations);
      if (irreversible.length > 0) {
        throw new Error(
          `Cannot roll back: migration contains ${irreversible.length} ` +
            `irreversible operation(s) [${irreversible.map((o) => o.type).join(", ")}]. ` +
            `Rollback aborted before any changes were made.`,
        );
      }
    }

    // Rollback undoes operations in LIFO order — reverse the list for 'down'
    // so e.g. a `seed` is undone before the `create_collection` it depends on.
    const ordered = direction === "down" ? [...operations].reverse() : operations;

    for (const operation of ordered) {
      if (direction === 'up') {
        currentState = await applyOperation(currentState, operation);
      } else {
        currentState = await reverseOperation(currentState, operation);
      }
    }

    // No synchronization needed for memory applier
    return currentState;
  }

  return {
    applyOperation,
    reverseOperation,
    applyMigration,
  }
}