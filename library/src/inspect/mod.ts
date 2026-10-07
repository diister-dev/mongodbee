export { VERSION } from "../version.ts";
export {
  type DocumentId,
  isDocumentId,
  type StoredDocument,
  storedCollection,
  toStoredDocument,
} from "../stored-document.ts";
export { COMPUTED_REVISION, COMPUTED_ROOT } from "../computed-guard.ts";
export { type MongoValidator, validatorOf } from "./validator.ts";
export { REMOVE_FIELD } from "../sanitizer.ts";
export {
  entriesToNodes,
  type SchemaCheck,
  type SchemaNode,
  schemaToNode,
} from "./schema-tree.ts";
export {
  type IndexPlanKind,
  type IndexPlanTarget,
  plannedIndexes,
} from "./indexes.ts";

export { loadConfig } from "../migration/config/loader.ts";
export {
  diffSchemas,
  flattenSchema,
  loadProjectSchema,
  simplifySchema,
} from "../migration/schema-validation.ts";
export {
  buildMigrationChain,
  loadAllMigrations,
} from "../migration/discovery.ts";
export {
  getAllOperations,
  getAppliedMigrationIds,
  MIGRATION_OPERATIONS_COLLECTION,
  type MigrationOperation,
} from "../migration/history.ts";
export {
  getAllMigrationStates,
  type MigrationStateRecord,
} from "../migration/state.ts";
export {
  createMetadataSchemas,
  discoverMultiCollectionInstances,
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
} from "../migration/multicollection-registry.ts";
export {
  getIrreversibleOperations,
  getLossyOperations,
  getMigrationSummary,
  migrationBuilder,
} from "../migration/builder.ts";
export { referencesTo } from "../migration/validators/delete-checks.ts";
export {
  DEFAULT_STATE_RETENTION_RATIO,
  presetDocsPerCollection,
} from "../migration/validators/mock/config.ts";
export {
  type CheckHooks,
  type CheckMigrationReport,
  type CheckReport,
  type CheckStage,
  MAX_DOCS_PER_COLLECTION,
  parseDocsPerCollection,
  parseRetention,
  runCheck,
} from "../migration/cli/commands/check.ts";
export {
  digestWarnings,
  type WarningDigest,
} from "../migration/cli/utils/warning-digest.ts";
export type {
  DatabaseState,
  MigrationDefinition,
  MigrationRule,
  SchemasDefinition,
  TypeSource,
} from "../migration/types.ts";
export { blue, bold, dim, green, red, yellow } from "../utils/colors.ts";
