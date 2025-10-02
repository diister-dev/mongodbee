/**
 * @fileoverview Migration chain validation for ensuring migration integrity
 * 
 * This module provides comprehensive validation for migration chains to ensure:
 * - Parent-child relationships are valid and form a proper chain
 * - No circular dependencies or broken links exist
 * - Migration IDs are unique and properly formatted
 * - Chain consistency and completeness
 * 
 * @example
 * ```typescript
 * import { validateMigrationChain, createChainValidator } from "@diister/mongodbee/migration/validators";
 * 
 * const validator = createChainValidator();
 * const migrations = [
 *   { id: '001', parent: null, operations: [] },
 *   { id: '002', parent: '001', operations: [] },
 *   { id: '003', parent: '002', operations: [] }
 * ];
 * 
 * const result = await validator.validateChain(migrations);
 * if (!result.isValid) {
 *   console.error('Chain validation failed:', result.errors);
 * }
 * ```
 * 
 * @module
 */

import type { MigrationDefinition } from '../types.ts';

/**
 * Result of migration chain validation
 */
export interface ChainValidationResult {
  /** Whether the chain is valid */
  isValid: boolean;
  
  /** List of validation errors found */
  errors: string[];
  
  /** List of validation warnings */
  warnings: string[];
  
  /** Additional metadata about the chain */
  metadata: {
    /** Total number of migrations in the chain */
    totalMigrations: number;
    
    /** Number of root migrations (no parent) */
    rootMigrations: number;
    
    /** Number of leaf migrations (no children) */
    leafMigrations: number;
    
    /** Maximum depth of the chain */
    maxDepth: number;
    
    /** List of migration IDs in topological order */
    topologicalOrder: string[];
  };
}

/**
 * Configuration options for chain validation
 */
export interface ChainValidatorOptions {
  /** Whether to allow multiple root migrations (default: false) */
  allowMultipleRoots?: boolean;
  
  /** Whether to allow multiple leaf migrations (default: true) */
  allowMultipleLeaves?: boolean;
  
  /** Maximum allowed chain depth (default: unlimited) */
  maxDepth?: number;
  
  /** Whether to perform strict ID format validation */
  strictIdFormat?: boolean;
  
  /** Whether to validate operation integrity within migrations */
  validateOperations?: boolean;
}

/**
 * Migration chain validator for ensuring integrity and consistency
 */
export class ChainValidator {
  constructor(private options: ChainValidatorOptions = {}) {
    // Set defaults
    this.options = {
      allowMultipleRoots: false,
      allowMultipleLeaves: true,
      strictIdFormat: true,
      validateOperations: true,
      ...options,
    };
  }

  /**
   * Validates a migration chain for integrity and consistency
   * 
   * @param migrations - Array of migration definitions to validate
   * @returns Validation result with errors, warnings, and metadata
   * 
   * @example
   * ```typescript
   * const validator = new ChainValidator({ allowMultipleRoots: false });
   * const result = validator.validateChain(migrations);
   * 
   * if (result.isValid) {
   *   console.log('Chain is valid. Topological order:', result.metadata.topologicalOrder);
   * } else {
   *   console.error('Validation errors:', result.errors);
   * }
   * ```
   */
  validateChain(migrations: readonly MigrationDefinition[]): ChainValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];
    
    // Basic validations
    this.validateBasicStructure(migrations, errors);
    this.validateUniqueIds(migrations, errors);
    this.validateParentReferences(migrations, errors);
    
    // Advanced validations
    this.validateCircularDependencies(migrations, errors);
    const { roots, leaves, depth, topologicalOrder } = this.analyzeChainStructure(migrations, errors, warnings);
    
    // Configuration-based validations
    this.validateRootsAndLeaves(roots, leaves, errors, warnings);
    this.validateDepth(depth, errors);
    
    // Operation validations (if enabled)
    if (this.options.validateOperations) {
      this.validateOperationIntegrity(migrations, errors, warnings);
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings,
      metadata: {
        totalMigrations: migrations.length,
        rootMigrations: roots.length,
        leafMigrations: leaves.length,
        maxDepth: depth,
        topologicalOrder,
      }
    };
  }

  /**
   * Validates basic structure requirements
   * 
   * @private
   */
  private validateBasicStructure(migrations: readonly MigrationDefinition[], errors: string[]): void {
    if (migrations.length === 0) {
      errors.push('Migration chain cannot be empty');
      return;
    }

    for (const migration of migrations) {
      if (!migration.id || typeof migration.id !== 'string') {
        errors.push(`Migration missing or invalid ID: ${JSON.stringify(migration.id)}`);
      }

      if (this.options.strictIdFormat && migration.id) {
        if (!/^[a-zA-Z0-9_@-]+$/.test(migration.id)) {
          errors.push(`Migration ID "${migration.id}" contains invalid characters. Use only letters, numbers, underscores, hyphens, and @`);
        }
      }

      if (!migration.name || typeof migration.name !== 'string') {
        errors.push(`Migration "${migration.id}" must have a name`);
      }

      if (typeof migration.migrate !== 'function') {
        errors.push(`Migration "${migration.id}" must have a migrate function`);
      }

      if (migration.parent !== null && (!migration.parent || typeof migration.parent !== 'object')) {
        errors.push(`Migration "${migration.id}" has invalid parent reference: ${JSON.stringify(migration.parent)}`);
      }
    }
  }

  /**
   * Validates that all migration IDs are unique
   * 
   * @private
   */
  private validateUniqueIds(migrations: readonly MigrationDefinition[], errors: string[]): void {
    const idSet = new Set<string>();
    const duplicates = new Set<string>();

    for (const migration of migrations) {
      if (migration.id) {
        if (idSet.has(migration.id)) {
          duplicates.add(migration.id);
        } else {
          idSet.add(migration.id);
        }
      }
    }

    for (const duplicate of duplicates) {
      errors.push(`Duplicate migration ID found: "${duplicate}"`);
    }
  }

  /**
   * Validates parent references point to existing migrations
   * 
   * @private
   */
  private validateParentReferences(migrations: readonly MigrationDefinition[], errors: string[]): void {
    const idSet = new Set(migrations.map(m => m.id).filter(id => id));

    for (const migration of migrations) {
      if (migration.parent && !idSet.has(migration.parent.id)) {
        errors.push(`Migration "${migration.id}" references non-existent parent "${migration.parent.id}"`);
      }
    }
  }

  /**
   * Validates there are no circular dependencies
   * 
   * @private
   */
  private validateCircularDependencies(migrations: readonly MigrationDefinition[], errors: string[]): void {
    const parentMap = new Map<string, string | null>();
    
    for (const migration of migrations) {
      if (migration.id) {
        parentMap.set(migration.id, migration.parent?.id || null);
      }
    }

    // Check each migration for cycles using DFS
    for (const migration of migrations) {
      if (!migration.id) continue;

      const visited = new Set<string>();
      const stack = new Set<string>();
      
      if (this.hasCycle(migration.id, parentMap, visited, stack)) {
        errors.push(`Circular dependency detected involving migration "${migration.id}"`);
        break; // One error message is enough for cycles
      }
    }
  }

  /**
   * Helper method to detect cycles using DFS
   * 
   * @private
   */
  private hasCycle(
    migrationId: string,
    parentMap: Map<string, string | null>,
    visited: Set<string>,
    stack: Set<string>
  ): boolean {
    if (stack.has(migrationId)) {
      return true; // Found a cycle
    }
    
    if (visited.has(migrationId)) {
      return false; // Already processed
    }

    visited.add(migrationId);
    stack.add(migrationId);

    const parent = parentMap.get(migrationId);
    if (parent && this.hasCycle(parent, parentMap, visited, stack)) {
      return true;
    }

    stack.delete(migrationId);
    return false;
  }

  /**
   * Analyzes chain structure and builds topological order
   * 
   * @private
   */
  private analyzeChainStructure(
    migrations: readonly MigrationDefinition[],
    _errors: string[],
    _warnings: string[]
  ): {
    roots: string[];
    leaves: string[];
    depth: number;
    topologicalOrder: string[];
  } {
    const migrationMap = new Map<string, MigrationDefinition>();
    const childrenMap = new Map<string, Set<string>>();
    
    // Build maps
    for (const migration of migrations) {
      if (!migration.id) continue;
      
      migrationMap.set(migration.id, migration);
      
      if (migration.parent) {
        if (!childrenMap.has(migration.parent.id)) {
          childrenMap.set(migration.parent.id, new Set());
        }
        childrenMap.get(migration.parent.id)!.add(migration.id);
      }
    }

    // Find roots (no parent) and leaves (no children)
    const roots: string[] = [];
    const leaves: string[] = [];
    
    for (const migration of migrations) {
      if (!migration.id) continue;
      
      if (!migration.parent) {
        roots.push(migration.id);
      }
      
      if (!childrenMap.has(migration.id) || childrenMap.get(migration.id)!.size === 0) {
        leaves.push(migration.id);
      }
    }

    // Calculate maximum depth and build topological order
    let maxDepth = 0;
    const topologicalOrder: string[] = [];
    const depthMap = new Map<string, number>();

    // Calculate depths using DFS from roots
    for (const root of roots) {
      const depth = this.calculateDepth(root, childrenMap, depthMap, 0);
      maxDepth = Math.max(maxDepth, depth);
    }

    // Build topological order by processing migrations level by level
    this.buildTopologicalOrder(roots, childrenMap, topologicalOrder);

    return {
      roots,
      leaves,
      depth: maxDepth,
      topologicalOrder,
    };
  }

  /**
   * Calculates maximum depth from a given migration
   * 
   * @private
   */
  private calculateDepth(
    migrationId: string,
    childrenMap: Map<string, Set<string>>,
    depthMap: Map<string, number>,
    currentDepth: number
  ): number {
    if (depthMap.has(migrationId)) {
      return depthMap.get(migrationId)!;
    }

    let maxChildDepth = currentDepth;
    const children = childrenMap.get(migrationId) || new Set();
    
    for (const child of children) {
      const childDepth = this.calculateDepth(child, childrenMap, depthMap, currentDepth + 1);
      maxChildDepth = Math.max(maxChildDepth, childDepth);
    }

    depthMap.set(migrationId, maxChildDepth);
    return maxChildDepth;
  }

  /**
   * Builds topological order using BFS
   * 
   * @private
   */
  private buildTopologicalOrder(
    roots: string[],
    childrenMap: Map<string, Set<string>>,
    topologicalOrder: string[]
  ): void {
    const queue = [...roots];
    const visited = new Set<string>();

    while (queue.length > 0) {
      const current = queue.shift()!;
      
      if (visited.has(current)) {
        continue;
      }
      
      visited.add(current);
      topologicalOrder.push(current);
      
      const children = childrenMap.get(current) || new Set();
      for (const child of children) {
        if (!visited.has(child)) {
          queue.push(child);
        }
      }
    }
  }

  /**
   * Validates root and leaf configurations
   * 
   * @private
   */
  private validateRootsAndLeaves(
    roots: string[],
    leaves: string[],
    errors: string[],
    warnings: string[]
  ): void {
    if (roots.length === 0) {
      errors.push('Migration chain must have at least one root migration (with parent: null)');
    } else if (roots.length > 1 && !this.options.allowMultipleRoots) {
      errors.push(`Multiple root migrations found: ${roots.join(', ')}. Set allowMultipleRoots: true if this is intended`);
    }

    if (leaves.length === 0) {
      warnings.push('No leaf migrations found. This may indicate a complex branching structure');
    } else if (leaves.length > 1 && !this.options.allowMultipleLeaves) {
      errors.push(`Multiple leaf migrations found: ${leaves.join(', ')}. Set allowMultipleLeaves: true if this is intended`);
    }
  }

  /**
   * Validates chain depth against configured limits
   * 
   * @private
   */
  private validateDepth(depth: number, errors: string[]): void {
    if (this.options.maxDepth && depth > this.options.maxDepth) {
      errors.push(`Migration chain depth ${depth} exceeds maximum allowed depth ${this.options.maxDepth}`);
    }
  }

  /**
   * Validates operation integrity within migrations
   * 
   * Note: This performs basic validation of migration structure.
   * For detailed operation validation, the migration would need to be executed
   * with a simulation applier to validate the actual operations.
   * 
   * @private
   */
  private validateOperationIntegrity(
    migrations: readonly MigrationDefinition[],
    errors: string[],
    warnings: string[]
  ): void {
    for (const migration of migrations) {
      if (!migration.id) continue;

      // Validate migration structure
      if (!migration.migrate || typeof migration.migrate !== 'function') {
        errors.push(`Migration "${migration.id}" missing or invalid migrate function`);
        continue;
      }

      if (!migration.schemas || typeof migration.schemas !== 'object') {
        warnings.push(`Migration "${migration.id}" missing or invalid schemas definition`);
      }

      // Basic validation - we could attempt to call the migrate function
      // with a mock builder to validate operations, but that's complex
      // and might have side effects. For now, we just validate structure.
    }
  }
}

/**
 * Factory function to create a chain validator
 * 
 * @param options - Configuration options for the validator
 * @returns A new chain validator instance
 * 
 * @example
 * ```typescript
 * import { createChainValidator } from "@diister/mongodbee/migration/validators";
 * 
 * const validator = createChainValidator({
 *   allowMultipleRoots: false,
 *   strictIdFormat: true,
 *   maxDepth: 100
 * });
 * ```
 */
export function createChainValidator(options?: ChainValidatorOptions): ChainValidator {
  return new ChainValidator(options);
}

/**
 * Convenience function to validate a migration chain
 * 
 * @param migrations - Array of migration definitions to validate
 * @param options - Optional validator configuration
 * @returns Validation result
 * 
 * @example
 * ```typescript
 * import { validateMigrationChain } from "@diister/mongodbee/migration/validators";
 * 
 * const result = validateMigrationChain(migrations, {
 *   allowMultipleRoots: false
 * });
 * 
 * if (!result.isValid) {
 *   throw new Error(`Chain validation failed: ${result.errors.join(', ')}`);
 * }
 * ```
 */
export function validateMigrationChain(
  migrations: readonly MigrationDefinition[],
  options?: ChainValidatorOptions
): ChainValidationResult {
  const validator = createChainValidator(options);
  return validator.validateChain(migrations);
}