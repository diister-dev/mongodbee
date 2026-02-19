type AnySchema = import("./schema.ts").BaseSchema<unknown, unknown, import("./schema.ts").BaseIssue<unknown>>;

export type MultiCollectionSchema = Record<string, Record<string, AnySchema>>;

// Type for aggregation pipeline stages
export type AggregationStage = Record<string, unknown>;

export type StageBuilder<T extends MultiCollectionSchema> = {
  match: <E extends keyof T>(
    key: E,
    filter: Record<string, unknown>,
  ) => AggregationStage;
  unwind: <E extends keyof T>(key: E, field: string) => AggregationStage;
  lookup: <E extends keyof T>(
    key: E,
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: (stage: StageBuilder<T>) => AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  /**
   * Lookup without _type constraint - useful for polymorphic references
   * where the ID prefix (e.g., "collaborator:xxx") already guarantees uniqueness.
   * Returns documents from any type in the collection.
   */
  anyLookup: (
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: (stage: StageBuilder<T>) => AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  /**
   * Lookup into an external collection (outside this multi-collection).
   * Useful for joining with other MongoDB collections or other multi-collections.
   */
  externalLookup: (
    fromCollection: string,
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  project: (projection: Record<string, 1 | 0 | string | Record<string, unknown>>) => AggregationStage;
  addFields: (fields: Record<string, unknown>) => AggregationStage;
  group: (grouping: Record<string, unknown>) => AggregationStage;
  sort: (sort: Record<string, 1 | -1>) => AggregationStage;
  limit: (limit: number) => AggregationStage;
  skip: (skip: number) => AggregationStage;
};

/**
 * Creates a StageBuilder instance for building aggregation pipeline stages
 * in a multi-collection context.
 *
 * @param collectionName - The name of the MongoDB collection (used for $lookup)
 * @returns A StageBuilder with helpers for match, lookup, project, etc.
 */
export function createStageBuilder<T extends MultiCollectionSchema>(
  collectionName: string,
): StageBuilder<T> {
  const builder: StageBuilder<T> = {
    match: (matchKey, matchFilter) => ({
      $match: {
        _type: matchKey as string,
        ...matchFilter,
      },
    }),
    unwind: (_unwindKey, field) => ({
      $unwind: `$${field}`,
    }),
    lookup: (lookupKey, localField, foreignField, asOrOptions) => {
      // Simple case: string parameter is the 'as' field name
      // Automatically filter by _type for multi-collection support
      if (typeof asOrOptions === 'string') {
        return {
          $lookup: {
            from: collectionName,
            let: { localValue: `$${localField}` },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: [`$${foreignField}`, "$$localValue"] },
                      { $eq: ["$_type", lookupKey as string] },
                    ],
                  },
                },
              },
            ],
            as: asOrOptions,
          },
        };
      }

      // Advanced case: object with options
      const lookupOptions = asOrOptions || {};
      const as = lookupOptions.as || localField;

      // Build the lookup with automatic _type filter
      const lookupStage: Record<string, unknown> = {
        from: collectionName,
        let: { localValue: `$${localField}`, ...(lookupOptions.let || {}) },
        as,
      };

      // Build pipeline: start with _type match, then add user pipeline if provided
      const basePipeline: AggregationStage[] = [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: [`$${foreignField}`, "$$localValue"] },
                { $eq: ["$_type", lookupKey as string] },
              ],
            },
          },
        },
      ];

      // Add user-provided pipeline stages after the base filter
      if (lookupOptions.pipeline) {
        const userPipeline = lookupOptions.pipeline(createStageBuilder<T>(collectionName));
        basePipeline.push(...userPipeline);
      }

      lookupStage.pipeline = basePipeline;

      return { $lookup: lookupStage };
    },
    anyLookup: (localField, foreignField, asOrOptions) => {
      // Simple case: string parameter is the 'as' field name
      // No _type filter - matches any document type
      if (typeof asOrOptions === 'string') {
        return {
          $lookup: {
            from: collectionName,
            localField,
            foreignField,
            as: asOrOptions,
          },
        };
      }

      // Advanced case: object with options
      const anyLookupOptions = asOrOptions || {};
      const as = anyLookupOptions.as || localField;
      const anyLookupStage: Record<string, unknown> = {
        from: collectionName,
        localField,
        foreignField,
        as,
      };

      // Add let variables if provided
      if (anyLookupOptions.let) {
        anyLookupStage.let = anyLookupOptions.let;
      }

      // Add pipeline if provided (execute the builder function)
      if (anyLookupOptions.pipeline) {
        anyLookupStage.pipeline = anyLookupOptions.pipeline(createStageBuilder<T>(collectionName));
      }

      return { $lookup: anyLookupStage };
    },
    externalLookup: (fromCollection, localField, foreignField, asOrOptions) => {
      // Simple case: string parameter is the 'as' field name
      if (typeof asOrOptions === 'string') {
        return {
          $lookup: {
            from: fromCollection,
            localField,
            foreignField,
            as: asOrOptions,
          },
        };
      }

      // Advanced case: object with options
      const extLookupOptions = asOrOptions || {};
      const as = extLookupOptions.as || localField;
      const extLookupStage: Record<string, unknown> = {
        from: fromCollection,
        localField,
        foreignField,
        as,
      };

      // Add let variables if provided
      if (extLookupOptions.let) {
        extLookupStage.let = extLookupOptions.let;
      }

      // Add pipeline if provided (raw pipeline, not using StageBuilder)
      if (extLookupOptions.pipeline) {
        extLookupStage.pipeline = extLookupOptions.pipeline;
      }

      return { $lookup: extLookupStage };
    },
    project: (projection) => ({
      $project: projection,
    }),
    addFields: (fields) => ({
      $addFields: fields,
    }),
    group: (grouping) => ({
      $group: grouping,
    }),
    sort: (sortSpec) => ({
      $sort: sortSpec,
    }),
    limit: (limitVal) => ({
      $limit: limitVal,
    }),
    skip: (skipVal) => ({
      $skip: skipVal,
    }),
  };

  return builder;
}
