import type * as m from "mongodb";

/**
 * Type for change event callback to avoid 'any' usage
 */
type ChangeEventCallback = (
  change: m.ChangeStreamDocument<m.BSON.Document>,
) => void;

/**
 * Content structure for managing database watchers
 * @internal
 */
type OneWatcherContent = {
  /** The MongoDB database being watched */
  db: m.Db;
  /** Global database event listeners */
  listener: ChangeEventCallback[];
  /** Collection-specific event listeners, indexed by collection name */
  namespaceListener: Record<string, ChangeEventCallback[]>;
  /** The MongoDB change stream instance */
  changeStream: m.ChangeStream<m.BSON.Document>;
};

/**
 * Map to store database watchers, using weak references to allow garbage collection
 * @internal
 */
const watchingMap = new WeakMap<m.Db, OneWatcherContent>();

/**
 * Sets up a MongoDB change stream watcher for a database and registers callbacks for events.
 * The function efficiently reuses existing change streams when possible to avoid creating multiple
 * streams for the same database.
 *
 * @param db - The MongoDB database to watch for changes
 * @param collection - The MongoDB collection to associate with this watcher
 * @param callback - The callback function to execute when changes occur
 * @returns A cleanup function that removes the registered callback when called
 *
 * @example
 * ```typescript
 * // Setup a watcher
 * const unsubscribe = watchEvent(db, collection, (change) => {
 *   if (change.operationType === "insert") {
 *     console.log("Document inserted:", change.fullDocument);
 *   }
 * });
 *
 * // Later, when no longer needed
 * unsubscribe();
 * ```
 */
export function watchEvent<TSchema extends m.Document = m.Document>(
  db: m.Db,
  collection: m.Collection<TSchema>,
  callback: ChangeEventCallback,
): () => void {
  // Try to get existing watchers for this database
  let watchers = watchingMap.get(db);

  if (!watchers) {
    // No existing watcher, create a new change stream
    const changeStream = db.watch();
    changeStream.on("change", (change) => {
      const watchers = watchingMap.get(db);
      // MongoDB change stream namespace info contains the collection name
      const collectionName = (change as unknown as { ns?: { coll?: string } })
        .ns?.coll;

      if (collectionName) {
        // Collection-specific event, notify collection watchers
        const namespaceWatchers = watchers?.namespaceListener[collectionName];
        if (namespaceWatchers) {
          for (const watcher of namespaceWatchers) {
            watcher(change);
          }
        }
      } else {
        // Database-level event, notify all watchers
        if (watchers?.listener) {
          for (const watcher of watchers.listener) {
            watcher(change);
          }
        }
      }

      // Clean up if the database was dropped
      if (change.operationType === "dropDatabase") {
        changeStream.close();
        watchingMap.delete(db);
      }
    });

    // Create watchers container
    watchers = {
      db,
      listener: [callback],
      namespaceListener: {
        [collection.collectionName]: [callback],
      },
      changeStream,
    };

    // Register the change stream to be closed when the watchers are garbage collected
    new FinalizationRegistry((watchers: OneWatcherContent) => {
      watchers.changeStream.close();
      watchingMap.delete(watchers.db);
    }).register(db, watchers);

    watchingMap.set(db, watchers);
  } else {
    // Reuse existing watcher, just add the callback
    watchers.listener.push(callback);

    // Initialize collection namespace if needed
    if (!watchers.namespaceListener[collection.collectionName]) {
      watchers.namespaceListener[collection.collectionName] = [];
    }

    // Add callback to collection namespace
    watchers.namespaceListener[collection.collectionName].push(callback);
  }

  // Return a function to unsubscribe this callback
  return () => {
    const watchers = watchingMap.get(db);
    if (watchers) {
      // Remove from global listeners
      watchers.listener = watchers.listener.filter((l) => l !== callback);

      // Remove from collection-specific listeners
      const namespace = watchers.namespaceListener[collection.collectionName];
      if (namespace) {
        watchers.namespaceListener[collection.collectionName] = namespace
          .filter((l) => l !== callback);
      }

      // If no listeners left, clean up
      if (watchers.listener.length === 0) {
        watchers.changeStream.close();
        watchingMap.delete(db);
      }
    }
  };
}

/**
 * Closes all change streams for a given database and cleans up resources
 * This should be called when closing or dropping a database to prevent resource leaks
 *
 * @param db - The MongoDB database to clean up watchers for
 */
export function closeAllWatchers(db: m.Db): void {
  const watchers = watchingMap.get(db);
  if (watchers) {
    watchers.changeStream.close();
    watchingMap.delete(db);
  }
}
