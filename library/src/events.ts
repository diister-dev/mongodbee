/**
 * Creates a type-safe event emitter system
 *
 * This utility creates a strongly-typed event emitter that ensures type consistency
 * between event names and their respective callback signatures. It provides methods
 * for registering, removing, and triggering event listeners with proper TypeScript typing.
 *
 * @template T - An object type where keys are event names and values are callback function signatures
 * @returns A type-safe event emitter object with methods for managing event listeners
 * @example
 * ```typescript
 * // Define event types
 * type MyEvents = {
 *   'userCreated': (userId: string, username: string) => void;
 *   'dataUpdated': (newData: Record<string, unknown>) => void;
 * };
 *
 * // Create typed event emitter
 * const events = EventEmitter<MyEvents>();
 *
 * // Register event listener with type checking
 * events.on('userCreated', (userId, username) => {
 *   console.log(`User ${username} (${userId}) was created`);
 * });
 *
 * // Trigger event with type-checked parameters
 * events.call('userCreated', 'id123', 'johndoe');
 * ```
 */
export function EventEmitter<
  T extends { [key: string]: (...params: any[]) => void },
>(): {
  on<E extends keyof T>(event: E, callback: T[E]): () => void;
  off<E extends keyof T>(event: E, callback: T[E]): void;
  call<E extends keyof T>(event: E, ...params: Parameters<T[E]>): void;
  expose(): {
    on<E extends keyof T>(event: E, callback: T[E]): () => void;
    off<E extends keyof T>(event: E, callback: T[E]): void;
  };
} {
  const events: {
    [key in keyof T]?: (T[key])[];
  } = {};

  function on<E extends keyof T>(event: E, callback: T[E]) {
    if (!events[event]) events[event] = []; // Create callback
    events[event]?.push(callback);

    // Return a function to remove the callback
    return () => off(event, callback);
  }

  function off<E extends keyof T>(event: E, callback: T[E]) {
    if (!events[event]) return; //No callback to remove
    //Remove it
    const indexOf = events[event]?.indexOf(callback) ?? -1;
    events[event]?.splice(indexOf, 1);
    if (events[event]?.length == 0) delete events[event]; //Remove element if there is no callback
  }

  function call<E extends keyof T>(event: E, ...params: Parameters<T[E]>) {
    if (!events[event]) return; //No callback to call
    for (let i = 0; i < (events[event]?.length ?? 0); i++) {
      events[event]?.[i](...params);
    }
  }

  function expose() {
    return { on, off };
  }

  return {
    on,
    off,
    call,
    expose,
  };
}
