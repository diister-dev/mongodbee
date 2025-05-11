/**
 * Utility to create a safe event system
 */

export function EventEmitter<T extends {[key: string] : (...params: any[]) => void }>(): {
  on<E extends keyof T>(event: E, callback: T[E]): () => void;
  off<E extends keyof T>(event: E, callback: T[E]): void;
  call<E extends keyof T>(event: E, ...params: Parameters<T[E]>): void;
  expose(): {
    on<E extends keyof T>(event: E, callback: T[E]): () => void;
    off<E extends keyof T>(event: E, callback: T[E]): void;
  };
} {
  const events: {
    [key in keyof T]?: (T[key])[]
  } = {};

  function on<E extends keyof T>(event: E, callback : T[E]){
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
    return { on, off }
  }

  return {
    on,
    off,
    call,
    expose,
  }
}