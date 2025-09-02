/**
 * A very dirty deep equals implementation using JSON.stringify.
 */
export function dirtyEquals(a: unknown, b: unknown): boolean {
    try {
        return JSON.stringify(a) === JSON.stringify(b);
    } catch {
        return false;
    }
}

/**
 * A dirty deep equals implementation that is insensitive to key order.
 */
export function dirtyEquivalent(a: unknown, b: unknown): boolean {
    return dirtyEquals(deepSortObject(a), deepSortObject(b));
}

/**
 * Deep sorts an object by its keys.
 * Arrays are not sorted, only their elements are deep sorted.
 */
export function deepSortObject(obj: any): any {
    const t = typeof obj;
    if(t !== 'object') return obj;

    // Only sort keys
    if(Array.isArray(obj)) {
        return obj.map(deepSortObject);
    }

    const sorted: Record<string, any> = {};
    Object.keys(obj).sort().forEach(key => {
        sorted[key] = deepSortObject(obj[key]);
    });
    return sorted;
}