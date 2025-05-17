type HandleArray<T, K extends string = "", MAX extends keyof DECREMENT = 10> = MAX extends 0 ? never :
    T extends any[] ?
        { path: K, value: T }
        | { path: `${K}.${number}`, value: T[number] }
        | { path: `${K}.$[]`, value: T[number] }
        | NodesType<T[number], `${K}.$[]`, DECREMENT[MAX]>
        | NodesType<T[number], `${K}.${number}`, DECREMENT[MAX]>
    : never;

type HandleRecord<T, K extends string = "", MAX extends keyof DECREMENT = 10> = MAX extends 0 ? never :
    T extends Record<string, any> ?
        (K extends "" ? never : { path: K, value: T })
        | {
            [k in keyof T]: NodesType<T[k], `${K}${K extends "" ? "" : "."}${k & string}`, DECREMENT[MAX]>
        }[keyof T] extends infer U ? U extends undefined ? never : U : never
    : never;

export type NodesType<V, K extends string = "", MAX extends keyof DECREMENT = 10> = MAX extends 0 ? never :
    V extends (infer T)[] ? HandleArray<T[], K, DECREMENT[MAX]>
    : V extends Record<string, infer T> ? HandleRecord<V, K, DECREMENT[MAX]>
    : { path: K, value: V };

export type FlatKey<T extends Record<string, any>> = NodesType<T> extends infer U ?
    U extends { path: infer P } ? P : never : never;

export type FlatType<T> = T extends Record<string, any> ? NodesType<T> extends infer U ? {
    [k in FlatKey<T>]: U extends { path: k, value: infer V } ? V : never
} : never : never;

// System to prevent infinite recursion
// DECREMENT is a type that maps numbers to their decremented values
type DECREMENT = {
    0: 0,
    1: 0,
    2: 1,
    3: 2,
    4: 3,
    5: 4,
    6: 5,
    7: 6,
    8: 7,
    9: 8,
    10: 9,
    11: 10,
    12: 11,
    13: 12,
    14: 13,
    15: 14,
    16: 15,
    17: 16,
    18: 17,
    19: 18,
    20: 19,
    21: 20,
    22: 21,
    23: 22,
    24: 23,
    25: 24,
    26: 25,
    27: 26,
    28: 27,
    29: 28,
    30: 29,
}