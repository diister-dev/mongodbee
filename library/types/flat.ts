// // Improve the speed
// // -> Create directly the final object
export type NodesType<V, K extends string = "", MAX extends keyof DECREMENT = 10> = MAX extends 0 ? never :
    V extends any[] ?
    { path: K, value: V } | NodesType<V[number], `${K}.$[]`, DECREMENT[MAX]> | NodesType<V[number], `${K}.${number}`, DECREMENT[MAX]>
    : V extends Record<string, any> ?
    {
        [k in keyof V]: NodesType<V[k], `${K}${K extends "" ? "" : "."}${k & string}`, DECREMENT[MAX]>
    }[keyof V] extends infer U ? U extends undefined ? never : U : never
    : { path: K, value: V };

export type FlatKey<T extends Record<string, any>> = NodesType<T> extends infer U ?
    U extends { path: infer P } ? P : never : never;

export type FlatType<T extends Record<string, any>> = {
    [k in FlatKey<T>]: NodesType<T> extends infer U ? U extends { path: k, value: infer V } ? V : never : never
}

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