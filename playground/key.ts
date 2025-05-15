import * as v from "valibot"
import { FlatType } from "../library/types/flat.ts";

const schema = v.object({
    hello: v.string(),
    world: v.number(),
    deep: v.object({
        d: v.array(v.object({
            f: v.string(),
        })),
        key: v.string(),
        a: v.optional(v.object({
            b: v.string(),
            c: v.object({
                d: v.string(),
                e: v.array(v.string()),
            }),
            d: v.array(v.object({
                f: v.string(),
            })),
        })),
    }),
})

type Output = v.InferOutput<typeof schema>;

// type OutputNodes = NodesType<Output>['path'];
type FlatOutput = Partial<FlatType<Output>>;

// type FlatOutput2 = FlatType2<Output>;

let a2: Partial<FlatOutput> = {
    
}

type Uh = string[] extends Record<string, any> ? true : false;

type KeyFullPathElement = string | ((v: string) => boolean);
type KeyFullPath = KeyFullPathElement[];

function checkPath(fullPath: KeyFullPath, p: unknown) {
    if (typeof p !== 'string') return false;
    const split = p.split('.');
    if (split.length !== fullPath.length) return false;
    for (let i = 0; i < split.length; i++) {
        const k = fullPath[i];
        if (typeof k === 'string') {
            if (k !== split[i]) return false;
        } else if (typeof k === 'function') {
            if (!k(split[i])) return false;
        } else {
            return false;
        }
    }
    
    return true;
}

// console.log(schema);
const entries: any[] = [];
let toTreat: any[] = [{ key: [], value: schema }];

// TODO:
// Support array syntax
// nested.$[].other.value
// nested.${number}.other.value
// Maybe use `intersect`
while (toTreat.length) {
    const { key, value } = toTreat.pop()!;
    
    if(value.type === "object") {
        for(const k in value.entries) {
            const v = value.entries[k as keyof typeof value.entries];
            toTreat.push({
                key: [...key, k],
                value: v,
            });
        }
    } else if (value.type === "array") {
        toTreat.push({
            key: [...key, `$[]`],
            value: value.item,
        });

        toTreat.push({
            key: [...key, (v: string) => !isNaN(Number(v))],
            value: value.item,
        });
    } else {
        // console.log("TODO", value.type, key);
    }
    
    if(key.length > 0) {
        console.log(key);
        entries.push([key, value]);
    }
}

const schema2 = v.custom((input) => {
    if(typeof input !== 'object' || input === null) return false;
    for(const key in input) {
        const value = input[key as keyof typeof input];
        const found = entries.find(([fullPath, _]) => checkPath(fullPath, key));
        if(!found) {
            continue;
        }
        
        const [fullPath, schema] = found;
        const result = v.safeParse(schema, value);
        if(!result.success) {
            return false;
        }
    }
    return true;
});

console.log(schema2);

const parsed = v.safeParse(schema2, {
    // 'hello': 'world',
    // 'deep.d.1.f': "10",
    // world: 42,
    "deep.d" : [
        { f: "test" },
        { f: "test2" }
    ]
})

console.log(parsed.success, parsed.output);