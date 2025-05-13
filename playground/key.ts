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

// console.log(schema);
const entries: v.ObjectSchema<any, any>[] = [];

let toTreat: any[] = [{ key: "", value: schema }];

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
                key: `${key}${key ? "." : ""}${k}`,
                value: v,
            });
        }
    } else if (value.type === "array") {
        toTreat.push({
            key: `${key}${key ? "." : ""}$[]`,
            value: value.item,
        });
    }
    
    if(key !== "") {
        entries.push(v.partial(v.object({
            [key]: value,
        })))
    }
}

const schema2 = v.partial(v.intersect(entries));

// console.log(schema2);

const parsed = v.safeParse(schema2, {
    'deep.d.$[].f': '10'
})

console.log(parsed.success);