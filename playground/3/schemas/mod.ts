import { dbId } from "mongodbee";
import * as v from "valibot";

export const schemas = {
    collections: {
        "+users": {
          _id: dbId("user"),
          firstname: v.string(),
          lastname: v.string(),
        },
        "+articles": {
          _id: dbId("article"),
          title: v.string(),
          content: v.string(),
        },
        "+groups": {
            _id: dbId("group"),
            name: v.string(),
        }
    }
}