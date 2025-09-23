/**
 * User Schema Example
 * 
 * This is an example schema file to demonstrate MongoDBee's
 * schema validation system integration with migrations
 */

import * as v from "valibot";

/**
 * User document schema
 */
export const UserSchema = v.object({
  _id: v.optional(v.any()), // MongoDB ObjectId
  username: v.string([v.minLength(3), v.maxLength(50)]),
  email: v.string([v.email()]),
  createdAt: v.date(),
  updatedAt: v.date(),
  profile: v.optional(v.object({
    firstName: v.optional(v.string()),
    lastName: v.optional(v.string()),
    bio: v.optional(v.string([v.maxLength(500)])),
  })),
  settings: v.optional(v.object({
    theme: v.picklist(['light', 'dark']),
    notifications: v.boolean(),
  })),
});

/**
 * Type inference for User
 */
export type User = v.InferInput<typeof UserSchema>;

/**
 * Product schema example
 */
export const ProductSchema = v.object({
  _id: v.optional(v.any()),
  name: v.string([v.minLength(1), v.maxLength(200)]),
  description: v.string([v.maxLength(2000)]),
  price: v.number([v.minValue(0)]),
  category: v.string(),
  inStock: v.boolean(),
  tags: v.array(v.string()),
  createdAt: v.date(),
  updatedAt: v.date(),
});

export type Product = v.InferInput<typeof ProductSchema>;