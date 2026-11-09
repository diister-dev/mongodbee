# The database validator

Every collection carries a MongoDB `$jsonSchema` validator generated from its
Valibot schema by `toMongoValidator`. With `schemaManagement: "auto"` it is
written (`create` or `collMod`) when the collection opens; with `"managed"` it
is written by the migration applier after every migration and by
`mongodbee sync`.

The validator is a second line, not the first: every write is parsed by the
Valibot schema before it reaches MongoDB. So the validator follows one rule:

**It never rejects a value the schema accepts.** Where `$jsonSchema` cannot say
exactly what an action checks, the validator says less and Valibot enforces
the rest. A validator stricter than the schema fails writes in production
only, where the validator is in force, on values every test accepted.

## What is translated

| Valibot | `$jsonSchema` | Exact |
| --- | --- | --- |
| `minValue(n)`, `maxValue(n)` on a number | `minimum`, `maximum` | yes |
| `gtValue(n)`, `ltValue(n)` on a number | `minimum` + `exclusiveMinimum: true`, `maximum` + `exclusiveMaximum: true` | yes |
| `minValue`, `maxValue`, `gtValue`, `ltValue` with a non-numeric requirement (string, `Date`, bigint) | nothing | looser |
| `integer()` | `multipleOf: 1` | yes |
| `minLength`, `maxLength`, `length`, `nonEmpty` on an array | `minItems`, `maxItems` | yes |
| any action on `v.date()` | nothing (`bsonType: "date"` only) | looser |
| `value`, `notValue`, `multipleOf`, `minEntries`, `maxEntries`, `minWords`, `minBytes`, `minGraphemes`, `finite`, `check`, `url`, and the other actions without a regex | nothing | looser |

MongoDB's `$jsonSchema` is draft-4: `exclusiveMinimum` and `exclusiveMaximum`
are booleans that modify `minimum` and `maximum`. A numeric form, or the flag
without its bound, is refused when the collection is created.

## Several actions on one value

Bounds on the same side keep the tightest one, with its own exclusivity:

| Pipe | Validator |
| --- | --- |
| `gtValue(5), minValue(10)` | `minimum: 10` |
| `minValue(5), gtValue(5)` | `minimum: 5, exclusiveMinimum: true` |
| `maxValue(5), ltValue(10)` | `maximum: 5` |
| `minLength(2), nonEmpty()` | `minLength: 2` |

The order of the actions does not change the validator.

## Changes to generated validators

Each change below rewrites the validator of the collections it affects: with
`"auto"`, a `collMod` when the collection next opens; with `"managed"`, at the
next `mongodbee migrate` or `mongodbee sync`, and until then the studio's Drift
tab lists them. Collections whose schemas use none of these forms keep their
validator byte for byte.

- **Two bounds on one side of a number** (issue #51). `gtValue(5)` with
  `minValue(10)` produced `minimum: 10, exclusiveMinimum: true`: MongoDB
  refused `10`, which Valibot accepts. Affected: a number pipe holding a
  `gtValue` and a `minValue` (or an `ltValue` and a `maxValue`) where the
  inclusive bound is the tighter one. A non-numeric bound (`minValue("b")` on a
  string) produced a validator MongoDB refused outright; it is now left to
  Valibot.
