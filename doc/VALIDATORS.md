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
| `maxLength(n)` on a string | `maxLength: n` | looser on characters outside the BMP |
| `minLength(n)`, `length(n)` on a string, `n` ≥ 2 | `anyOf: [{ minLength: n }, { minLength: ⌈n/2⌉, pattern: <a character outside the BMP> }]` | exact without a character outside the BMP, looser with one |
| `nonEmpty()`, `minLength(1)` on a string | `minLength: 1` | yes |
| `regex`, and actions built on a regex (`uuid`, `email`, …) | `pattern`; flags `i`, `m`, `s` as an inline group `(?ims:…)`; `u`, `g`, `d` dropped | yes, within the JS/PCRE differences below |
| a regex with the `y` or `v` flag | nothing | looser |
| any action on `v.date()` | nothing (`bsonType: "date"` only) | looser |
| `value`, `notValue`, `multipleOf`, `minEntries`, `maxEntries`, `minWords`, `minBytes`, `minGraphemes`, `finite`, `check`, `url`, and the other actions without a regex | nothing | looser |

MongoDB's `$jsonSchema` is draft-4: `exclusiveMinimum` and `exclusiveMaximum`
are booleans that modify `minimum` and `maximum`. A numeric form, or the flag
without its bound, is refused when the collection is created.

Valibot measures a string in UTF-16 code units (`"😀".length === 2`);
`$jsonSchema` counts code points (`"😀"` is 1). A plain `minLength: n` would
refuse `"😀"` under `minLength(2)`, which Valibot accepts. A code point outside
the BMP is two units, so a string holding one has at least `⌈n/2⌉` code
points: the second branch of the `anyOf` admits it, and Valibot enforces the
exact count. For `maxLength`, code points never exceed units, so the plain
keyword is safe.

MongoDB compiles patterns with PCRE, not JavaScript. Two differences stay
stricter than Valibot and are not translated: `\s` and `\w` are ASCII in
PCRE (JavaScript's `\s` matches `\u00a0`), and under `m` PCRE breaks lines on
`\n` only (JavaScript also on `\r`, `\u2028`, `\u2029`). A `\u0041` escape
is refused when the collection is created.

## Actions before a transformation

MongoDB sees the parsed output, not the input. An action placed before a
transformation (`trim`, `toLowerCase`, `transform`, …) checked the input, so
it says nothing about the stored value: `v.pipe(v.string(), v.minLength(5),
v.trim())` accepts `"  ab  "` and stores `"ab"`. Only the actions after the
last transformation reach the validator. `brand`, `flavor` and `readonly` do
not change the value and do not count as transformations.

## Several actions on one value

Bounds on the same side keep the tightest one, with its own exclusivity:

| Pipe | Validator |
| --- | --- |
| `gtValue(5), minValue(10)` | `minimum: 10` |
| `minValue(5), gtValue(5)` | `minimum: 5, exclusiveMinimum: true` |
| `maxValue(5), ltValue(10)` | `maximum: 5` |
| `minLength(3), maxLength(3)` | `anyOf: [{ minLength: 3 }, { minLength: 2, pattern: … }], maxLength: 3` |

Several regexes become one pattern of lookaheads from the start,
`^(?=[\s\S]*?(?:a))(?=[\s\S]*?(?:b))`, so each may match anywhere, as each
Valibot `regex` is tested on its own. A backreference in a regex other than
the first would point at the wrong group once combined; none is rewritten.

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
- **Minimum lengths of strings.** A string `minLength(n)` or `length(n)` with
  `n` ≥ 2 produced `minLength: n`, counted in code points: MongoDB refused a
  string with an emoji that Valibot accepts. Affected: every string field with
  `minLength(n)` or `length(n)`, `n` ≥ 2 — the widest of these changes, since
  most schemas with string lengths have one. The strength for strings without
  such a character is unchanged.
- **Actions placed before a transformation.** They were translated as if they
  checked the stored value, so `minLength(5)` before `trim()` made MongoDB
  refuse the trimmed `"ab"`. Affected: pipes with a validation action before a
  transformation other than `brand`, `flavor` and `readonly`; their validator
  loses those actions.
- **Several regexes on one string, and regex flags.** Two regexes became
  `(?=a)(?=b)`, which requires both to match at the same position: MongoDB
  refused `"abc def"` under `regex(/abc/)` and `regex(/def/)`. The `i`, `m`
  and `s` flags were dropped with a warning, so `v.uuid()` (`/…/iu`) refused
  an uppercase uuid. Affected: string fields with two or more regex-based
  actions, or one with the `i`, `m` or `s` flag (`v.uuid()` among them).
