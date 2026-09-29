# MongoDBee Studio

## Overview

`mongodbee studio` starts a local web explorer for a database managed by
MongoDBee, in the spirit of Drizzle Studio. It reads the same
`mongodbee.config.ts` as the other CLI commands, so it knows the declared
collections, their types and scopes, the declared indexes and the migration
chain, and shows them next to what is actually stored.

By default the studio is **read-only**: the server exposes GET endpoints only,
and every other method is refused with `405`. Editing is opt-in with
`--write` (see [Editing](#editing)).

## Installing

The studio is its own npm package, `@diister/mongodbee-studio`, so projects
that never open it do not install its server or its UI. Install it next to
`@diister/mongodbee`, with the same version: the studio depends on that exact
version of the core, and both are released together.

```bash
npm install --save-dev @diister/mongodbee-studio
bun add --dev @diister/mongodbee-studio
deno add --dev npm:@diister/mongodbee-studio
```

The command stays `mongodbee studio`: the core CLI loads the studio package
when it is installed. Without it, the command prints how to install it and exits
with code 1. The studio is published to npm only, so run the command from the
npm package of the core (as below), not from JSR.

## Running it

The studio runs in the project's own runtime, like Drizzle Studio. That
matters because the studio loads the project's `mongodbee.config.ts`, its
migrations and its `schemas.ts`: a Deno project with an import map can only be
loaded by Deno, a Bun project by Bun. The server uses `node:http`, which Node,
Deno and Bun all provide, and serves the prebuilt UI.

```bash
npx mongodbee studio
bunx mongodbee studio --port 5000
deno run -A --env-file=.env --config=deno.jsonc npm:@diister/mongodbee/migration/cli/bin studio
```

For a Deno project, pass the project's `deno.jsonc` so its import map resolves,
and `--env-file` when the configuration reads the connection string from the
environment.

| Option         | Default           | Meaning                                                  |
| -------------- | ----------------- | -------------------------------------------------------- |
| `--port`       | `4983`            | Port to listen on (`0` picks a free one)                 |
| `--host`       | `127.0.0.1`       | Interface to bind                                        |
| `--config`     | discovered        | Configuration file, as for every command                 |
| `--project`    | current directory | Project to open: config discovery and relative paths     |
| `--uri`        | from the config   | Connection string, or set `MONGODBEE_STUDIO_URI`         |
| `--db`         | from the config   | Database name                                            |
| `--migrations` | from the config   | Migrations directory                                     |
| `--schemas`    | from the config   | Schemas file                                             |
| `--write`      | off               | Allow editing, creating and deleting documents           |

### Opening another project

Like Drizzle Studio, the studio can be pointed at any project and database
without being run from inside it:

```bash
npx mongodbee studio --project ../other-app
npx mongodbee studio --db staging_copy --migrations ./db/migrations --schemas ./db/schemas.ts
MONGODBEE_STUDIO_URI="mongodb://..." npx mongodbee studio --project ../other-app
```

`--project` is used for configuration discovery and to resolve the relative
paths, exactly as if the command ran from there. The explicit options override
the configuration. When any of them is given and no configuration file can be
loaded, the studio starts from the options alone and says so in its warnings.
Prefer `MONGODBEE_STUDIO_URI` over `--uri` for credentials, so the connection
string stays out of the shell history. A project that reads its connection
string from a `.env` file needs the runtime to load that file: run the command
from the project directory with the runtime's env-file option, or pass the URI
through the environment.

The studio may run from a different copy of mongodbee than the one the project
imports (a linked checkout, another version). Type definitions are recognised
through a shared `Symbol.for` brand, and `withIndex` metadata written by another
copy is read by its symbol description, so schemas built by either copy are
understood.

The server binds to the loopback interface by default and rejects requests
whose `Host` header is neither a loopback name nor the bound host, which keeps
a malicious web page from reaching it through DNS rebinding.

Schemas come from the project's `schemas.ts`. When that file cannot be loaded,
the studio falls back to the schemas of the latest migration and says so in the
header.

## What each view shows

### Overview

Every physical collection with its kind, document count and per-type counts:

- declared `collections`, `multiCollections` and `scopedMultiCollections`;
- the instances of every `multiModels` entry, discovered through their
  `_information` marker;
- collections present in the database but not declared;
- mongodbee's own collections (the migration history), listed apart as
  internal and never counted as undeclared.

Types found in the database but absent from the schemas are listed as
undeclared. For a scoped multi-collection, the distinct scope count and the
largest scopes are shown with their per-type counts.

### Summary

Multi-collections, multi-model instances and scoped collections open on a
summary instead of the raw table, because their documents are organised by
`_type` and, for scoped collections, by `_scope`:

- the number of documents, the types in use against the declared ones, the
  number of scopes, and the age of the newest document, read from the time in
  its id. Ids dated in the future are left out and marked on their type,
  since they were not generated when the documents were inserted;
- one bar per type with its share of the collection, the number of scopes it
  appears in and the age of its newest id. An undeclared type is marked, and
  declared types without documents and mongodbee's own markers are listed
  below;
- for a scoped collection, the largest scopes split by their five main types,
  and how many scopes fall into each size band (1-9, 10-99, 100-999, 1k-10k
  and 10k+ documents), plus a warning for documents without a scope.

Every type and scope row opens the Data tab filtered on it. The summary is
computed with bounded aggregations on each visit.

Every other collection (plain, undeclared) has a Summary tab too, next to
Data, which stays its default. It shows the number of documents, how many
declared fields are actually used, how many documents the latest month
brought and the age of the newest one, then the share of documents that
fill each field (fields found in the data but not in the schema are marked).

A collection whose types declare computed fields gets three more panels:

- **computed fields**: the share of sampled documents (up to 5,000 per type)
  holding a value for each field, the recomputations waiting for the drainer
  per field and the age of the oldest one, and a warning when no sampled
  document holds any value, which means the migration that applies them has
  not run on this database.
- **recomputations**: documents by `_computed._rev`, with the ones never
  recomputed set apart.
- **stored values against a full recompute**: recomputes the first 200, 1,000
  or 2,000 documents of each type from their sources, the way
  `checkComputed` does, and compares. It only reads. Each field says "in
  step" or how many documents drifted, drifts still waiting for the drainer
  are said as such, and up to 20 examples show the stored and the recomputed
  value, each opening its document.

For every collection, a "created per month" chart reads the creation time
carried by the ids (ULIDs, typed ids built on them, ObjectIds) of a random
sample of up to 5,000 documents and scales it to the collection. Ids without
a time are counted apart, and ids dated in the future are left out and
reported.

When something cannot be read, the page says what failed before why: the
kind of failure (no response from the studio server, not found, refused,
server error), the HTTP status and the route, then the server's message,
with a Retry button. An empty result says whether the collection is empty,
not created yet, or filtered out by the conditions or the scope, and offers
to clear them.

### Data

A dense table whose columns come from the type's schema (or from the documents
themselves for an undeclared collection).

The query reads as a sentence above the table, for example
`artwork in exposition:alpha01 where year at least 2000 and status is published,
sorted by year descending`, and every part of it is a control:

- Pick a type in the left rail to narrow a multi-collection to that `_type`.
- Pick a scope for a scoped multi-collection; the largest scopes are suggested
  and any value can be typed.
- Add conditions with `/` or the `where` button. Each condition is a field
  (searchable, grouped by type), an operator that fits the field (is, is not,
  is one of, above, at least, below, at most, contains, starts with, is set,
  is empty) and a value; "is one of" takes a comma separated list or a JSON
  array; picklists and booleans offer their values. Any other value field
  suggests the most frequent values recorded for that field, with their
  counts, narrowed by the type, the scope, the other conditions and what is
  typed (a prefix match). The list opens on focus and with `ctrl space`;
  Enter keeps the typed text unless a suggestion was picked with the arrows.
  Suggestions come from at most 20,000 matching documents, and the counts say
  so with a `+` when that bound was reached. Conditions accept dotted
  paths (`address.city`): the field list offers every nested path of the
  schema, through objects, arrays of objects and variants, four levels deep,
  under "Nested fields", and a nested field gets the operators, value choices
  and conversion of its own schema. Values are converted according to the field's schema,
  `null` matches null, a value starting with `{`, `[` or `"` is read as extended
  JSON, and text matching is escaped, so a condition can never inject an
  operator or a regular expression.
- Sort by any field from the sentence or by clicking a column header (ascending,
  descending, then back to `_id`).
- The number of matching documents is counted with a bound and shown under the
  sentence.
- Sorting by `_id` pages with a keyset cursor. Sorting by another field pages
  with a bounded offset, because MongoDB compares values of different types in
  brackets, which makes a cursor on an arbitrary field unreliable.
- A reference such as `user:01k...` links to its document when the studio can
  locate it: by the type of a multi-collection, or by the `refId` prefix
  declared on a collection's `_id` (the overview reports it as `idPrefix`).
  Such a reference, and a scope, then reads as the referenced document's
  name: its `name`, `displayName`, `title`, `label` or `fullName` (a
  localized object gives its first language), else first and last name,
  else `email`, `slug` or `code`, also looked up under `identity`,
  `profile`, `information` and `info`. Holding Alt shows the ids again, and
  the hover card names the field used. Names are fetched in batches of up to
  100 ids with a projection on those fields only, and forgotten on reload.
- The page can be shown three ways, like MongoDB Compass, and the choice is
  remembered in the browser:
  - **Table**: when a typed collection is shown without choosing a type, the
    columns are those of the types present on the page, minus the fields
    empty on every row of it, and the number left out is shown. A cell whose
    field does not belong to that row's type is hatched instead of reading as
    a missing value. When the page holds consecutive runs of different types
    (two to six runs, which sorting by `_id` gives naturally since ids carry
    their type), each run becomes its own table under a type heading, with
    only that type's columns; the page order is kept, and J/K still move
    across the whole page.
  - **Documents**: one compact record per document, headed by its id (and
    its type when the id does not already carry it) and its scope, with its
    own fields laid out as key and value pairs in as many columns as fit.
    Nested values show as a short summary that opens the drawer on them.
  - **JSON**: each document as extended JSON with syntax colouring, numbered,
    where a nested object or array short enough stays on one line; a button
    copies the page.
- 25 to 200 rows per page; clicking a row opens the full document in a JSON
  drawer. Long strings wrap inside the drawer with the key kept on their first
  line. The drawer is resized by dragging its left edge or, once the edge is
  focused, with the arrow keys, Home and End; a double click restores the
  default width, and the width is remembered in the browser.
- The drawer is modal, like the search palette: it opens in the browser's top
  layer over a light veil, the rest of the page cannot be clicked, focused or
  scrolled while it is open, and focus stays inside it. A click on the veil,
  Escape or the close button closes it and gives focus back. J and K still
  move to the previous and next document of the page behind it. The search
  palette opens above an open drawer.
- Computed fields (`_computed`) are never mixed with the application's own
  fields. In the table each one is its own column, after the user fields,
  headed by its name and a dashed "computed" tag, and it can be filtered and
  sorted like any field (`_computed.organizationIds`). In the documents view
  they sit under a dashed "computed" rule. In the drawer they get their own
  block below the document: the revision, each value (a list of references
  shows its type once), the declaration it comes from ("organizationId of
  org_membership by participantId where status = active"), and a link that
  opens the source documents already filtered: the `by` field equal to this
  document, the declaration's `where` conditions, and the scope when the
  sources are scoped.

### Schema

A schema lives twice in MongoDBee: as the Valibot schema in the project, and as
the `$jsonSchema` validator MongoDB enforces. The Schema tab shows both, in
three modes:

- **Fields**: each type's Valibot tree (kind, optional, nullable, default,
  picklist values, `refId` target, nested objects, arrays, unions and variants,
  pipe actions and field indexes), and on the same row what that field
  compiles to in the `$jsonSchema`: required or optional, the `bsonType`, and
  the enforced constraints (`minLength`, `minimum`, `enum`, `pattern`...).
  An "in use" column shows, for each top-level field, the share of documents
  where it is set and not null, measured on a random sample of up to 5,000
  documents of that type (`$sample`); fields found in the data but absent
  from the schema are listed under the table with their share, which is how
  leftovers of an old shape stand out.
  `defineType` composite indexes are listed under the type.
  Computed fields sit under `_computed`, tagged as computed, each with the
  declaration it comes from ("organizationId of org_membership by
  participantId, where status"); `_computed._rev` is tagged as the revision
  mongodbee bumps on every recompute.
  A constraint MongoDB ignores for the field's type (such as `minItems` on a
  string, which `nonEmpty` emitted for both strings and arrays before
  0.23.0-beta.32) is struck through, with the reason on hover; the regex of a
  `pattern` is shown on hover too.
- **$jsonSchema**: the full `$jsonSchema` each type compiles to, browsable and
  copyable.
- **Validator**: the collection validator the last applied migration expects,
  next to the one actually set in MongoDB, with their status (in sync,
  different, missing, not declared, or no migration applied yet). The same
  status is shown as a chip above the fields.

### Indexes

The indexes MongoDBee would create for the collection, compared with
`listIndexes`:

- **matching**: same name, key and options;
- **different**: same name, different key or options (the differing parts are
  listed);
- **missing**: declared but absent. The note says why: created by a pending
  migration (`mongodbee migrate`), declared by the applied migrations but not
  synchronized (`mongodbee sync`, or `mongodbee migrate` while migrations are
  pending, since `sync` refuses to run then), or declared in the schemas but in
  no migration yet (`mongodbee generate`);
- **extra**: present but not declared.

The declared set is not recomputed by the studio: it runs the same index
appliers that `sync` and the runtime use, against a recording stand-in for the
collection, so the naming and the options always match what would be applied.

Each built index also shows how often it was used (`$indexStats`, lookups since
the server started tracking it) and its size (`$collStats`), so an extra index
that is never used stands out. Both reads are bounded and best effort: without
the privileges they need, the columns stay empty and the page says so.

### Migrations

Five sections, each with its own link (`#/migrations/<section>`):

- **Timeline**: the migration files in chain order with their state from the
  history collection (applied, pending, failed, reverted), the applied date,
  the last duration, the `irreversible` and `lossy` flags, and the operations
  each migration compiles to, one dense row per migration. Rows are grouped
  by run (migrations applied in the same minute), then "Not applied yet",
  with sticky group headings, and the page opens on the first pending group.
  Migrations recorded in the database without a matching file are listed at
  the end.
- **Plan**: the pending migrations in the order `mongodbee migrate` would run
  them, with read-only, bounded impact estimates: documents touched, deletes
  that would leave dangling references, create conflicts, index builds (a
  pending unique index is checked for duplicate keys, with samples), seeds and
  scopes. Each migration says whether it can be rolled back, why it is
  irreversible or lossy, what blocks it, and the exact command.
- **Check**: the same validation as `mongodbee check`, run on demand. The
  settings read as a sentence: simulate from a chosen migration (every
  migration, or any one of them, pending ones marked), in quick, normal or
  hard mode, with a precise number of mock documents per collection (the
  mode's volume by default, 1 to 5,000) and the share of documents kept from
  one migration to the next (50% by default). The equivalent command,
  `mongodbee check --mode quick --last 23 --docs 20 --retention 0.3` for
  example, is shown ready to copy; `--docs` and `--retention` are CLI options
  too. The simulation is
  purely in memory and never connects to the database. It runs in a worker
  thread (`node:worker_threads`, available in Node, Deno and Bun), so the
  rest of the studio stays responsive during a long check, and stopping it
  ends the worker at once, even in the middle of a migration. Progress streams per
  migration. Identical warnings are shown once, and the warnings raised by
  every checked migration are grouped at the top under "Shared warnings", so
  each migration only lists what is specific to it. Failures stay expanded.
  "Stop check" ends the run: closing the stream terminates the worker, the
  finished results stay on screen and the rest are marked as not run.
- **Drift**: `schemas.ts` against the last migration (with a
  `mongodbee generate` hint), the validators in MongoDB against the ones the
  last applied migration expects, and index drift across every collection.
  Only the collections that drifted get a row, linked to their Schema or
  Indexes tab; the ones in sync are listed on one line.
- **History**: the applied runs and rollbacks, newest first, with durations.

## Packaging

The repository is a Bun workspace with two packages:

| Directory  | Package                     | Registries |
| ---------- | --------------------------- | ---------- |
| `library/` | `@diister/mongodbee`        | npm, JSR   |
| `studio/`  | `@diister/mongodbee-studio` | npm        |

The studio uses the core only through its public entry points:
`@diister/mongodbee`, `/schema`, `/ids` (in the browser, for id times) and
`/inspect`, which gathers what a tool needs to look at a project without
importing core internals: project and configuration loading, the migration
chain and history, the check runner (abort signal, document count, retention),
schema navigation (`schemaToNode`), the index plan of a collection
(`plannedIndexes`), validators and schema diffs. `@diister/mongodbee/inspect`
is public but made for tools; applications have no use for it.

`bun run version:set <version>` in `library/` bumps both packages, the
studio's dependency on the core and the core's optional peer on the studio.
`bun run build` refuses to run when any of them disagree, and when the studio
and the core pin different `mongodb` drivers.

The UI is prebuilt when the studio is built: `bun run build` in `studio/`
compiles the server, then `bun run build:ui` bundles the Svelte UI into
`dist/studio-ui` (the HTML page, content-hashed JavaScript, CSS, fonts and logo,
plus a `manifest.json`). At runtime the studio serves those files and never
needs `svelte` or `bun-plugin-svelte`, which, like `lucide` and the
`@fontsource-variable` packages, stay development dependencies of the studio.
The core package ships none of it.

The studio resolves the core through the workspace, from the core's `dist`:
build `library/` before type-checking, testing or building `studio/`. Running
the studio from its sources under Bun bundles the UI in memory with
`Bun.build` on start; under Node or Deno the sources serve the UI last built
into `studio/dist/studio-ui`, so run `bun run build:ui` first.

## API

The UI is a thin client over a JSON API that can be used directly. Values are
serialized as relaxed extended JSON (`{"$date": ...}`, `{"$oid": ...}`).

| Route                                       | Parameters                                                        |
| ------------------------------------------- | ----------------------------------------------------------------- |
| `GET /api/meta`                             |                                                                   |
| `GET /api/overview`                         | `scopes` (top scopes per scoped collection, default 10)           |
| `GET /api/collections/:name/documents`      | `type`, `scope`, `w` (repeatable `field:operator:value`), `f.<field>`, `sort`, `dir`, `after`, `before`, `offset`, `count`, `limit` (≤ 200) |
| `GET /api/collections/:name/document`       | `id` (extended JSON, e.g. `"user:01j..."` with the quotes)        |
| `GET /api/collections/:name/scopes`         | `limit`                                                           |
| `GET /api/collections/:name/summary`        | types, scope coverage, scope sizes, creation per month            |
| `GET /api/collections/:name/values`         | `field`, `q` (prefix), `limit` (≤ 100), plus `type`, `scope`, `w` |
| `GET /api/labels`                           | `id` (repeatable typed id, ≤ 100): a readable name for each       |
| `GET /api/collections/:name/coverage`       | `type`, `scope`, `w`: filled share of each field on a 5,000 sample |
| `GET /api/collections/:name/computed`       | `type`, `scope`, `stats` (`false` skips the sample): each computed field, its declaration and source collection, filled share, revisions, pending recomputations |
| `GET /api/collections/:name/computed-check` | `type`, `scope`, `limit` (1 to 2,000, default 200): drifts against a full recompute |
| `GET /api/collections/:name/schema`         |                                                                   |
| `GET /api/collections/:name/indexes`        |                                                                   |
| `GET /api/migrations`                       |                                                                   |
| `GET /api/migrations/plan`                  |                                                                   |
| `GET /api/migrations/drift`                 |                                                                   |
| `GET /api/migrations/history`               |                                                                   |
| `GET /api/migrations/check`                 | `mode` (quick, normal, hard), `last`; a server-sent event stream  |

Every document read has a limit and a `maxTimeMS`; counts use
`estimatedDocumentCount` for totals and indexed `_type` counts per type.

## Editing

```bash
npx mongodbee studio --write
```

With `--write`, the document drawer offers Edit and Delete, and the Data tab
a "New document" button once a type (and, for a scoped collection, a scope)
is chosen. The sidebar then reads "Write enabled".

Editing is a form built from the type's schema, with a JSON view one click
away for bulk changes:

- each field is one compact row, name on the left and input on the right,
  with the input of its kind: text, a number field that accepts a comma, an
  on/off switch for booleans, the same colored chips as the tables for short
  picklists (a searchable list past six choices), a date field with "Now",
  nested groups for objects, items to add and remove for arrays, and a choice
  of option for variants that keeps the fields both options share. Unknown or
  free-form fields fall back to a small JSON input;
- text fields suggest the values already stored in that field (for the same
  type and scope), filtered as you type; Ctrl+Space opens the list without
  typing. Lists never open on focus alone, so tabbing through the form stays
  quiet;
- a reference (`refId`) shows its target type tag and is picked from the
  documents it points to, listed by their name; any id can still be typed;
- `_computed` is never offered: mongodbee maintains it, and the write API
  refuses it like `_id`, `_type` and `_scope`;
- nullable fields have a `null` switch; optional fields can be removed, and
  the ones not set yet are offered as "+ field" buttons;
- every field shows what its schema expects ("at least 1 character, an
  email address"), and the whole document is checked against the schema as
  you type: problems appear under their field, the header counts them, and
  saving waits until the document matches;
- a new document starts with the type's required fields filled with
  sensible values (first choice of a picklist, the current date).

Deleting takes one confirmation in the drawer header, then shows "Deleted
… Undo" for a few seconds. Undo puts the exact document back; the server
keeps a deleted document for ten minutes for that purpose.

Every write goes through MongoDBee collections, never through the raw
driver:

- writes are issued by `collection()`, `multiCollection()` and
  `scopedMultiCollection()` instances built from the project schemas, with
  `schemaManagement: "managed"` so the studio never touches validators or
  indexes. Each changed field is first checked against its own schema, so a
  refusal names the field and the nested path (`address.city: Invalid type`);
  the collection then validates again, and MongoDB's validator last;
- updates are guarded: the editor sends, for every changed or removed
  top-level field, the value it loaded, and the update only matches if those
  values are still there. A document changed in the meantime is refused with
  `409` and the drawer offers to reload it;
- scoped writes go through a `.scope(value)` view, so a document can never be
  moved to another tenant;
- `_id`, `_type` and `_scope` cannot be edited;
- a delete must carry the document id as `confirm`, which the UI adds after
  the confirmation click;
- the server logs every write (`[studio] update users u1 fields name`).

Writing is refused, with the reason shown in the UI:

- unless the studio runs with `--write`, which also refuses a non-loopback
  `--host`;
- while migrations are pending, since `schemas.ts` would describe a shape the
  database is not in yet (`mongodbee migrate` first);
- when the schemas come from the latest migration instead of `schemas.ts`;
- for multi-model instances and undeclared collections, which stay read-only;
- for requests without the `x-mongodbee-studio: write` header, with a body
  that is not JSON, or from another origin, so a web page cannot forge a
  write.

| Route                                       | Body                                                    |
| ------------------------------------------- | ------------------------------------------------------- |
| `PATCH /api/collections/:name/document`     | `id`, `type`, `scope`, `set`, `unset`, `expected`       |
| `POST /api/collections/:name/documents`     | `type`, `scope`, `document`                             |
| `DELETE /api/collections/:name/document`    | `id`, `type`, `scope`, `confirm` (the id); returns a `restoreToken` |
| `POST /api/collections/:name/restore`       | `token`: puts a deleted document back, within ten minutes |
| `POST /api/collections/:name/validate`      | `type`, `document`: the schema's `issues`, nothing written |

Values are extended JSON, so `{"$date": "..."}` stores a date. A schema
refusal answers `422` with `issues` (`path`, `message`), a unique index
conflict `409`.
