# @diister/mongodbee-studio

A local web explorer for the database of a
[MongoDBee](https://github.com/diister-dev/mongodbee) project: collections by
type and scope, schemas next to the validators MongoDB enforces, declared and
actual indexes, the migration chain, and a configurable migration check. It is
read-only unless started with `--write`.

Install it next to `@diister/mongodbee`, with the same version, and open it
with the core CLI:

```bash
npm install --save-dev @diister/mongodbee-studio
npx mongodbee studio
```

```bash
bun add --dev @diister/mongodbee-studio
bunx mongodbee studio
```

```bash
deno add --dev npm:@diister/mongodbee-studio
deno run -A --env-file=.env --config=deno.jsonc npm:@diister/mongodbee/migration/cli/bin studio
```

The full guide, with every view, option and the write mode, is
[doc/STUDIO.md](https://github.com/diister-dev/mongodbee/blob/main/doc/STUDIO.md).

## License

MIT
