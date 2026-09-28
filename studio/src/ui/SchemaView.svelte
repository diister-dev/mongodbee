<script>
  import SchemaField from "./SchemaField.svelte";
  import JsonTree from "./JsonTree.svelte";
  import Status from "./Status.svelte";
  import Segmented from "./controls/Segmented.svelte";
  import { api, collectionPath } from "./lib/api.js";
  import { isTypedKind, plural } from "./lib/format.js";
  import { jsonSchemaChild } from "./lib/json-schema.ts";
  import { formatCount } from "./lib/values.ts";
  import Tooltip from "./controls/Tooltip.svelte";
  import CopyButton from "./values/CopyButton.svelte";
  import IndexKey from "./values/IndexKey.svelte";
  import MigrationId from "./values/MigrationId.svelte";
  import TypeTag from "./values/TypeTag.svelte";

  let { schema, type, collection } = $props();

  let mode = $state("fields");
  let usage = $state({});

  const types = $derived(type ? schema.types.filter((t) => t.name === type) : schema.types);

  const IMPLICIT = new Set(["_id", "_type", "_scope"]);

  function totalFor(name) {
    if (!collection) return 0;
    if (!isTypedKind(collection.kind)) return collection.total ?? 0;
    return collection.types.find((t) => t.name === name)?.count ?? 0;
  }

  $effect(() => {
    if (!collection?.exists) return;
    const names = types.map((t) => t.name).filter((name) => totalFor(name) > 0);
    const typed = isTypedKind(collection.kind);
    let cancelled = false;
    const queue = [...names];
    const worker = async () => {
      while (!cancelled && queue.length > 0) {
        const name = queue.shift();
        try {
          const result = await api(collectionPath(collection.name, "coverage"), typed ? { type: name } : {});
          if (!cancelled) usage = { ...usage, [name]: { ...result, total: totalFor(name) } };
        } catch {
          continue;
        }
      }
    };
    worker();
    worker();
    return () => {
      cancelled = true;
    };
  });

  function extraFields(t) {
    const found = usage[t.name];
    if (!found || found.sampled === 0) return [];
    return Object.entries(found.fields)
      .filter(([key]) => !IMPLICIT.has(key) && !(key in t.fields))
      .sort((a, b) => b[1] - a[1])
      .map(([key, count]) => ({ key, count, ratio: count / found.sampled }));
  }

  function share(ratio) {
    const value = ratio * 100;
    return value >= 99.5 ? "100%" : value < 1 ? "<1%" : `${Math.round(value)}%`;
  }

  const VALIDATOR = {
    matching: { tone: "success", label: "validator in sync" },
    different: { tone: "warning", label: "validator differs" },
    missing: { tone: "danger", label: "no validator set" },
    unexpected: { tone: "neutral", label: "validator not declared" },
    "not-applied": { tone: "neutral", label: "no migration applied yet" },
  };

  const MODES = [
    { id: "fields", label: "Fields" },
    { id: "json", label: "$jsonSchema" },
    { id: "validator", label: "Validator" },
  ];

  function requiredCount(t) {
    return t.jsonSchema?.required?.length ?? 0;
  }

  function stringify(value) {
    return JSON.stringify(value, null, 2);
  }

  function unwrap(validator) {
    return validator && typeof validator === "object" && "$jsonSchema" in validator
      ? validator.$jsonSchema
      : validator;
  }
</script>

<div class="page">
  {#if schema.kind === "undeclared" || schema.kind === "internal"}
    <div class="bezel">
      <div class="bezel-card">
        <div class="empty-state">
          <span>No schema</span>
          <span class="faint">This collection is not declared in the project schemas</span>
        </div>
      </div>
    </div>
  {:else}
    <div class="bar">
      <div class="facts">
        <span class="label-mono">managed by mongodbee</span>
        {#each schema.implicitFields as field}
          <span class="tag">{field}</span>
        {/each}
        {#if schema.scope}
          <span class="label-mono">scope</span>
          {#if schema.scope.ref}
            <TypeTag name={schema.scope.ref} title="Scope references {schema.scope.ref}" />
          {:else}
            <span class="tag">{schema.scope.kind}</span>
          {/if}
        {/if}
        {#if schema.validator}
          {@const state = VALIDATOR[schema.validator.status]}
          <button class="validator-chip press" onclick={() => (mode = "validator")}>
            <Status tone={state.tone} hollow={schema.validator.status === "not-applied"} label={state.label} />
          </button>
        {/if}
      </div>
      <Segmented items={MODES} value={mode} label="Schema view" onselect={(id) => (mode = id)} />
    </div>

    {#if mode === "validator"}
      {@const validator = schema.validator}
      <section class="bezel">
        <div class="bezel-card">
          <header class="card-head">
            <span class="eyebrow">collection validator</span>
            {#if validator}
              <Status tone={VALIDATOR[validator.status].tone} label={VALIDATOR[validator.status].label} />
              {#if validator.baseline}
                <span class="muted small">expected by the last applied migration</span>
                <MigrationId value={validator.baseline.id} />
              {/if}
            {/if}
          </header>
          <div class="compare">
            <div class="pane">
              <div class="pane-head">
                <span class="label-mono">expected $jsonSchema</span>
                {#if validator?.expected}<CopyButton text={stringify(validator.expected)} label="Copy the expected validator" compact />{/if}
              </div>
              {#if validator?.expected}
                <div class="tree"><JsonTree value={unwrap(validator.expected)} /></div>
              {:else}
                <p class="faint small pad">Nothing is expected for this collection.</p>
              {/if}
            </div>
            <div class="pane">
              <div class="pane-head">
                <span class="label-mono">$jsonSchema in mongodb</span>
                {#if validator?.actual}<CopyButton text={stringify(validator.actual)} label="Copy the validator in MongoDB" compact />{/if}
              </div>
              {#if validator?.actual}
                <div class="tree"><JsonTree value={unwrap(validator.actual)} /></div>
              {:else}
                <p class="faint small pad">The collection has no validator.</p>
              {/if}
            </div>
          </div>
        </div>
        {#if validator?.status === "different"}
          <footer class="bezel-foot">
            <span class="muted small">mongodbee sync rewrites the validator from the last applied migration</span>
          </footer>
        {/if}
      </section>
    {:else}
      {#each types as t, i (t.name)}
        <section class="bezel stagger" style="--i: {i}">
          <div class="bezel-card">
            <header class="card-head">
              {#if schema.kind === "collection"}
                <h2 class="section-title mono">{t.name}</h2>
              {:else}
                <TypeTag name={t.name} />
              {/if}
              <span class="muted small">{plural(Object.keys(t.fields).length, "field")}</span>
              {#if t.jsonSchema}<span class="muted small">{requiredCount(t)} required</span>{/if}
              <span class="spacer"></span>
              {#if mode === "json" && t.jsonSchema}
                <CopyButton text={stringify(t.jsonSchema)} label="Copy the $jsonSchema" />
              {/if}
            </header>
            {#if mode === "json"}
              <div class="json-pane">
                {#if t.jsonSchema}
                  <JsonTree value={t.jsonSchema} />
                {:else}
                  <p class="faint small pad">This type could not be converted to a $jsonSchema.</p>
                {/if}
              </div>
            {:else}
              <table class="table fixed">
                <colgroup>
                  <col style="width: 20%" />
                  <col style="width: 9%" />
                  <col style="width: 18%" />
                  <col style="width: 25%" />
                  <col />
                </colgroup>
                <thead>
                  <tr>
                    <th>field</th>
                    <th>
                      <Tooltip text="Share of documents where the field is set and not null, over a random sample of up to 5,000">
                        <span>in use</span>
                      </Tooltip>
                    </th>
                    <th>valibot type</th>
                    <th>rules</th>
                    <th>$jsonSchema</th>
                  </tr>
                </thead>
                <tbody>
                  {#each Object.entries(t.fields) as [name, node] (name)}
                    <SchemaField
                      {name}
                      {node}
                      json={jsonSchemaChild(t.jsonSchema, name)}
                      parentJson={t.jsonSchema}
                      usage={usage[t.name]}
                    />
                  {/each}
                </tbody>
              </table>
              {@const extra = extraFields(t)}
              {#if extra.length > 0}
                <div class="extra">
                  <span class="label-mono">in the data, not in the schema</span>
                  {#each extra as field (field.key)}
                    <Tooltip text="{formatCount(field.count)} of {formatCount(usage[t.name].sampled)} sampled documents">
                      <span class="extra-field"><span class="mono">{field.key}</span> <span class="num faint">{share(field.ratio)}</span></span>
                    </Tooltip>
                  {/each}
                </div>
              {/if}
            {/if}
          </div>
          {#if t.indexes.length > 0 && mode === "fields"}
            <footer class="bezel-foot indexes">
              <span class="label-mono">declared indexes</span>
              <div class="index-list">
                {#each t.indexes as index}
                  <span class="index">
                    <IndexKey
                      fields={index.key}
                      unique={index.unique}
                      partial={index.partialFilterExpression}
                      ttl={index.expireAfterSeconds}
                    />
                    {#if index.global}<span class="muted small">across scopes</span>{/if}
                  </span>
                {/each}
              </div>
            </footer>
          {/if}
        </section>
      {/each}
    {/if}
  {/if}
</div>

<style>
  .page {
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 14px;
    min-height: 0;
    padding: 0 0 8px;
    overflow: auto;
  }

  .page > :global(.bezel) {
    flex: none;
  }

  .bar {
    display: flex;
    flex: none;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 10px 16px;
    padding-left: 4px;
  }

  .facts {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px 8px;
  }

  .validator-chip {
    display: inline-flex;
    align-items: center;
    height: 24px;
    margin-left: 8px;
    padding: 0 8px;
    border: 1px solid var(--card-border);
    background: var(--card);
  }

  .validator-chip :global(.status) {
    font-size: var(--text-xs);
  }

  @media (hover: hover) and (pointer: fine) {
    .validator-chip:hover {
      border-color: var(--text);
    }
  }

  .card-head {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px 12px;
    padding: 14px 16px 12px;
  }

  .card-head :global(.status) {
    font-size: var(--text-xs);
  }

  .spacer {
    flex: 1;
  }

  .extra {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px 14px;
    padding: 10px 16px 12px;
    border-top: 1px dashed var(--card-border);
    background: var(--frame);
  }

  .extra .label-mono {
    color: var(--warning);
  }

  .extra-field {
    font-size: var(--text-xs);
  }

  .small {
    font-size: var(--text-xs);
  }

  .pad {
    margin: 0;
    padding: 12px 16px;
  }

  .fixed {
    table-layout: fixed;
  }

  .fixed :global(th:first-child),
  .fixed :global(td:first-child) {
    padding-left: 16px;
  }

  .json-pane {
    padding: 8px 16px 16px 32px;
    border-top: 1px solid var(--hairline);
  }

  .compare {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    border-top: 1px solid var(--hairline);
  }

  .pane {
    min-width: 0;
  }

  .pane + .pane {
    border-left: 1px solid var(--hairline);
  }

  .pane-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 36px;
    padding: 0 12px 0 16px;
    border-bottom: 1px solid var(--hairline);
    background: var(--frame);
  }

  .tree {
    max-height: 60vh;
    padding: 8px 16px 16px 32px;
    overflow: auto;
  }

  .indexes {
    justify-content: flex-start;
    gap: 16px;
    padding-right: 10px;
  }

  .index-list {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 20px;
  }

  .index {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
</style>
