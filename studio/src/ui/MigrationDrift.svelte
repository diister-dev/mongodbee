<script>
  import { onMount } from "svelte";
  import { api } from "./lib/api.js";
  import { plural } from "./lib/format.js";
  import ErrorState from "./ErrorState.svelte";
  import Status from "./Status.svelte";
  import CommandChip from "./CommandChip.svelte";
  import Spectrum from "./controls/Spectrum.svelte";
  import HoverCard from "./values/HoverCard.svelte";
  import MigrationId from "./values/MigrationId.svelte";
  import TypeTag from "./values/TypeTag.svelte";

  let { navigate } = $props();

  let drift = $state(null);
  let error = $state(null);

  const CHANGE = { added: "success", removed: "danger", changed: "warning" };
  const VALIDATOR = { matching: "success", different: "warning", missing: "danger", unexpected: "neutral" };
  const INDEX = { missing: "danger", different: "warning", extra: "neutral", matching: "success" };

  const VALIDATOR_ORDER = ["missing", "different", "unexpected"];
  const validatorProblems = $derived(
    drift
      ? drift.validators.rows
          .filter((row) => row.status !== "matching")
          .toSorted(
            (a, b) =>
              VALIDATOR_ORDER.indexOf(a.status) - VALIDATOR_ORDER.indexOf(b.status) ||
              a.collection.localeCompare(b.collection),
          )
      : [],
  );
  const validatorClean = $derived(drift ? drift.validators.rows.filter((row) => row.status === "matching") : []);
  const indexProblems = $derived(drift ? drift.indexes.filter((row) => row.problems.length > 0) : []);
  const indexClean = $derived(drift ? drift.indexes.filter((row) => row.problems.length === 0) : []);

  async function load() {
    error = null;
    try {
      drift = await api("/api/migrations/drift");
    } catch (e) {
      error = e;
    }
  }

  onMount(load);
</script>

<div class="page">
  {#if error}
    <ErrorState {error} title="Drift could not be computed" onretry={load} />
  {:else if !drift}
    <div class="empty-state">
      <Spectrum size={8} loading />
      <span class="shimmer">Comparing schemas, validators and indexes</span>
    </div>
  {:else}
    <section class="bezel stagger" style="--i: 0">
      <div class="bezel-card">
        <header class="card-head">
          <h3 class="section-title">Schemas</h3>
          <span class="muted small">
            schemas.ts against the last migration
            {#if drift.schema.baseline}<MigrationId value={drift.schema.baseline.id} />{/if}
          </span>
          {#if !drift.schema.available}
            <Status tone="neutral" label="not available" />
          {:else if drift.schema.rows.length === 0}
            <Status tone="success" label="in sync" />
          {:else}
            <Status tone="warning" label="a migration needs to be generated" />
          {/if}
        </header>
        {#if drift.schema.rows.length > 0}
          <div class="rows">
            {#each drift.schema.rows as row}
              <div class="drift-row">
                <Status tone={CHANGE[row.change]} label={row.change} />
                <span class="where">
                  <span class="mono">{row.collection}</span>
                  {#if row.type}<TypeTag name={row.type} />{/if}
                </span>
                <span class="fields">
                  {#each row.fields as field}
                    <HoverCard label="{field.field} {field.change}">
                      <span class="field">
                        <span class="sign {field.change}">{field.change === "added" ? "+" : field.change === "removed" ? "−" : "~"}</span>
                        <span class="mono">{field.field}</span>
                      </span>
                      {#snippet card()}
                        <div class="vc-body">
                          <div class="vc-title">
                            <span class="mono">{field.field}</span>
                            <span class="muted">{field.change} in schemas.ts</span>
                          </div>
                          <div class="vc-sep"></div>
                          {#each field.details as detail}
                            <div class="detail mono">{detail}</div>
                          {/each}
                        </div>
                      {/snippet}
                    </HoverCard>
                  {/each}
                </span>
              </div>
            {/each}
          </div>
        {:else if !drift.schema.available}
          <p class="pad muted small">The studio could not load schemas.ts, so there is nothing to compare with.</p>
        {/if}
      </div>
      {#if drift.schema.command}
        <footer class="bezel-foot">
          <span class="muted small">Capture these changes in a new migration</span>
          <CommandChip command={drift.schema.command} />
        </footer>
      {/if}
    </section>

    <section class="bezel stagger" style="--i: 1">
      <div class="bezel-card">
        <header class="card-head">
          <h3 class="section-title">Validators</h3>
          <span class="muted small">
            applied in MongoDB against the last applied migration
            {#if drift.validators.baseline}<MigrationId value={drift.validators.baseline.id} />{/if}
          </span>
          {#if drift.validators.rows.length > 0}
            {#if validatorProblems.length === 0}
              <Status tone="success" label="in sync" />
            {:else}
              <Status tone="warning" label="{plural(validatorProblems.length, 'collection')} drifted" />
            {/if}
          {/if}
        </header>
        {#if drift.validators.rows.length === 0}
          <p class="pad muted small">No migration has been applied yet.</p>
        {:else}
          <div class="rows">
            {#each validatorProblems as row (row.collection)}
              <div class="drift-row">
                <Status tone={VALIDATOR[row.status]} label={row.status} />
                <span class="where">
                  <button
                    class="collection-link mono"
                    onclick={() => navigate({ view: "collection", collection: row.collection, tab: "schema" })}
                  >
                    {row.collection}
                  </button>
                </span>
                <span class="muted small">
                  {#if row.status === "different"}The validator in the database differs; mongodbee sync rewrites it.
                  {:else if row.status === "missing"}No validator is set on this collection.
                  {:else if row.status === "unexpected"}A validator exists but the migration does not declare this collection.
                  {/if}
                </span>
              </div>
            {/each}
            {#if validatorClean.length > 0}
              <div class="clean muted small">
                In sync: {#each validatorClean as row, i (row.collection)}<span class="mono">{row.collection}</span>{i < validatorClean.length - 1 ? ", " : ""}{/each}
              </div>
            {/if}
          </div>
        {/if}
      </div>
    </section>

    <section class="bezel stagger" style="--i: 2">
      <div class="bezel-card">
        <header class="card-head">
          <h3 class="section-title">Indexes</h3>
          <span class="muted small">declared against listIndexes, every collection</span>
          {#if indexProblems.length === 0}
            <Status tone="success" label="in sync" />
          {:else}
            <Status tone="warning" label="{plural(indexProblems.length, 'collection')} drifted" />
          {/if}
        </header>
        <div class="rows">
          {#each indexProblems as row (row.collection)}
            <div class="index-block">
              <button class="collection-link mono" onclick={() => navigate({ view: "collection", collection: row.collection, tab: "indexes" })}>
                {row.collection}
              </button>
              <span class="problems">
                {#each row.problems as problem}
                  <span class="problem">
                    <Status tone={INDEX[problem.status]} label={problem.status} />
                    <span class="mono small">{problem.name}</span>
                    {#if problem.hint}<span class="faint small">{problem.hint}</span>{/if}
                  </span>
                {/each}
              </span>
            </div>
          {/each}
          {#if indexClean.length > 0}
            <div class="clean muted small">
              In sync: {#each indexClean as row, i (row.collection)}<span class="mono">{row.collection}</span>{i < indexClean.length - 1 ? ", " : ""}{/each}
            </div>
          {/if}
        </div>
      </div>
    </section>
  {/if}
</div>

<style>
  .page {
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 16px;
    min-height: 0;
    margin-right: -16px;
    padding: 4px 4px 24px 8px;
    scrollbar-gutter: stable;
    overflow: auto;
  }

  .page > :global(.bezel) {
    flex: none;
  }

  .card-head {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px 14px;
    padding: 14px 16px 10px;
  }

  .card-head :global(.status),
  .drift-row :global(.status),
  .problem :global(.status) {
    font-size: var(--text-xs);
  }

  .small {
    font-size: var(--text-xs);
  }

  .pad {
    margin: 0;
    padding: 0 16px 14px;
  }

  .rows {
    border-top: 1px solid var(--hairline);
  }

  .drift-row {
    display: grid;
    grid-template-columns: 110px minmax(160px, 260px) minmax(0, 1fr);
    align-items: center;
    gap: 14px;
    min-height: 38px;
    padding: 0 16px;
    border-bottom: 1px solid var(--hairline);
  }

  .drift-row:last-child {
    border-bottom: 0;
  }

  .where {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .fields {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 14px;
  }

  .field {
    display: inline-flex;
    align-items: center;
    gap: 4px;
  }

  .vc-title {
    display: flex;
    align-items: baseline;
    gap: 8px;
  }

  .detail {
    padding: 2px 0;
    color: var(--text-muted);
  }

  .sign {
    width: 10px;
    font-family: var(--font-mono);
    text-align: center;
  }

  .sign.added {
    color: var(--success);
  }

  .sign.removed {
    color: var(--danger);
  }

  .sign.changed {
    color: var(--warning);
  }

  .index-block {
    display: grid;
    grid-template-columns: 160px minmax(0, 1fr);
    gap: 14px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--hairline);
  }

  .collection-link {
    align-self: start;
    padding: 0;
    border: 0;
    background: none;
    color: var(--text);
    font-size: var(--text-sm);
    text-align: left;
    text-decoration: underline;
    text-decoration-color: var(--card-border);
    text-underline-offset: 3px;
  }

  .problems {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .problem {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .clean {
    padding: 10px 16px;
  }
</style>
