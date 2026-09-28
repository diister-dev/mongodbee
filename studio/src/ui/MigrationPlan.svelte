<script>
  import { onMount } from "svelte";
  import { api } from "./lib/api.js";
  import { plural } from "./lib/format.js";
  import ErrorState from "./ErrorState.svelte";
  import Status from "./Status.svelte";
  import CommandChip from "./CommandChip.svelte";
  import Spectrum from "./controls/Spectrum.svelte";
  import CountValue from "./CountValue.svelte";
  import OperationStack from "./OperationStack.svelte";
  import PlanOperation from "./PlanOperation.svelte";
  import IndexKey from "./values/IndexKey.svelte";
  import MigrationId from "./values/MigrationId.svelte";
  import Value from "./values/Value.svelte";

  let { navigate } = $props();

  let plan = $state(null);
  let error = $state(null);

  const ROLLBACK = {
    possible: { tone: "success", label: "rollback possible" },
    lossy: { tone: "warning", label: "rollback is lossy" },
    impossible: { tone: "danger", label: "no rollback" },
  };

  async function load() {
    error = null;
    try {
      plan = await api("/api/migrations/plan");
    } catch (e) {
      error = e;
    }
  }

  onMount(load);
</script>

<div class="page">
  {#if error}
    <ErrorState {error} title="The plan could not be computed" onretry={load} />
  {:else if !plan}
    <div class="empty-state">
      <Spectrum size={8} loading />
      <span class="shimmer">Estimating the plan against the live database</span>
    </div>
  {:else if plan.pending === 0}
    <div class="bezel">
      <div class="bezel-card">
        <div class="empty-state">
          <span>Nothing pending</span>
          <span class="faint">The database is at the last migration.</span>
        </div>
      </div>
    </div>
  {:else}
    <section class="bezel intro stagger" style="--i: 0">
      <div class="bezel-card intro-card">
        <div class="intro-text">
          <h2 class="section-title">{plural(plan.pending, "pending migration")} would run, in this order</h2>
          <p class="muted">
            Impact is estimated read-only from the live database with bounded counts. Nothing is written until you run the
            commands below.
          </p>
        </div>
        {#if plan.blocking.length > 0}
          <ul class="notes">
            {#each plan.blocking as note}
              <li><Status tone="warning" /> <span>{note}</span></li>
            {/each}
          </ul>
        {/if}
      </div>
      <footer class="bezel-foot commands">
        <span class="muted small">Run</span>
        {#each plan.commands as command, index}
          {#if index > 0}<span class="faint small">then</span>{/if}
          <CommandChip {command} />
        {/each}
        <button class="control quiet small-button" onclick={() => navigate({ view: "migrations", section: "check" })}>
          Dry run the check here
        </button>
      </footer>
    </section>

    {#each plan.migrations as migration, index (migration.id)}
      <section class="bezel migration stagger" style="--i: {index + 1}">
        <div class="bezel-card">
          <header class="card-head">
            <span class="step num">{index + 1}</span>
            <h3 class="name">{migration.name}</h3>
            <Status tone={ROLLBACK[migration.rollback].tone} label={ROLLBACK[migration.rollback].label} />
            {#if migration.blocking.length > 0}
              <Status tone="danger" label={plural(migration.blocking.length, "blocking issue")} />
            {/if}
          </header>

          {#if migration.compileError}
            <div class="pad"><div class="notice danger">{migration.compileError}</div></div>
          {/if}

          {#if migration.blocking.length > 0}
            <ul class="blocking">
              {#each migration.blocking as reason}
                <li>{reason}</li>
              {/each}
            </ul>
          {/if}

          <div class="ops">
            <OperationStack operations={migration.operations}>
              {#snippet row(operation, nested)}
                <PlanOperation {operation} {nested} />
              {/snippet}
            </OperationStack>
          </div>

          {#if migration.indexes.length > 0}
            <div class="indexes">
              <div class="sub-title muted small">Indexes to build</div>
              {#each migration.indexes as build (build.collection + build.name)}
                <div class="index-row">
                  <span class="mono collection">{build.collection}</span>
                  <IndexKey fields={build.key} unique={build.unique} />
                  <span class="muted small">{build.change === "new" ? "new" : "rebuilt"}</span>
                  <span class="small muted">over <CountValue count={build.documents} noun="document" /></span>
                  {#if build.duplicates}
                    {#if build.duplicates.groups > 0}
                      <Status tone="danger" label="{plural(build.duplicates.groups, 'duplicate key')} would fail it" />
                    {:else if build.duplicates.timedOut}
                      <Status tone="warning" label="duplicate scan timed out" />
                    {:else}
                      <Status tone="success" label="no duplicates" />
                    {/if}
                  {/if}
                </div>
                {#if build.duplicates?.groups > 0}
                  <div class="duplicates">
                    {#each build.duplicates.sample as dup}
                      <div class="dup">
                        <span class="mono key">{Object.entries(dup.key).map(([k, v]) => `${k} = ${JSON.stringify(v)}`).join(", ")}</span>
                        <span class="num small muted">{plural(dup.count, "document")}</span>
                        <span class="ids">
                          {#each dup.ids.slice(0, 3) as id}<Value value={id} field="_id" />{/each}
                        </span>
                      </div>
                    {/each}
                    {#if build.duplicates.groups > build.duplicates.sample.length}
                      <div class="faint small">
                        and {plural(build.duplicates.groups - build.duplicates.sample.length, "more group")},
                        {plural(build.duplicates.documents, "document")} involved in total
                      </div>
                    {/if}
                  </div>
                {/if}
              {/each}
            </div>
          {/if}
        </div>
        <footer class="bezel-foot foot">
          <MigrationId value={migration.fileName ?? migration.id} />
          <CommandChip command={migration.command} />
        </footer>
      </section>
    {/each}
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

  .intro-card {
    gap: 10px;
    padding: 16px;
  }

  .intro-text p {
    margin: 4px 0 0;
  }

  .notes,
  .blocking {
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .notes li {
    display: flex;
    align-items: baseline;
    gap: 8px;
    color: var(--text-muted);
  }

  .commands {
    justify-content: flex-start;
    flex-wrap: wrap;
    gap: 8px;
    padding-right: 10px;
  }

  .small {
    font-size: var(--text-xs);
  }

  .small-button {
    margin-left: auto;
    height: 28px;
    font-size: var(--text-xs);
  }

  .card-head {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 14px 16px 10px;
  }

  .card-head :global(.status) {
    font-size: var(--text-xs);
  }

  .step {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    border: 0;
    border-radius: 0;
    background: var(--text);
    color: var(--card);
    font-family: var(--font-mono);
    font-size: 11px;
  }

  .name {
    margin: 0;
    font-size: var(--text-md);
    font-weight: 500;
  }

  .pad {
    padding: 0 16px 10px;
  }

  .blocking {
    padding: 0 16px 10px 50px;
    color: var(--danger);
    font-size: var(--text-xs);
  }

  .ops {
    padding: 0 16px 6px 12px;
    border-top: 1px solid var(--hairline);
  }

  .indexes {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 10px 16px 12px 38px;
    border-top: 1px solid var(--hairline);
  }

  .index-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px 14px;
    min-height: 30px;
  }

  .index-row :global(.status) {
    font-size: var(--text-xs);
  }

  .duplicates {
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 8px 12px;
    border: 1px dashed color-mix(in srgb, var(--danger) 55%, var(--hairline));
    border-radius: 0;
    background: color-mix(in srgb, var(--danger) 4%, var(--card));
  }

  .dup {
    display: grid;
    grid-template-columns: minmax(120px, 200px) 90px minmax(0, 1fr);
    align-items: center;
    gap: 12px;
    min-height: 26px;
  }

  .ids {
    display: flex;
    gap: 12px;
    min-width: 0;
    overflow: hidden;
  }

  .foot {
    padding-right: 6px;
  }
</style>
