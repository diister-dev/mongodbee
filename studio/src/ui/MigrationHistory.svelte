<script>
  import { onMount } from "svelte";
  import { api } from "./lib/api.js";
  import { plural } from "./lib/format.js";
  import ErrorState from "./ErrorState.svelte";
  import Status from "./Status.svelte";
  import Spectrum from "./controls/Spectrum.svelte";
  import DateValue from "./values/DateValue.svelte";
  import MigrationId from "./values/MigrationId.svelte";
  import Duration from "./values/Duration.svelte";

  let history = $state(null);
  let error = $state(null);

  const TONE = { applied: "success", reverted: "neutral", failed: "danger" };

  async function load() {
    error = null;
    try {
      history = await api("/api/migrations/history");
    } catch (e) {
      error = e;
    }
  }

  onMount(load);
</script>

<div class="page">
  {#if error}
    <ErrorState {error} title="The migration history could not be read" onretry={load} />
  {:else if !history}
    <div class="empty-state">
      <Spectrum size={8} loading />
      <span class="shimmer">Reading the migration history</span>
    </div>
  {:else}
    <section class="bezel">
      <div class="bezel-card">
        {#if history.entries.length === 0}
          <div class="empty-state">
            <span>No runs recorded</span>
            <span class="faint">mongodbee migrate records every apply and rollback here.</span>
          </div>
        {:else}
          <table class="table">
            <colgroup>
              <col style="width: 170px" />
              <col style="width: 130px" />
              <col />
              <col style="width: 110px" />
              <col style="width: 150px" />
            </colgroup>
            <thead>
              <tr>
                <th>When</th>
                <th>Run</th>
                <th>Migration</th>
                <th class="num">Duration</th>
                <th>mongodbee</th>
              </tr>
            </thead>
            <tbody>
              {#each history.entries as entry, index (index)}
                <tr>
                  <td><DateValue value={entry.executedAt} /></td>
                  <td>
                    <Status
                      tone={entry.status === "failure" ? "danger" : TONE[entry.operation]}
                      label={entry.status === "failure" ? `${entry.operation}, failed` : entry.operation}
                    />
                  </td>
                  <td>
                    <div class="migration">
                      <span class="name">{entry.migrationName}</span>
                      <MigrationId value={entry.migrationId} />
                      {#if entry.adopted}<span class="tag">adopted</span>{/if}
                      {#if !entry.knownFile}<span class="tag warning">file missing</span>{/if}
                      {#if entry.error}<span class="error small">{entry.error}</span>{/if}
                    </div>
                  </td>
                  <td class="num">
                    {#if entry.duration}<Duration ms={entry.duration} />{/if}
                  </td>
                  <td class="mono faint">{entry.mongodbeeVersion}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        {/if}
      </div>
      <footer class="bezel-foot">
        <span class="small muted">
          {plural(history.applied, "apply", "applies")}, {plural(history.reverted, "rollback")}, {plural(history.failed, "failure")}
        </span>
        {#if history.total > history.shown}
          <span class="small faint">latest {history.shown} of {history.total}</span>
        {/if}
      </footer>
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

  .table {
    table-layout: fixed;
  }

  .table :global(th:first-child),
  .table :global(td:first-child) {
    padding-left: 16px;
  }

  .table :global(.status) {
    font-size: var(--text-xs);
  }

  .migration {
    display: flex;
    align-items: center;
    gap: 10px;
    overflow: hidden;
  }

  .name {
    font-weight: 500;
  }

  .small {
    font-size: var(--text-xs);
  }

  .error {
    color: var(--danger);
  }
</style>
