<script>
  import { onMount } from "svelte";
  import { api, collectionPath } from "./lib/api.js";
  import { formatNumber, plural } from "./lib/format.js";
  import { formatBytes } from "./lib/values.ts";
  import Status from "./Status.svelte";
  import CommandChip from "./CommandChip.svelte";
  import ErrorState from "./ErrorState.svelte";
  import Spectrum from "./controls/Spectrum.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import DateValue from "./values/DateValue.svelte";
  import IndexKey from "./values/IndexKey.svelte";

  let { collection } = $props();

  let report = $state(null);
  let error = $state(null);

  const TONE = { matching: "success", missing: "danger", different: "warning", extra: "neutral" };
  const ORDER = ["missing", "different", "extra", "matching"];
  const LABEL = {
    missing: "declared, not in the database",
    different: "declared differently",
    extra: "in the database only",
    matching: "in sync",
  };

  const REASON = {
    "pending-migration": "Created by a pending migration.",
    "not-synced": "Declared by the applied migrations but not in the database.",
    "no-migration": "Declared in the schemas but in no migration yet.",
  };

  const rows = $derived(
    report ? [...report.rows].sort((a, b) => ORDER.indexOf(a.status) - ORDER.indexOf(b.status)) : [],
  );

  const busiest = $derived(Math.max(1, ...rows.map((row) => row.usage?.ops ?? 0)));
  const totalSize = $derived(rows.reduce((sum, row) => sum + (row.sizeBytes ?? 0), 0));

  function options(spec) {
    if (!spec) return [];
    const result = [];
    if (spec.unique) result.push("unique");
    if (spec.sparse) result.push("sparse");
    if (spec.expireAfterSeconds !== undefined) result.push(`ttl ${spec.expireAfterSeconds}s`);
    if (spec.collation) result.push(`collation ${spec.collation.locale ?? ""}`.trim());
    if (spec.partialFilterExpression) result.push("partial");
    return result;
  }

  function partial(spec) {
    return spec?.partialFilterExpression ? JSON.stringify(spec.partialFilterExpression) : undefined;
  }

  async function load() {
    error = null;
    try {
      report = await api(collectionPath(collection.name, "indexes"));
    } catch (e) {
      error = e;
    }
  }

  onMount(load);
</script>

<div class="page">
  {#if error}
    <ErrorState {error} title="The indexes could not be read" onretry={load} />
  {:else if !report}
    <div class="empty-state">
      <Spectrum size={8} loading />
      <span class="muted">Reading the indexes</span>
    </div>
  {:else}
    <section class="tiles" aria-label="Index summary">
      {#each ORDER as status, i (status)}
        <div class="tile stagger {status}" class:zero={report.summary[status] === 0} style="--i: {i}">
          <span class="tile-count num">{report.summary[status]}</span>
          <Status tone={TONE[status]} hollow={status === "missing"} label={LABEL[status]} />
        </div>
      {/each}
      <div class="tile stagger metrics" style="--i: 4">
        {#if report.stats.size}
          <span class="metric"><span class="label-mono">index size</span><span class="num">{formatBytes(totalSize)}</span></span>
        {/if}
        {#if report.stats.documents !== undefined}
          <span class="metric"
            ><span class="label-mono">documents</span><span class="num">{formatNumber(report.stats.documents)}</span></span
          >
        {/if}
        {#if !report.stats.usage && !report.stats.size}
          <span class="faint small">Usage and size need the indexStats and collStats privileges</span>
        {/if}
      </div>
    </section>

    <section class="bezel">
      <div class="bezel-card card">
        <div class="scroller">
          <table class="table fixed">
            <colgroup>
              <col style="width: 108px" />
              <col style="width: 20%" />
              <col style="width: 24%" />
              <col style="width: 15%" />
              <col style="width: 10%" />
              <col style="width: 72px" />
              <col />
            </colgroup>
            <thead>
              <tr>
                <th>status</th>
                <th>name</th>
                <th>key</th>
                <th>options</th>
                <th>usage</th>
                <th class="num">size</th>
                <th>note</th>
              </tr>
            </thead>
            <tbody>
              {#each rows as row (row.name)}
                {@const spec = row.actual ?? row.declared}
                <tr class:ghost={!row.actual}>
                  <td><Status tone={TONE[row.status]} hollow={row.status === "missing"} label={row.status} /></td>
                  <td class="ellipsis"><Tooltip text={row.name} overflow><span class="mono name">{row.name}</span></Tooltip></td>
                  <td class="wrap-cell"><IndexKey fields={spec.key ?? {}} wrap /></td>
                  <td class="wrap-cell">
                    <span class="options">
                      {#each options(spec) as option}
                        {#if option === "partial"}
                          <Tooltip text="Only documents matching {partial(spec)}"><span class="tag">{option}</span></Tooltip>
                        {:else}
                          <span class="tag">{option}</span>
                        {/if}
                      {/each}
                    </span>
                  </td>
                  <td>
                    {#if row.usage}
                      <Tooltip text="{plural(row.usage.ops, 'lookup')} since the server started tracking this index">
                        <span class="usage">
                          <span class="meter" aria-hidden="true">
                            <span class="fill" style="--ratio: {row.usage.ops / busiest}"></span>
                          </span>
                          <span class="num ops" class:unused={row.usage.ops === 0}>{formatNumber(row.usage.ops)}</span>
                        </span>
                      </Tooltip>
                    {:else if row.actual}
                      <span class="faint small">not tracked</span>
                    {:else}
                      <span class="faint small">not built</span>
                    {/if}
                  </td>
                  <td class="num">
                    {#if row.sizeBytes !== undefined}<span class="mono small">{formatBytes(row.sizeBytes)}</span>{/if}
                  </td>
                  <td class="note">
                    <div class="note-lines">
                      {#if row.status === "different"}
                        <span class="muted">Differs on {row.differences.join(", ")}.</span>
                      {/if}
                      {#if row.status === "extra"}
                        <span class="faint">Not declared in the schemas.</span>
                        {#if row.usage?.ops === 0}
                          <span class="unused">
                            <span class="muted">Unused</span>
                            {#if row.usage.since}<span class="faint">since</span> <DateValue value={row.usage.since} />{/if}
                          </span>
                        {/if}
                      {/if}
                      {#if row.hint}
                        <span class="muted">{REASON[row.hint.reason]}</span>
                        <CommandChip command={row.hint.command} />
                      {/if}
                    </div>
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </div>
      {#if !report.declaredKnown}
        <footer class="bezel-foot">
          <span class="faint small">Not declared in the schemas, so there is nothing to compare with</span>
        </footer>
      {/if}
    </section>
  {/if}
</div>

<style>
  .page {
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 12px;
    min-height: 0;
  }

  .page > .bezel {
    flex: none;
    max-height: 100%;
  }

  .tiles {
    display: grid;
    flex: none;
    grid-template-columns: repeat(4, minmax(0, 1fr)) minmax(0, 1.3fr);
    gap: 8px;
  }

  .tile {
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    gap: 10px;
    min-height: 84px;
    padding: 10px 12px;
    border: 1px solid var(--card-border);
    background: var(--card);
  }

  .tile :global(.status) {
    font-size: var(--text-xs);
    white-space: normal;
  }

  .tile.missing:not(.zero) .tile-count {
    color: var(--danger);
  }

  .tile.different:not(.zero) .tile-count {
    color: var(--warning);
  }

  .tile.zero {
    border-style: dashed;
    background: transparent;
  }

  .tile.zero .tile-count {
    color: var(--text-faint);
  }

  .tile-count {
    font-size: 30px;
    font-weight: 500;
    line-height: 30px;
    letter-spacing: -0.04em;
  }

  .metrics {
    justify-content: center;
    gap: 6px;
    background: var(--frame);
  }

  .metric {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
  }

  .metric .num {
    font-size: var(--text-md);
    font-weight: 500;
  }

  .card {
    min-height: 0;
  }

  .scroller {
    min-height: 0;
    overflow: auto;
  }

  .fixed {
    table-layout: fixed;
  }

  .fixed th:first-child,
  .fixed td:first-child {
    padding-left: 16px;
  }

  tr.ghost td {
    background: repeating-linear-gradient(
      -45deg,
      transparent 0 6px,
      color-mix(in srgb, var(--card-border) 35%, transparent) 6px 7px
    );
  }

  .ellipsis {
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .options {
    display: inline-flex;
    flex-wrap: wrap;
    gap: 4px 6px;
  }

  .wrap-cell {
    padding-top: 6px;
    padding-bottom: 6px;
    white-space: normal;
  }

  .usage {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    width: 100%;
  }

  .meter {
    position: relative;
    flex: 1;
    height: 8px;
    min-width: 40px;
    border: 1px solid var(--card-border);
    background: var(--frame);
  }

  .fill {
    position: absolute;
    inset: 0 auto 0 0;
    width: calc(var(--ratio) * 100%);
    background: var(--leaf);
  }

  .ops {
    min-width: 32px;
    font-size: var(--text-xs);
    text-align: right;
  }

  .ops.unused {
    color: var(--text-faint);
  }

  .note {
    padding-top: 8px;
    padding-bottom: 8px;
    white-space: normal;
    line-height: 20px;
  }

  .note-lines {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }

  .unused {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px;
    font-size: var(--text-xs);
  }

  .small {
    font-size: var(--text-xs);
  }
</style>
