<script>
  import { defaultTab, formatNumber, GROUPS, kindLabel, plural } from "./lib/format.js";
  import Status from "./Status.svelte";
  import BarRow from "./charts/BarRow.svelte";
  import ChartFrame from "./charts/ChartFrame.svelte";
  import StatStrip from "./charts/StatStrip.svelte";
  import TypeTag from "./values/TypeTag.svelte";

  let { meta, overview, migrations, navigate } = $props();

  const KIND_COLOR = {
    collection: "var(--spectrum-1)",
    multiCollection: "var(--spectrum-2)",
    scopedMultiCollection: "var(--spectrum-3)",
    multiModelInstance: "var(--spectrum-4)",
    undeclared: "var(--spectrum-5)",
    internal: "var(--ink-panel-line)",
  };

  const collections = $derived(overview?.collections ?? []);
  const declared = $derived(collections.filter((c) => c.kind !== "undeclared" && c.kind !== "internal"));
  const undeclared = $derived(collections.filter((c) => c.kind === "undeclared"));
  const documents = $derived(declared.reduce((sum, c) => sum + (c.total ?? 0), 0));
  const pending = $derived(migrations?.pending ?? 0);

  const groups = $derived(
    GROUPS.map((group) => ({ ...group, items: collections.filter((c) => c.kind === group.kind) })).filter(
      (group) => group.items.length > 0,
    ),
  );

  const MAP_ROWS = 8;

  const bySize = $derived(
    collections
      .filter((c) => c.kind !== "internal")
      .toSorted((a, b) => (b.total ?? 0) - (a.total ?? 0) || a.name.localeCompare(b.name)),
  );
  const ranked = $derived(bySize.slice(0, MAP_ROWS));
  const rest = $derived(bySize.slice(MAP_ROWS));
  const largest = $derived(Math.max(1, ...ranked.map((c) => c.total ?? 0)));

  function barRatio(total) {
    if (!total) return 0;
    return Math.max(0.01, Math.log10(total + 1) / Math.log10(largest + 1));
  }

  const kindsPresent = $derived(GROUPS.filter((group) => collections.some((c) => c.kind === group.kind)));

  const SOURCE = {
    project: "schemas.ts",
    "latest-migration": "the latest migration",
    none: "no schemas",
  };

  function open(collection) {
    navigate({ view: "collection", collection: collection.name, tab: defaultTab(collection.kind) });
  }
</script>

<div class="bezel page">
  <div class="bezel-card body">
    <div class="scroll">
      <header class="head">
        <span class="eyebrow">overview</span>
        <h1 class="page-title mono-title">{meta?.database}</h1>
        <p class="muted">Everything mongodbee manages in this database, read-only.</p>
      </header>

      {#each meta?.warnings ?? [] as warning}
        <div class="notice warn">{warning}</div>
      {/each}

      {#snippet pendingLink()}
        <button class="stat-link press" onclick={() => navigate({ view: "migrations", section: "plan" })}>
          <Status tone="warning" hollow label="{pending} pending" />
        </button>
      {/snippet}

      {#snippet restFoot()}
        {plural(rest.length, "more collection")}, {formatNumber(rest.reduce((sum, c) => sum + (c.total ?? 0), 0))} documents
      {/snippet}

      {#snippet kindLegend()}
        {#each kindsPresent as group (group.kind)}
          <span class="legend-item"><i style="background: {KIND_COLOR[group.kind]}"></i>{kindLabel(group.kind)}</span>
        {/each}
      {/snippet}

      <div class="inset">
        <StatStrip
          items={[
            { label: "collections", value: formatNumber(declared.length) },
            { label: "documents", value: formatNumber(documents) },
            {
              label: "migrations applied",
              value: String(migrations?.applied ?? 0),
              of: `of ${migrations?.total ?? 0}`,
              extra: pending > 0 ? pendingLink : undefined,
            },
            { label: "undeclared", value: formatNumber(undeclared.length), faint: undeclared.length === 0 },
          ]}
        />
      </div>

      <div class="inset">
        <ChartFrame
          title="largest collections"
          label="Largest collections by documents"
          grid
          index={4}
          legend={kindLegend}
          foot={rest.length > 0 ? restFoot : undefined}
        >
          <ol class="bars">
            {#each ranked as collection (collection.name)}
              <BarRow
                ratio={barRatio(collection.total)}
                fill={KIND_COLOR[collection.kind] ?? "var(--text-faint)"}
                label="{collection.name}, {plural(collection.total ?? 0, 'document')}"
                count={formatNumber(collection.total)}
                onclick={() => open(collection)}
              >
                {#snippet name()}<span class="bar-name mono">{collection.name}</span>{/snippet}
              </BarRow>
            {/each}
          </ol>
        </ChartFrame>
      </div>

      <table class="table overview">
        <colgroup>
          <col style="width: 32%" />
          <col />
          <col style="width: 120px" />
        </colgroup>
        <thead>
          <tr>
            <th>name</th>
            <th>types</th>
            <th class="num">documents</th>
          </tr>
        </thead>
        {#each groups as group (group.kind)}
          <tbody>
            <tr class="group">
              <td colspan="3">
                <span class="group-label"><i style="background: {KIND_COLOR[group.kind]}"></i>{group.label}</span>
              </td>
            </tr>
            {#each group.items as collection (collection.name)}
              <tr class="row" class:dim={collection.kind === "internal"} onclick={() => open(collection)}>
                <td>
                  <span class="mono name">{collection.name}</span>
                  {#if !collection.exists}
                    <span class="faint note">not created yet</span>
                  {/if}
                </td>
                <td class="types">
                  {#if collection.kind !== "collection"}
                    {#each collection.types.filter((t) => !t.meta) as type (type.name)}
                      <span class="type">
                        <TypeTag name={type.name} />
                        <span class="faint num">{formatNumber(type.count)}</span>
                      </span>
                    {/each}
                    {#if collection.scopes}
                      <span class="muted">across {plural(collection.scopes.distinct, "scope")}</span>
                    {/if}
                  {/if}
                </td>
                <td class="num">{formatNumber(collection.total)}</td>
              </tr>
            {/each}
          </tbody>
        {/each}
      </table>
    </div>
  </div>
  <footer class="bezel-foot">
    <span class="muted small">
      Schemas from {SOURCE[meta?.schemasSource] ?? "schemas.ts"}
    </span>
    <span class="faint small num mono">mongodbee {meta?.version}</span>
  </footer>
</div>

<style>
  .page {
    flex: 1;
  }

  .body {
    flex: 1;
  }

  .scroll {
    display: flex;
    flex-direction: column;
    gap: 24px;
    padding: 28px 0 8px;
    overflow: auto;
  }

  .head,
  .notice {
    margin: 0 28px;
  }

  .head {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .head p {
    margin: 0;
  }

  .mono-title {
    font-family: var(--font-mono);
    font-size: 26px;
    font-weight: 500;
    letter-spacing: -0.04em;
  }

  .notice.warn {
    color: var(--warning);
  }

  .inset {
    margin: 0 28px;
  }

  .stat-link {
    display: inline-flex;
    padding: 0;
    border: 0;
    background: none;
  }

  .stat-link :global(.status) {
    font-size: var(--text-xs);
  }

  .group-label {
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }

  .group-label i {
    width: 7px;
    height: 7px;
  }

  .bars {
    display: flex;
    flex-direction: column;
    gap: 2px;
    margin: 0;
    padding: 0;
  }

  .bar-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .overview th:first-child,
  .overview td:first-child {
    padding-left: 28px;
  }

  .overview th:last-child,
  .overview td:last-child {
    padding-right: 28px;
  }

  .overview th {
    border-top: 1px solid var(--card-border);
  }

  .group td {
    height: 32px;
    padding-top: 12px;
    border-bottom: 0;
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: 11px;
    vertical-align: bottom;
  }

  @media (hover: hover) and (pointer: fine) {
    .overview tbody tr.group:hover {
      background: none;
    }
  }

  .row {
    cursor: pointer;
  }

  .row.dim .name {
    color: var(--text-faint);
  }

  .name {
    font-size: var(--text-sm);
  }

  .note {
    margin-left: 8px;
    font-size: var(--text-xs);
  }

  .types {
    white-space: normal;
  }

  .type {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    margin-right: 16px;
  }

  .type .num {
    font-size: var(--text-xs);
  }

  .small {
    font-size: var(--text-xs);
  }

  .bezel-foot {
    padding-right: 10px;
  }
</style>
