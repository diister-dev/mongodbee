<script>
  import { onMount } from "svelte";
  import { api, collectionPath } from "./lib/api.js";
  import { formatNumber, kindLabel, plural } from "./lib/format.js";
  import Segmented from "./controls/Segmented.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import DataView from "./DataView.svelte";
  import SchemaView from "./SchemaView.svelte";
  import IndexesView from "./IndexesView.svelte";
  import SummaryView from "./SummaryView.svelte";
  import ErrorState from "./ErrorState.svelte";
  import TypeTag from "./values/TypeTag.svelte";

  let { collection, tab, type, scope = "", open = "", navigate } = $props();

  let schema = $state(null);
  let schemaError = $state(null);

  const withSummary = $derived(!type && collection.kind !== "internal");

  const tabs = $derived([
    ...(withSummary ? [{ id: "summary", label: "Summary" }] : []),
    { id: "data", label: "Data" },
    { id: "schema", label: "Schema" },
    { id: "indexes", label: "Indexes" },
  ]);

  const shown = $derived(tab === "summary" && !withSummary ? "data" : tab);

  const count = $derived(type ? (collection.types.find((t) => t.name === type)?.count ?? 0) : collection.total);

  function select(nextTab) {
    navigate({ view: "collection", collection: collection.name, tab: nextTab, type });
  }

  async function loadSchema() {
    schemaError = null;
    try {
      schema = await api(collectionPath(collection.name, "schema"));
    } catch (e) {
      schemaError = e;
    }
  }

  onMount(loadSchema);
</script>

<header class="head">
  <div class="stack" class:stacked={Boolean(type)}>
    {#if type}
      <button
        class="behind"
        aria-label="Show every type of {collection.name}"
        onclick={() => navigate({ view: "collection", collection: collection.name, tab })}
      >
        <span class="mono">{collection.name}</span>
        <span class="faint">{kindLabel(collection.kind)}</span>
        <span class="faint back">every type</span>
      </button>
    {/if}
    <div class="front">
      {#if type}
        <TypeTag name={type} />
      {:else}
        <span class="title mono">{collection.name}</span>
      {/if}
      <Tooltip text={plural(count, "document")}><span class="count-badge">{formatNumber(count)}</span></Tooltip>
      {#if !type}
        <span class="faint kind">{kindLabel(collection.kind)}</span>
      {/if}
      {#if collection.model}
        <span class="faint kind">model <span class="mono">{collection.model}</span></span>
      {/if}
      {#if !collection.exists}
        <span class="warn kind">not created yet</span>
      {/if}
    </div>
  </div>

  <Segmented items={tabs} value={shown} label="Collection sections" onselect={select} />
</header>

{#if shown === "summary"}
  <div class="tab-body">
    <SummaryView {collection} {navigate} {schema} />
  </div>
{:else if schemaError}
  <ErrorState error={schemaError} title="The schema of {collection.name} could not be read" onretry={loadSchema} />
{:else if !schema}
  <div class="empty-state"><span class="shimmer">Loading schema</span></div>
{:else}
  {#key `${shown}|${type}|${scope}|${open}`}
    <div class="tab-body">
      {#if shown === "schema"}
        <SchemaView {schema} {type} {collection} />
      {:else if shown === "indexes"}
        <IndexesView {collection} />
      {:else}
        <DataView {collection} {schema} {type} initialScope={scope} initialOpen={open} />
      {/if}
    </div>
  {/key}
{/if}

<style>
  .tab-body {
    display: flex;
    flex: 1;
    flex-direction: column;
    min-height: 0;
  }

  .head {
    display: flex;
    flex: none;
    flex-wrap: wrap;
    align-items: flex-end;
    justify-content: space-between;
    gap: 12px 16px;
    padding: 4px 0 14px 8px;
  }

  .stack {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    min-width: 0;
  }

  .behind {
    position: relative;
    z-index: 0;
    display: inline-flex;
    align-items: center;
    gap: 10px;
    height: 30px;
    margin: 0 0 -6px 10px;
    padding: 0 12px 6px;
    border: 1px solid var(--card-border);
    border-bottom: 0;
    border-radius: 0;
    background: var(--frame);
    color: var(--text-muted);
    font-size: var(--text-xs);
    transition: color var(--dur-quick) var(--ease);
  }

  .behind .mono {
    font-size: var(--text-xs);
  }

  .back {
    opacity: 0;
    transition: opacity var(--dur-quick) var(--ease-out);
  }

  .behind:focus-visible .back {
    opacity: 1;
  }

  @media (hover: hover) and (pointer: fine) {
    .behind:hover {
      color: var(--text);
    }

    .behind:hover .back {
      opacity: 1;
    }
  }

  .front {
    position: relative;
    z-index: 1;
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 0;
    height: 44px;
    padding: 0 14px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-card);
    background: var(--card);
    box-shadow:
      0 0 0 4px var(--frame),
      0 0 0 5px var(--frame-border);
  }

  .stack:not(.stacked) .front {
    margin-top: 5px;
  }

  .stacked .front {
    margin-left: 5px;
  }

  .title {
    overflow: hidden;
    color: var(--text);
    font-size: 18px;
    font-weight: 600;
    letter-spacing: -0.02em;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .kind {
    font-size: var(--text-xs);
    white-space: nowrap;
  }

  .kind .mono {
    color: var(--text-muted);
  }

  .warn {
    color: var(--warning);
  }
</style>
