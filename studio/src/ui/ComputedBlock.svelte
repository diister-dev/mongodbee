<script>
  import Icon from "./Icon.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import SystemTag from "./values/SystemTag.svelte";
  import Value from "./values/Value.svelte";
  import RefPrefix from "./values/RefPrefix.svelte";
  import { ArrowUpRight } from "./lib/icons.js";
  import { sharedPrefix } from "./lib/values.ts";
  import { COMPUTED_REVISION, computedOf, sourceLabel, sourceTarget } from "./lib/computed.ts";

  let { document, node, infos = [], focus = [], navigate } = $props();

  const stored = $derived(computedOf(document) ?? {});
  const revision = $derived(stored[COMPUTED_REVISION]);
  const focused = $derived(focus[0] === "_computed" ? focus[1] : undefined);

  const rows = $derived.by(() => {
    const names = [
      ...infos.map((info) => info.name),
      ...Object.keys(node?.entries ?? {}),
      ...Object.keys(stored),
    ].filter((name, index, all) => name !== COMPUTED_REVISION && all.indexOf(name) === index);
    return names.map((name) => ({
      name,
      value: stored[name],
      missing: !(name in stored),
      node: node?.entries?.[name],
      info: infos.find((info) => info.name === name),
    }));
  });

  function open(info) {
    const target = sourceTarget(info, document);
    if (target) navigate?.(target);
  }

  function reveal(element, active) {
    if (active) requestAnimationFrame(() => element.scrollIntoView({ block: "center" }));
    return {
      update(next) {
        if (next) element.scrollIntoView({ block: "center" });
      },
    };
  }
</script>

<section class="computed" aria-label="Computed fields">
  <header class="head">
    <SystemTag />
    <span class="faint small">maintained by mongodbee, never edited here</span>
    <span class="spacer"></span>
    {#if typeof revision === "number"}
      <Tooltip text="Bumped by every transaction that recomputes this document">
        <span class="revision small">revision <span class="num">{revision}</span></span>
      </Tooltip>
    {/if}
  </header>
  {#each rows as row (row.name)}
    <div class="row" class:targeted={focused === row.name} use:reveal={focused === row.name}>
      <div class="line">
        <span class="name mono">{row.name}</span>
        <span class="value">
          {#if row.missing}
            <span class="faint small">not computed yet</span>
          {:else if Array.isArray(row.value)}
            {#if row.value.length === 0}
              <span class="faint small">none</span>
            {:else}
              {@const prefix = sharedPrefix(row.value)}
              <span class="items">
                {#if prefix}<RefPrefix name={prefix} />{/if}
                {#each row.value as item, index (index)}
                  <Value value={item} node={row.node?.item} field={row.name} bare={Boolean(prefix)} />
                {/each}
              </span>
            {/if}
          {:else}
            <Value value={row.value} node={row.node} field={row.name} />
          {/if}
        </span>
        {#if row.info && navigate}
          <button class="link small press" type="button" onclick={() => open(row.info)}>
            <span>{sourceLabel(row.info.source)} sources</span>
            <Icon icon={ArrowUpRight} size={12} />
          </button>
        {/if}
      </div>
      {#if row.info}
        <div class="origin">
          <span class="description small">{row.info.description}</span>
        </div>
      {/if}
    </div>
  {/each}
</section>

<style>
  .computed {
    margin-top: 16px;
    margin-left: -16px;
    border-top: 1px dashed var(--card-border);
  }

  .head {
    display: flex;
    align-items: center;
    gap: 8px;
    height: 36px;
  }

  .spacer {
    flex: 1;
  }

  .small {
    font-size: var(--text-xs);
  }

  .revision {
    color: var(--text-muted);
    font-family: var(--font-mono);
  }

  .row {
    padding: 6px 0;
    border-bottom: 1px solid var(--hairline);
  }

  .row:last-child {
    border-bottom: 0;
  }

  .row.targeted {
    margin: 0 -8px;
    padding: 6px 8px;
    background: var(--select-bg);
    box-shadow: inset 2px 0 0 var(--select-edge);
  }

  .line {
    display: grid;
    grid-template-columns: minmax(96px, 32%) minmax(0, 1fr) auto;
    align-items: center;
    gap: 12px;
    min-height: 24px;
  }

  .name {
    overflow: hidden;
    color: var(--text-muted);
    font-size: var(--text-xs);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .value {
    display: flex;
    min-width: 0;
    font-size: var(--text-sm);
  }

  .items {
    display: flex;
    flex-wrap: wrap;
    gap: 4px 6px;
    min-width: 0;
  }

  .origin {
    display: grid;
    grid-template-columns: minmax(96px, 32%) minmax(0, 1fr);
    gap: 12px;
    margin-top: 2px;
  }

  .description {
    grid-column: 2;
    color: var(--text-faint);
  }

  .link {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 0;
    border: 0;
    background: none;
    color: var(--text-muted);
    white-space: nowrap;
    cursor: pointer;
  }

  @media (hover: hover) and (pointer: fine) {
    .link:hover {
      color: var(--text);
      text-decoration: underline;
      text-decoration-color: var(--card-border);
      text-underline-offset: 3px;
    }
  }
</style>
