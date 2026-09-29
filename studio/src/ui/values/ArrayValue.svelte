<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import HoverCard from "./HoverCard.svelte";
  import Value from "./Value.svelte";
  import RefPrefix from "./RefPrefix.svelte";
  import { plural } from "../lib/format.js";
  import { sharedPrefix } from "../lib/values.ts";

  interface Props {
    value: unknown[];
    item?: Record<string, unknown>;
    field?: string;
    visible?: number;
  }

  let { value, item, field, visible = 2 }: Props = $props();

  const shown = $derived(value.slice(0, visible));
  const rest = $derived(value.length - shown.length);
  const listed = $derived(value.slice(0, 50));
  const prefix = $derived(sharedPrefix(value));
</script>

{#if value.length === 0}
  <Tooltip text="Empty list"><span class="empty mono">[ ]</span></Tooltip>
{:else}
  <HoverCard label={plural(value.length, "item")}>
    <span class="array">
      {#if prefix}<RefPrefix name={prefix} />{/if}
      {#each shown as entry, index (index)}
        <span class="entry"><Value value={entry} node={item} {field} compact bare={Boolean(prefix)} /></span>
      {/each}
      {#if rest > 0}
        <span class="more num">+{rest} more</span>
      {/if}
    </span>
    {#snippet card()}
      <div class="vc-body list">
        {#each listed as entry, index (index)}
          <div class="vc-row item-row">
            <span class="vc-label num">{index}</span>
            <span class="item-value"><Value value={entry} node={item} {field} /></span>
          </div>
        {/each}
      </div>
      <div class="vc-foot">
        <span>{plural(value.length, "item")}</span>
        {#if value.length > listed.length}<span class="faint">first {listed.length} shown</span>{/if}
      </div>
    {/snippet}
  </HoverCard>
{/if}

<style>
  .array {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    max-width: 100%;
    overflow: hidden;
  }

  .entry {
    display: inline-flex;
    flex: 0 1 auto;
    min-width: 0;
    max-width: 180px;
  }

  .more {
    flex: none;
    color: var(--text-faint);
    font-size: var(--text-xs);
  }

  .empty {
    color: var(--text-faint);
  }

  .list {
    max-height: 260px;
    overflow: auto;
  }

  .item-row {
    grid-template-columns: 22px minmax(0, 1fr);
  }

  .item-row + .item-row {
    border-top: 1px solid var(--hairline);
  }

  .item-value {
    display: flex;
    min-width: 0;
  }
</style>
