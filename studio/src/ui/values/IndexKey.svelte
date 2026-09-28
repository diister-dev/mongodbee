<script lang="ts">
  import Icon from "../Icon.svelte";
  import Tooltip from "../controls/Tooltip.svelte";
  import { ArrowDown, ArrowUp } from "../lib/icons.js";

  interface Props {
    fields: Record<string, unknown>;
    unique?: boolean;
    sparse?: boolean;
    partial?: unknown;
    ttl?: number;
    wrap?: boolean;
  }

  let { fields, unique = false, sparse = false, partial, ttl, wrap = false }: Props = $props();

  const parts = $derived(
    Object.entries(fields).map(([path, direction]) => ({
      path,
      direction: direction === 1 ? "asc" : direction === -1 ? "desc" : String(direction),
    })),
  );

  const label = $derived(
    parts.map((p) => `${p.path} ${p.direction}`).join(", ") + (unique ? ", unique" : ""),
  );
</script>

<span class="key" class:wrap aria-label={label}>
  {#each parts as part, index (part.path)}
    <span class="field">
      <span class="mono path">{part.path}</span>
      {#if part.direction === "asc"}
        <span class="dir" aria-label="ascending"><Icon icon={ArrowUp} size={11} stroke={1.75} /></span>
      {:else if part.direction === "desc"}
        <span class="dir desc" aria-label="descending"><Icon icon={ArrowDown} size={11} stroke={1.75} /></span>
      {:else}
        <span class="mono kind">{part.direction}</span>
      {/if}
    </span>
    {#if index < parts.length - 1}<span class="plus">+</span>{/if}
  {/each}
  {#if unique}<span class="tag">unique</span>{/if}
  {#if sparse}<span class="tag">sparse</span>{/if}
  {#if partial}<Tooltip text="Only documents matching {JSON.stringify(partial)}"><span class="tag">partial</span></Tooltip>{/if}
  {#if ttl !== undefined}<span class="tag">ttl {ttl}s</span>{/if}
</span>

<style>
  .key {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    max-width: 100%;
    overflow: hidden;
    white-space: nowrap;
    vertical-align: middle;
  }

  .key.wrap {
    flex-wrap: wrap;
    row-gap: 2px;
    overflow: visible;
  }

  .field {
    display: inline-flex;
    align-items: center;
    gap: 2px;
  }

  .dir {
    display: inline-flex;
    color: var(--text-faint);
  }

  .dir.desc {
    color: var(--accent);
  }

  .kind {
    color: var(--text-muted);
  }

  .plus {
    color: var(--text-faint);
    font-size: var(--text-xs);
  }

  .tag {
    height: 18px;
    padding: 0 5px;
    font-size: 11px;
    line-height: 16px;
  }
</style>
