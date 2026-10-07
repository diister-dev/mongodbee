<script>
  import { quiet } from "../lib/motion.ts";

  let { items, value, onselect, label, size = "md" } = $props();

  let root = $state();

  function move(event, index) {
    const last = items.length - 1;
    const next =
      event.key === "ArrowRight"
        ? (index + 1) % items.length
        : event.key === "ArrowLeft"
          ? (index + last) % items.length
          : event.key === "Home"
            ? 0
            : event.key === "End"
              ? last
              : null;
    if (next === null) return;
    event.preventDefault();
    quiet();
    onselect(items[next].id);
    requestAnimationFrame(() => root?.querySelector(`[data-segment="${items[next].id}"]`)?.focus());
  }
</script>

<div class="segmented" class:sm={size === "sm"} role="tablist" aria-label={label} bind:this={root}>
  {#each items as item, index (item.id)}
    <button
      role="tab"
      class="segment press"
      class:active={value === item.id}
      aria-selected={value === item.id}
      tabindex={value === item.id ? 0 : -1}
      data-segment={item.id}
      onclick={() => onselect(item.id)}
      onkeydown={(event) => move(event, index)}
    >
      {item.label}
      {#if item.badge}<span class="count-badge badge">{item.badge}</span>{/if}
    </button>
  {/each}
</div>

<style>
  .segmented {
    display: inline-flex;
    flex: none;
    gap: 0;
    padding: 3px;
    border: 1px solid var(--card-border);
    border-radius: 0;
    background: var(--card);
  }

  .segment {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    height: 28px;
    padding: 0 14px;
    border: 0;
    border-radius: 0;
    background: none;
    color: var(--text-muted);
    font-size: var(--text-sm);
    transition: transform var(--dur-press) var(--ease-out);
  }

  .segmented.sm {
    padding: 2px;
  }

  .sm .segment {
    height: 22px;
    padding: 0 9px;
    font-size: var(--text-xs);
  }

  .segment.active {
    background: var(--text);
    color: var(--card);
  }

  @media (hover: hover) and (pointer: fine) {
    .segment:not(.active):hover {
      background: var(--bg-active);
      color: var(--text);
    }
  }

  .badge {
    min-width: 18px;
    height: 18px;
    padding: 0 5px;
    background: var(--bg-active);
    color: var(--text);
    font-size: 11px;
    line-height: 18px;
  }

  .segment.active .badge {
    background: rgb(255 253 248 / 0.18);
    color: var(--card);
  }
</style>
