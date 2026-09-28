<script>
  let { ratio, fill = "var(--text)", segments, label } = $props();

  const total = $derived(segments ? segments.reduce((sum, segment) => sum + segment.value, 0) : 0);
</script>

<span class="track" role="img" aria-label={label}>
  <span class="filled" style="--ratio: {Math.max(0, Math.min(1, ratio))}">
    {#if segments && total > 0}
      {#each segments as segment (segment.key)}
        {#if segment.value > 0}
          <span class="segment" style="flex-grow: {segment.value}; background: {segment.color}"></span>
        {/if}
      {/each}
    {:else}
      <span class="segment" style="flex-grow: 1; background: {fill}"></span>
    {/if}
  </span>
</span>

<style>
  .track {
    display: block;
    height: 10px;
    background: color-mix(in srgb, var(--card-border) 45%, transparent);
  }

  .filled {
    display: flex;
    gap: 1px;
    width: calc(var(--ratio) * 100%);
    height: 100%;
  }

  .segment {
    flex-basis: 0;
    min-width: 2px;
  }
</style>
