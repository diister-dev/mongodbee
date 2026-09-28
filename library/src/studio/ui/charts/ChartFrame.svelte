<script>
  let { title, label, grid = false, index = 0, legend, foot, children } = $props();
</script>

<figure class="frame stagger" class:grid style="--i: {index}" aria-label={label ?? title}>
  <div class="head">
    <span class="eyebrow">{title}</span>
    {#if legend}<span class="legend">{@render legend()}</span>{/if}
  </div>
  {@render children()}
  {#if foot}<p class="foot faint">{@render foot()}</p>{/if}
</figure>

<style>
  .frame {
    min-width: 0;
    margin: 0;
    padding: 14px 16px 12px;
    border: 1px solid var(--hairline);
    background-color: var(--frame);
  }

  .frame.grid {
    background-image:
      linear-gradient(var(--grid-line) 1px, transparent 1px),
      linear-gradient(90deg, var(--grid-line) 1px, transparent 1px);
    background-size: 16px 16px;
  }

  .head {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 8px 16px;
    margin-bottom: 10px;
  }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 4px 12px;
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: 10.5px;
  }

  .legend :global(.legend-item) {
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }

  .legend :global(.legend-item i) {
    width: 7px;
    height: 7px;
  }

  .foot {
    margin: 8px 6px 0;
    font-size: var(--text-xs);
  }
</style>
