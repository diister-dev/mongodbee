<script>
  let { items, start = 0 } = $props();
</script>

<div class="strip">
  <dl class="stats" style="--columns: {items.length}">
    {#each items as item, index (item.label)}
      <div class="stat stagger" style="--i: {start + index}">
        <dt class="label-mono">{item.label}</dt>
        <dd class="num" class:faint={item.faint}>
          {item.value}{#if item.of !== undefined}<span class="of">{item.of}</span>{/if}
        </dd>
        {#if item.note}<span class="note small faint">{item.note}</span>{/if}
        {#if item.extra}<div class="extra">{@render item.extra()}</div>{/if}
      </div>
    {/each}
  </dl>
</div>

<style>
  .strip {
    container-type: inline-size;
  }

  .stats {
    display: grid;
    grid-template-columns: repeat(var(--columns), minmax(0, 1fr));
    margin: 0;
    border-top: 1px solid var(--hairline);
    border-bottom: 1px solid var(--hairline);
  }

  .stat {
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 0;
    padding: 14px 16px 16px 0;
  }

  .stat + .stat {
    padding-left: 16px;
    border-left: 1px solid var(--hairline);
  }

  .stat dd {
    overflow: hidden;
    margin: 0;
    font-size: 30px;
    font-weight: 500;
    line-height: 32px;
    letter-spacing: -0.035em;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .of {
    margin-left: 8px;
    color: var(--text-faint);
    font-size: var(--text-md);
    font-weight: 400;
    letter-spacing: 0;
  }

  .note {
    margin-top: -4px;
    font-size: var(--text-xs);
  }

  .extra {
    position: absolute;
    top: 12px;
    right: 12px;
    display: inline-flex;
  }

  @container (max-width: 680px) {
    .stats {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .stat:nth-child(odd) {
      padding-left: 0;
      border-left: 0;
    }

    .stat:nth-child(n + 3) {
      border-top: 1px solid var(--hairline);
    }
  }
</style>
