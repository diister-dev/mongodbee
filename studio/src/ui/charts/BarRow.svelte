<script>
  import Bar from "./Bar.svelte";

  let { ratio, fill, segments, label, count, onclick, name, aside, dim = false } = $props();
</script>

<li class="item" class:dim>
  <svelte:element
    this={onclick ? "button" : "div"}
    class="row"
    class:has-aside={Boolean(aside)}
    type={onclick ? "button" : undefined}
    {onclick}
    role={onclick ? undefined : "group"}
    aria-label={onclick ? label : undefined}
  >
    <span class="name">{@render name()}</span>
    <Bar {ratio} {fill} {segments} {label} />
    <span class="count num">{count}</span>
    {#if aside}<span class="aside">{@render aside()}</span>{/if}
  </svelte:element>
</li>

<style>
  .item {
    container-type: inline-size;
    list-style: none;
  }

  .row {
    display: grid;
    grid-template-columns: minmax(120px, 220px) minmax(0, 1fr) 72px;
    align-items: center;
    gap: 14px;
    width: 100%;
    min-height: 26px;
    padding: 0 6px;
    border: 0;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text);
    font: inherit;
    text-align: left;
  }

  .row.has-aside {
    grid-template-columns: minmax(120px, 220px) minmax(0, 1fr) 72px minmax(0, 200px);
  }

  .name {
    display: flex;
    align-items: center;
    min-width: 0;
    overflow: hidden;
    white-space: nowrap;
  }

  .count {
    color: var(--text-muted);
    font-size: var(--text-xs);
    text-align: right;
  }

  .aside {
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 0;
    overflow: hidden;
    color: var(--text-faint);
    font-size: var(--text-xs);
    white-space: nowrap;
  }

  .dim {
    opacity: 0.55;
  }

  button.row {
    cursor: pointer;
  }

  @media (hover: hover) and (pointer: fine) {
    button.row:hover {
      background: var(--card);
    }
  }

  @container (max-width: 760px) {
    .row.has-aside {
      grid-template-columns: minmax(100px, 200px) minmax(0, 1fr) 64px;
      row-gap: 0;
      padding-block: 3px;
    }

    .has-aside .aside {
      grid-column: 2 / -1;
    }
  }

  @container (max-width: 420px) {
    .row,
    .row.has-aside {
      grid-template-columns: minmax(0, 1fr) 64px;
    }

    .row :global(.track),
    .has-aside .aside {
      grid-column: 1 / -1;
    }
  }
</style>
