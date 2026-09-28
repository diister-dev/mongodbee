<script>
  import { groupOperations } from "./lib/interaction.ts";
  import { ChevronRight } from "./lib/icons.js";
  import { expand, fade } from "./lib/motion.ts";
  import Icon from "./Icon.svelte";
  import OperationRow from "./OperationRow.svelte";
  import TypeTag from "./values/TypeTag.svelte";

  let { operations, row, summary } = $props();

  let open = $state({});

  const groups = $derived(groupOperations(operations));

  function types(group) {
    const names = [];
    for (const item of group.items) {
      const target = item.target ?? "";
      const index = target.lastIndexOf(".");
      const name = index > 0 && !target.includes("→") ? target.slice(index + 1) : target;
      if (name && !names.includes(name)) names.push(name);
    }
    return names;
  }
</script>

<ol class="stack">
  {#each groups as group, index (index)}
    {#if group.items.length === 1}
      {#if row}
        {@render row(group.items[0], false)}
      {:else}
        <OperationRow operation={group.items[0]} />
      {/if}
    {:else}
      <li class="group">
        <button class="summary" onclick={() => (open = { ...open, [index]: !open[index] })} aria-expanded={!!open[index]}>
          <span class="caret" class:open={open[index]}><Icon icon={ChevronRight} size={12} /></span>
          <span class="label">{group.verb}</span>
          <span class="count-badge">{group.items.length}</span>
          <span class="noun muted">{group.label.slice(group.verb.length + String(group.items.length).length + 2)}</span>
          {#if !open[index]}
            <span class="peek" in:fade={{ duration: 150 }} out:fade={{ duration: 100, exit: true }}>
              {#each types(group).slice(0, 4) as name}
                {#if group.items.some((item) => (item.target ?? "").includes("."))}
                  <TypeTag {name} />
                {:else}
                  <span class="mono muted">{name}</span>
                {/if}
              {/each}
              {#if types(group).length > 4}<span class="faint">+{types(group).length - 4}</span>{/if}
              {#if summary}{@render summary(group)}{/if}
            </span>
          {/if}
        </button>
        {#if open[index]}
          <ol class="items" in:expand out:expand={{ exit: true }}>
            {#each group.items as operation, itemIndex (itemIndex)}
              {#if row}
                {@render row(operation, true)}
              {:else}
                <OperationRow {operation} nested />
              {/if}
            {/each}
          </ol>
        {/if}
      </li>
    {/if}
  {/each}
</ol>

<style>
  .stack,
  .items {
    display: flex;
    flex-direction: column;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .group {
    border-bottom: 1px solid var(--hairline);
  }

  .group:last-child {
    border-bottom: 0;
  }

  .summary {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    min-height: 36px;
    padding: 4px 0;
    border: 0;
    background: none;
    color: inherit;
    font: inherit;
    text-align: left;
  }

  .caret {
    display: inline-flex;
    width: 14px;
    color: var(--text-faint);
    transition: transform var(--dur-move) var(--ease-enter);
  }

  .caret.open {
    transform: rotate(90deg);
  }

  .label {
    font-weight: 500;
  }

  .noun {
    font-size: var(--text-sm);
  }

  .peek {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    margin-left: 8px;
    font-size: var(--text-xs);
  }

  .items {
    margin-left: 12px;
    padding-left: 10px;
    border-left: 1px solid var(--hairline);
  }
</style>
