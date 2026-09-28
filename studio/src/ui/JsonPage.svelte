<script>
  import Icon from "./Icon.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import { idParam } from "./lib/format.js";
  import { ChevronRight } from "./lib/icons.js";
  import { compactJson, tokenizeJson } from "./lib/json-format.ts";

  let { items, selected = null, openId = null, onopen, list = $bindable() } = $props();

  const blocks = $derived(items.map((item) => ({ item, tokens: tokenizeJson(compactJson(item)) })));
</script>

<ol class="json-page" bind:this={list}>
  {#each blocks as { item, tokens }, index (idParam(item._id))}
    <li class="block" class:selected={selected === index} class:viewing={openId === idParam(item._id)}>
      <span class="gutter num">{index + 1}</span>
      <pre class="code mono">{#each tokens as token}<span class={token.kind}>{token.text}</span>{/each}</pre>
      <Tooltip text="Open document" kbd="↵">
        <button class="open press" type="button" aria-label="Open document" onclick={() => onopen(item)}>
          <Icon icon={ChevronRight} size={13} />
        </button>
      </Tooltip>
    </li>
  {/each}
</ol>

<style>
  .json-page {
    margin: 0;
    padding: 0;
    list-style: none;
    background: var(--card);
  }

  .block {
    display: grid;
    grid-template-columns: 40px minmax(0, 1fr) 32px;
    align-items: start;
    border-bottom: 1px solid var(--hairline);
  }

  .block.selected {
    box-shadow: inset 2px 0 0 var(--select-edge);
  }

  .block.viewing {
    background: var(--select-bg);
  }

  .gutter {
    padding: 8px 10px 0 0;
    color: var(--text-faint);
    font-size: 10.5px;
    line-height: 18px;
    text-align: right;
    user-select: none;
  }

  .code {
    margin: 0;
    padding: 8px 0;
    color: var(--text-muted);
    font-size: 11.5px;
    line-height: 18px;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    tab-size: 2;
  }

  .key {
    color: var(--text);
  }

  .string {
    color: var(--syntax-string);
  }

  .number,
  .literal {
    color: var(--accent);
  }

  .punct {
    color: var(--text-faint);
  }

  .open {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    margin-top: 5px;
    padding: 0;
    border: 0;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-faint);
  }

  @media (hover: hover) and (pointer: fine) {
    .open:hover {
      background: var(--bg-active);
      color: var(--text);
    }
  }
</style>
