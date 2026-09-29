<script>
  import Icon from "./Icon.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import { ChevronRight } from "./lib/icons.js";
  import { parseTypedId } from "./lib/values.ts";
  import { COMPUTED_PREFIX, COMPUTED_REVISION, COMPUTED_ROOT, computedOf } from "./lib/computed.ts";
  import Scope from "./values/Scope.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import Value from "./values/Value.svelte";
  import SystemTag from "./values/SystemTag.svelte";

  const HEAD_KEYS = ["_id", "_type", "_scope", COMPUTED_ROOT];

  let { item, fields, selected = false, open = false, onopen } = $props();

  const showType = $derived(typeof item._type === "string" && parseTypedId(item._id)?.prefix !== item._type);
  const entries = $derived(Object.entries(item).filter(([key]) => !HEAD_KEYS.includes(key)));
  const computed = $derived(
    Object.entries(computedOf(item) ?? {}).filter(([key]) => key !== COMPUTED_REVISION),
  );
</script>

<article class="record" class:selected class:viewing={open} aria-current={selected ? "true" : undefined}>
  <header class="head">
    <button class="title" type="button" onclick={() => onopen([])} aria-label="Open document">
      {#if showType}<TypeTag name={item._type} />{/if}
      <span class="id"><Value value={item._id} field="_id" /></span>
    </button>
    {#if item._scope !== undefined}
      <span class="scope"><Scope value={item._scope} /></span>
    {/if}
    <Tooltip text="Open document" kbd="↵">
      <button class="open press" type="button" aria-label="Open document" onclick={() => onopen([])}>
        <Icon icon={ChevronRight} size={13} />
      </button>
    </Tooltip>
  </header>
  {#if entries.length > 0 || computed.length > 0}
    <dl class="fields">
      {#each entries as [key, value] (key)}
        <div class="field">
          <dt class="key mono">{key}</dt>
          <dd class="value">
            <Value {value} node={fields?.[key]} field={key} compact onopen={() => onopen([key])} />
          </dd>
        </div>
      {/each}
      {#if computed.length > 0}
        <div class="system-rule" role="presentation"><SystemTag /></div>
        {#each computed as [key, value] (key)}
          <div class="field computed">
            <dt class="key mono">{key}</dt>
            <dd class="value">
              <Value
                {value}
                node={fields?.[COMPUTED_PREFIX + key]}
                field={key}
                compact
                onopen={() => onopen([COMPUTED_ROOT, key])}
              />
            </dd>
          </div>
        {/each}
      {/if}
    </dl>
  {/if}
</article>

<style>
  .record {
    border: 1px solid var(--card-border);
    border-radius: var(--radius-card);
    background: var(--card);
  }

  .record.selected {
    border-color: var(--text);
  }

  .record.viewing .head {
    background: var(--select-bg);
  }

  .head {
    display: flex;
    align-items: center;
    gap: 10px;
    height: 32px;
    padding: 0 4px 0 10px;
    border-bottom: 1px solid var(--hairline);
  }

  .title {
    display: inline-flex;
    flex: 1;
    align-items: center;
    gap: 8px;
    min-width: 0;
    padding: 0;
    border: 0;
    background: none;
    color: var(--text);
    font: inherit;
    text-align: left;
    cursor: pointer;
  }

  .id {
    display: inline-flex;
    min-width: 0;
    overflow: hidden;
  }

  .scope {
    display: inline-flex;
    flex: none;
    max-width: 40%;
  }

  .open {
    display: inline-flex;
    flex: none;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    padding: 0;
    border: 0;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-faint);
  }

  .fields {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
    column-gap: 20px;
    margin: 0;
    padding: 2px 10px 4px;
  }

  .field {
    display: grid;
    grid-template-columns: minmax(64px, 40%) minmax(0, 1fr);
    align-items: center;
    gap: 10px;
    min-width: 0;
    height: 26px;
    border-bottom: 1px dashed var(--hairline);
  }

  .system-rule {
    display: flex;
    grid-column: 1 / -1;
    align-items: center;
    gap: 8px;
    padding: 8px 0 2px;
  }

  .system-rule::after {
    flex: 1;
    border-top: 1px dashed var(--card-border);
    content: "";
  }

  .key {
    overflow: hidden;
    color: var(--text-muted);
    font-size: 11.5px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .value {
    display: flex;
    align-items: center;
    min-width: 0;
    margin: 0;
    overflow: hidden;
    font-size: var(--text-sm);
    white-space: nowrap;
  }

  @media (hover: hover) and (pointer: fine) {
    .open:hover {
      background: var(--bg-active);
      color: var(--text);
    }

    .title:hover .id {
      text-decoration: underline;
      text-decoration-color: var(--card-border);
      text-underline-offset: 3px;
    }
  }
</style>
