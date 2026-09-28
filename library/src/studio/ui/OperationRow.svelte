<script>
  import { plural } from "./lib/format.js";
  import { ChevronRight } from "./lib/icons.js";
  import { expand } from "./lib/motion.ts";
  import Icon from "./Icon.svelte";
  import Scope from "./values/Scope.svelte";
  import TypeTag from "./values/TypeTag.svelte";

  let { operation, nested = false } = $props();

  let open = $state(false);

  const target = $derived.by(() => {
    const value = operation.target;
    if (!value || value.includes("→")) return { collection: value ?? "" };
    const index = value.lastIndexOf(".");
    if (index <= 0) return { collection: value };
    return { collection: value.slice(0, index), type: value.slice(index + 1) };
  });

  const details = $derived(Object.entries(operation.details ?? {}));
  const expandable = $derived(details.length > 0);

  function text(value) {
    if (Array.isArray(value)) return value.join(", ");
    if (typeof value === "string") return value;
    return JSON.stringify(value);
  }

  function label(key) {
    return key.replace(/([A-Z])/g, " $1").toLowerCase();
  }
</script>

<li class="op" class:nested>
  <svelte:element
    this={expandable ? "button" : "div"}
    class="line"
    class:clickable={expandable}
    onclick={expandable ? () => (open = !open) : undefined}
    aria-expanded={expandable ? open : undefined}
    role={expandable ? undefined : "group"}
  >
    <span class="caret" class:open class:hidden={!expandable}><Icon icon={ChevronRight} size={12} /></span>
    <span class="label">{operation.label}</span>
    <span class="target">
      <span class="mono">{target.collection}</span>
      {#if target.type}<TypeTag name={target.type} />{/if}
    </span>
    <span class="facts">
      {#if operation.scope}<Scope value={operation.scope} interactive={false} />{/if}
      {#if operation.documentCount !== undefined}
        <span class="muted num">{plural(operation.documentCount, "document")}</span>
      {/if}
      {#each operation.flags ?? [] as flag}
        <span class="tag" class:danger={flag === "irreversible"} class:warning={flag === "lossy"}>{flag}</span>
      {/each}
    </span>
  </svelte:element>
  {#if open}
    <div class="details" in:expand out:expand={{ exit: true }}>
      <dl>
        {#each details as [key, value] (key)}
          <div class="pair">
            <dt>{label(key)}</dt>
            <dd class="mono">{text(value)}</dd>
          </div>
        {/each}
      </dl>
    </div>
  {/if}
</li>

<style>
  .op {
    border-bottom: 1px solid var(--hairline);
  }

  .op:last-child {
    border-bottom: 0;
  }

  .line {
    display: grid;
    grid-template-columns: 14px minmax(170px, 220px) minmax(160px, 240px) minmax(0, 1fr);
    align-items: center;
    gap: 12px;
    width: 100%;
    min-height: 36px;
    padding: 4px 0;
    border: 0;
    background: none;
    color: inherit;
    font: inherit;
    text-align: left;
  }

  .nested .line {
    grid-template-columns: 14px minmax(156px, 206px) minmax(160px, 240px) minmax(0, 1fr);
  }

  .clickable {
    cursor: pointer;
  }

  .caret {
    display: inline-flex;
    color: var(--text-faint);
    transition: transform var(--dur-move) var(--ease-enter);
  }

  .caret.open {
    transform: rotate(90deg);
  }

  .caret.hidden {
    visibility: hidden;
  }

  .target {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    overflow: hidden;
    white-space: nowrap;
  }

  .facts {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px 12px;
    font-size: var(--text-xs);
  }

  .details {
    padding: 0 0 10px 26px;
  }

  dl {
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin: 0;
    padding: 8px 12px;
    border: 1px solid var(--hairline);
    border-radius: 0;
    background: var(--frame);
  }

  .pair {
    display: grid;
    grid-template-columns: 120px minmax(0, 1fr);
    gap: 12px;
    font-size: var(--text-xs);
  }

  dt {
    color: var(--text-faint);
  }

  dd {
    margin: 0;
    overflow-wrap: anywhere;
  }
</style>
