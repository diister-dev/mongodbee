<script>
  import Icon from "./Icon.svelte";
  import Status from "./Status.svelte";
  import CountValue from "./CountValue.svelte";
  import Scope from "./values/Scope.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import { plural } from "./lib/format.js";
  import { ChevronRight } from "./lib/icons.js";
  import { expand } from "./lib/motion.ts";

  let { operation, nested = false } = $props();

  let open = $state(false);

  const target = $derived.by(() => {
    const value = operation.target;
    if (!value || value.includes("→")) return { collection: value ?? "" };
    const index = value.lastIndexOf(".");
    if (index <= 0) return { collection: value };
    return { collection: value.slice(0, index), type: value.slice(index + 1) };
  });

  const impact = $derived(operation.impact ?? {});
  const details = $derived(Object.entries(operation.details ?? {}));
  const dangling = $derived(impact.dangling?.references ?? []);
  const expandable = $derived(
    details.length > 0 || dangling.length > 0 || operation.reasons.length > 0 || Boolean(impact.note),
  );

  function location(value) {
    const [, name] = value.split("/");
    return name ?? value;
  }

  function text(value) {
    if (Array.isArray(value)) return value.join(", ");
    if (typeof value === "string") return value;
    return JSON.stringify(value);
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
    <span class="impact">
      {#if impact.documents}
        <span class="effect"><CountValue count={impact.documents} noun="document" /> <span class="muted">{impact.verb}</span></span>
      {/if}
      {#if impact.exists === false && operation.type.startsWith("create_")}
        <span class="muted">new</span>
      {/if}
      {#if impact.scopes}
        {#if impact.scopes.all}
          <span class="muted">every scope{#if impact.scopes.present !== undefined}, {plural(impact.scopes.present, "scope")} today{/if}</span>
        {:else}
          {#each impact.scopes.values as scope}<Scope value={scope} interactive={false} />{/each}
        {/if}
      {/if}
      {#if dangling.length > 0}
        <Status tone="warning" label={plural(dangling.reduce((sum, d) => sum + d.count, 0), "dangling reference")} />
      {/if}
      {#if operation.blocking}
        <Status tone="danger" label="blocking" />
      {/if}
      {#each operation.flags ?? [] as flag}
        <span class="tag" class:danger={flag === "irreversible"} class:warning={flag === "lossy"}>{flag}</span>
      {/each}
    </span>
  </svelte:element>
  {#if open}
    <div class="details" in:expand out:expand={{ exit: true }}>
      <div class="panel">
        {#if operation.blocking}
          <p class="blocking">{operation.blocking}</p>
        {/if}
        {#each operation.reasons as reason}
          <p class="reason">
            <span class="tag" class:danger={reason.flag === "irreversible"} class:warning={reason.flag === "lossy"}>{reason.flag}</span>
            <span>{reason.reason}</span>
          </p>
        {/each}
        {#if dangling.length > 0}
          <div class="dangling">
            <span class="muted">
              Documents it removes are still referenced. Checked {plural(impact.dangling.sampledIds, "removed id")} against
              {plural(impact.dangling.scanned, "document")}.
            </span>
            {#each dangling as ref}
              <span class="site">
                <span class="mono">{location(ref.location)}</span>
                <span class="mono muted">{ref.path}</span>
                <span class="num">{plural(ref.count, "reference")}</span>
              </span>
            {/each}
          </div>
        {/if}
        {#if details.length > 0}
          <dl>
            {#each details as [key, value] (key)}
              <div class="pair">
                <dt>{key.replace(/([A-Z])/g, " $1").toLowerCase()}</dt>
                <dd class="mono">{text(value)}</dd>
              </div>
            {/each}
          </dl>
        {/if}
        {#if impact.note}<p class="muted note">{impact.note}</p>{/if}
      </div>
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
    grid-template-columns: 14px minmax(160px, 210px) minmax(150px, 230px) minmax(0, 1fr);
    align-items: center;
    gap: 12px;
    width: 100%;
    min-height: 38px;
    padding: 4px 0;
    border: 0;
    background: none;
    color: inherit;
    font: inherit;
    text-align: left;
  }

  .nested .line {
    grid-template-columns: 14px minmax(146px, 196px) minmax(150px, 230px) minmax(0, 1fr);
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

  .impact {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px 12px;
    font-size: var(--text-xs);
  }

  .impact :global(.status) {
    font-size: var(--text-xs);
  }

  .effect {
    white-space: nowrap;
  }

  .details {
    padding: 0 0 10px 26px;
  }

  .panel {
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 10px 12px;
    border: 1px solid var(--hairline);
    border-radius: 0;
    background: var(--frame);
    font-size: var(--text-xs);
  }

  .panel p {
    margin: 0;
  }

  .blocking {
    color: var(--danger);
  }

  .reason {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .dangling {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .site {
    display: flex;
    align-items: baseline;
    gap: 10px;
  }

  dl {
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin: 0;
  }

  .pair {
    display: grid;
    grid-template-columns: 110px minmax(0, 1fr);
    gap: 12px;
  }

  dt {
    color: var(--text-faint);
  }

  dd {
    margin: 0;
    overflow-wrap: anywhere;
  }
</style>
