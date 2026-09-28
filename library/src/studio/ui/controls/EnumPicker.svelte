<script>
  import Combobox from "./Combobox.svelte";
  import { hueForOption } from "../lib/values.ts";

  let { value, options, field = "", label, invalid = false, onchange } = $props();

  const INLINE_LIMIT = 6;

  const texts = $derived(options.map(String));
  const inline = $derived(options.length <= INLINE_LIMIT && texts.every((text) => text.length <= 24));

  function hueOf(option) {
    return hueForOption(String(option), texts, field).dot;
  }

  function move(event, index) {
    const last = options.length - 1;
    const next =
      event.key === "ArrowRight" || event.key === "ArrowDown"
        ? (index + 1) % options.length
        : event.key === "ArrowLeft" || event.key === "ArrowUp"
          ? (index + last) % options.length
          : null;
    if (next === null) return;
    event.preventDefault();
    onchange(options[next]);
    requestAnimationFrame(() => event.currentTarget?.parentElement?.children[next]?.focus());
  }
</script>

{#if inline}
  <span class="picker" class:invalid role="radiogroup" aria-label={label}>
    {#each options as option, index (String(option))}
      <button
        type="button"
        role="radio"
        class="choice press"
        class:on={option === value}
        aria-checked={option === value}
        tabindex={option === value || (value === undefined && index === 0) ? 0 : -1}
        style="--enum-dot: {hueOf(option)}"
        onclick={() => onchange(option)}
        onkeydown={(event) => move(event, index)}
      >
        <span class="dot"></span>
        <span class="text">{String(option)}</span>
      </button>
    {/each}
  </span>
{:else}
  <Combobox
    value={value === undefined || value === null ? "" : String(value)}
    options={options.map((option) => ({ value: String(option), label: String(option) }))}
    {label}
    width="100%"
    {invalid}
    empty="No choice matches"
    onchange={(next) => onchange(options.find((option) => String(option) === next) ?? next)}
  />
{/if}

<style>
  .picker {
    display: inline-flex;
    flex-wrap: wrap;
    gap: 4px;
  }

  .choice {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    height: 26px;
    padding: 0 9px 0 8px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    background: var(--card);
    color: var(--text-muted);
    font-size: 12.5px;
    transition:
      border-color var(--dur-quick) var(--ease),
      background-color var(--dur-quick) var(--ease);
  }

  .dot {
    flex: none;
    width: 7px;
    height: 7px;
    background: var(--enum-dot);
    opacity: 0.45;
  }

  .choice.on {
    border-color: var(--text);
    background: color-mix(in srgb, var(--enum-dot) 12%, var(--card));
    color: var(--text);
  }

  .choice.on .dot {
    opacity: 1;
  }

  .invalid .choice {
    border-color: var(--danger);
  }

  .choice:focus-visible {
    outline: none;
    box-shadow: 0 0 0 3px var(--accent-ring);
  }

  @media (hover: hover) and (pointer: fine) {
    .choice:not(.on):hover {
      border-color: var(--text-faint);
      color: var(--text);
    }
  }
</style>
