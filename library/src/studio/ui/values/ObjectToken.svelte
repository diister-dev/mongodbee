<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import { plural } from "../lib/format.js";

  interface Props {
    value: Record<string, unknown>;
    onopen?: () => void;
  }

  let { value, onopen }: Props = $props();

  const keys = $derived(Object.keys(value));
  const label = $derived(keys.length === 0 ? "empty" : plural(keys.length, "key"));

  function open(event: MouseEvent) {
    event.stopPropagation();
    onopen?.();
  }
</script>

{#if onopen}
  <Tooltip text={keys.length ? keys.join(", ") : "Empty object"}>
    <button class="token mono" type="button" aria-label="Open {label}" onclick={open}>
      <span class="brace">{"{"}</span>{label}<span class="brace">{"}"}</span>
    </button>
  </Tooltip>
{:else}
  <Tooltip text={keys.length ? keys.join(", ") : "Empty object"}>
    <span class="token mono">
      <span class="brace">{"{"}</span>{label}<span class="brace">{"}"}</span>
    </span>
  </Tooltip>
{/if}

<style>
  .token {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    height: 20px;
    padding: 0 6px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    background: var(--frame);
    color: var(--text-muted);
    font-size: 11px;
    vertical-align: middle;
    white-space: nowrap;
    transition:
      border-color var(--dur-quick) var(--ease),
      color var(--dur-quick) var(--ease),
      transform var(--dur-press) var(--ease-out);
  }

  button.token:active {
    transform: scale(0.97);
  }

  @media (hover: hover) and (pointer: fine) {
    button.token:hover {
      border-color: color-mix(in srgb, var(--accent) 40%, var(--card-border));
      color: var(--text);
    }
  }

  .brace {
    color: var(--text-faint);
  }
</style>
