<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import { labels, requestLabel } from "../lib/labels.js";
  import { altHeld } from "../lib/modifiers.js";
  import { useStudio } from "../lib/studio.js";
  import { parseTypedId } from "../lib/values.ts";

  interface Props {
    value: unknown;
    interactive?: boolean;
  }

  let { value, interactive = true }: Props = $props();

  const studio = useStudio();
  const text = $derived(typeof value === "string" ? value : JSON.stringify(value));
  const clickable = $derived(interactive && typeof studio.openScope === "function");
  const typed = $derived(typeof value === "string" && parseTypedId(value) !== null);
  const named = $derived(typed ? ($labels[text] ?? null) : null);
  const shown = $derived(named && !$altHeld ? named.label : text);

  $effect(() => {
    if (typed) requestLabel(text);
  });

  function filter(event: MouseEvent) {
    event.stopPropagation();
    studio.openScope?.(text);
  }
</script>

{#if clickable}
  <Tooltip text={named ? `${text}, click to show only this scope` : "Show only this scope"}>
    <button class="scope" type="button" aria-label="Show only scope {named?.label ?? text}" onclick={filter}>
      <span class="ring"></span>
      <span class="text" class:mono={!named || $altHeld}>{shown}</span>
    </button>
  </Tooltip>
{:else}
  <Tooltip {text} overflow={!named}>
    <span class="scope">
      <span class="ring"></span>
      <span class="text" class:mono={!named || $altHeld}>{shown}</span>
    </span>
  </Tooltip>
{/if}

<style>
  .scope {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    max-width: 100%;
    height: 22px;
    padding: 0 10px 0 7px;
    border: 1px dashed var(--card-border);
    border-radius: var(--radius-control);
    background: var(--frame);
    color: var(--text);
    font: inherit;
    vertical-align: middle;
    transition:
      border-color var(--dur-quick) var(--ease),
      background-color var(--dur-quick) var(--ease),
      transform var(--dur-press) var(--ease-out);
  }

  button.scope:active {
    transform: scale(0.97);
  }

  @media (hover: hover) and (pointer: fine) {
    button.scope:hover {
      border-color: var(--text);
      background: var(--card);
    }

    button.scope:hover .ring {
      border-color: var(--text);
    }
  }

  .ring {
    flex: none;
    width: 8px;
    height: 8px;
    border: 1.5px solid var(--text-faint);
    border-radius: 0;
    rotate: 45deg;
    scale: 0.8;
  }

  .text {
    overflow: hidden;
    font-size: 11.5px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
