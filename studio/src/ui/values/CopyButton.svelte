<script lang="ts">
  import Icon from "../Icon.svelte";
  import Tooltip from "../controls/Tooltip.svelte";
  import { Check, Copy } from "../lib/icons.js";

  interface Props {
    text: string;
    label?: string;
    compact?: boolean;
  }

  let { text, label = "Copy", compact = false }: Props = $props();

  let copied = $state(false);

  async function copy(event: MouseEvent) {
    event.stopPropagation();
    try {
      await navigator.clipboard.writeText(text);
      copied = true;
      setTimeout(() => (copied = false), 1200);
    } catch {
      copied = false;
    }
  }
</script>

{#if compact}
  <Tooltip text={copied ? "Copied" : label}>
    <button class="icon-only" type="button" aria-label={label} onclick={copy}>
      <Icon icon={copied ? Check : Copy} size={12} />
    </button>
  </Tooltip>
{:else}
  <button class="vc-action" type="button" onclick={copy}>
    <Icon icon={copied ? Check : Copy} size={12} />
    {copied ? "Copied" : label}
  </button>
{/if}

<style>
  .icon-only {
    display: inline-flex;
    flex: none;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    border: 0;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-faint);
    transition:
      background-color var(--dur-quick) var(--ease),
      color var(--dur-quick) var(--ease),
      transform var(--dur-press) var(--ease-out);
  }

  .icon-only:active {
    transform: scale(0.97);
  }

  @media (hover: hover) and (pointer: fine) {
    .icon-only:hover {
      background: var(--bg-active);
      color: var(--text);
    }
  }
</style>
