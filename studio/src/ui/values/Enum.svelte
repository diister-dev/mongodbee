<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import { hueForOption } from "../lib/values.ts";

  interface Props {
    value: string | number | boolean;
    options?: readonly unknown[];
    field?: string;
  }

  let { value, options, field = "" }: Props = $props();

  const text = $derived(String(value));
  const hue = $derived(hueForOption(text, options?.map(String), field));
</script>

<Tooltip {text} overflow>
  <span class="enum" style="--enum-dot: {hue.dot}">
    <span class="dot"></span>
    <span class="label">{text}</span>
  </span>
</Tooltip>

<style>
  .enum {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    min-width: 0;
    max-width: 100%;
    vertical-align: middle;
  }

  .dot {
    flex: none;
    width: 7px;
    height: 7px;
    background: var(--enum-dot);
  }

  .label {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
