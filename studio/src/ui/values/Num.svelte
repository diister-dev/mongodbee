<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import { formatCount, formatNumberValue } from "../lib/values.ts";

  interface Props {
    value: number | string;
    count?: boolean;
  }

  let { value, count = false }: Props = $props();

  const text = $derived(
    typeof value === "number" ? (count ? formatCount(value) : formatNumberValue(value)) : value,
  );
</script>

<Tooltip text={String(value)} overflow={text === String(value)}><span class="num value">{text}</span></Tooltip>

<style>
  .value {
    display: inline-block;
    max-width: 100%;
    overflow: hidden;
    color: var(--text);
    font-variant-numeric: tabular-nums;
    text-overflow: ellipsis;
    white-space: nowrap;
    vertical-align: bottom;
  }
</style>
