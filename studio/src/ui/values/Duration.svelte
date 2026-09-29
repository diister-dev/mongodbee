<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import { durationParts, formatCount } from "../lib/values.ts";

  interface Props {
    ms: number;
  }

  let { ms }: Props = $props();

  const parts = $derived(durationParts(ms));
  const exact = $derived(`${formatCount(Math.round(ms))} ms`);
</script>

<Tooltip text={exact} disabled={ms < 1000}>
  <span class="duration num" aria-label={ms < 1000 ? undefined : exact}>
    {#each parts as [value, unit], index (unit)}{#if index > 0}&nbsp;{/if}<span class="value">{value}</span><span class="unit">&thinsp;{unit}</span>{/each}
  </span>
</Tooltip>

<style>
  .duration {
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  .value {
    color: var(--text);
  }

  .unit {
    color: var(--text-faint);
  }
</style>
