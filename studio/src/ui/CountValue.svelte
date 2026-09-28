<script>
  import Tooltip from "./controls/Tooltip.svelte";
  import { plural } from "./lib/format.js";

  let { count, noun } = $props();
</script>

{#if count.timedOut}
  <Tooltip text="The bounded count hit its time limit"><span class="timed-out">count timed out</span></Tooltip>
{:else}
  <span class="num value">{count.capped ? "at least " : ""}{plural(count.value, noun)}</span>
{/if}

<style>
  .value {
    color: var(--text);
  }

  .timed-out {
    color: var(--warning);
  }
</style>
