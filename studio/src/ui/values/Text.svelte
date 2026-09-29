<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";

  interface Props {
    value: string;
    quoted?: boolean;
  }

  let { value, quoted = false }: Props = $props();
</script>

{#if quoted}
  <span class="quoted mono">{JSON.stringify(value)}</span>
{:else if value === ""}
  <span class="empty mono" aria-label="Empty string">""</span>
{:else}
  <Tooltip text={value} overflow><span class="text">{value}</span></Tooltip>
{/if}

<style>
  .text {
    display: inline-block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    vertical-align: bottom;
  }

  .quoted {
    color: var(--syntax-string);
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }

  .empty {
    color: var(--text-faint);
  }
</style>
