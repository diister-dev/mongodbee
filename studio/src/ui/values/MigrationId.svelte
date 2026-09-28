<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import { splitMigrationId } from "../lib/values.ts";

  interface Props {
    value: string;
  }

  let { value }: Props = $props();

  const parts = $derived(splitMigrationId(value.replace(/\.ts$/, "")));
  const extension = $derived(value.endsWith(".ts") ? ".ts" : "");
</script>

<Tooltip text={value} overflow
  ><span class="migration mono"
    >{#if parts.date}<span class="date">{parts.date}</span><span class="sep">_</span>{/if}<span class="name"
      >{parts.name}</span
    >{#if extension}<span class="ext">{extension}</span>{/if}</span
  ></Tooltip
>

<style>
  .migration {
    display: inline-block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    vertical-align: bottom;
  }

  .date,
  .sep,
  .ext {
    color: var(--text-faint);
  }

  .name {
    color: var(--text);
  }
</style>
