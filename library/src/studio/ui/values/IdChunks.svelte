<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import { isObjectIdHex, isUlid, splitObjectId, splitUlid } from "../lib/values.ts";

  interface Props {
    value: string;
  }

  let { value }: Props = $props();

  const chunks = $derived.by((): { lead: string; tail: string[] } => {
    if (isUlid(value)) {
      const [time, random] = splitUlid(value);
      return { lead: time, tail: [random] };
    }
    if (isObjectIdHex(value)) {
      const [time, machine, counter] = splitObjectId(value);
      return { lead: time, tail: [machine, counter] };
    }
    return { lead: value, tail: [] };
  });
</script>

<Tooltip text={value} overflow
  ><span class="chunks mono"
    ><span class="lead">{chunks.lead}</span>{#each chunks.tail as part}<span class="tail">{part}</span>{/each}</span
  ></Tooltip
>

<style>
  .chunks {
    display: inline-block;
    min-width: 0;
    overflow: hidden;
    color: var(--text);
    text-overflow: ellipsis;
    white-space: nowrap;
    vertical-align: bottom;
  }

  .tail {
    margin-left: 0.25ch;
    color: var(--text-muted);
  }
</style>
