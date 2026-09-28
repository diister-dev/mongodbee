<script lang="ts">
  import HoverCard from "./HoverCard.svelte";
  import CardRow from "./CardRow.svelte";
  import CopyButton from "./CopyButton.svelte";
  import IdChunks from "./IdChunks.svelte";
  import Meta from "./Meta.svelte";
  import { altHeld } from "../lib/modifiers.js";
  import { Clock, Hash, Hourglass } from "../lib/icons.js";
  import { formatCompactDate, formatFullDate, objectIdTime, relativeTime } from "../lib/values.ts";

  interface Props {
    value: string;
    reveal?: boolean;
  }

  let { value, reveal = false }: Props = $props();

  const time = $derived(objectIdTime(value));
</script>

<HoverCard label="ObjectId {value}">
  <IdChunks {value} />
  {#if reveal && $altHeld && time !== null}<Meta text={formatCompactDate(time)} />{/if}
  {#snippet card()}
    <div class="vc-body">
      <div class="vc-title mono">{value}</div>
      <div class="vc-sep"></div>
      <CardRow icon={Hash} label="Format">ObjectId</CardRow>
      {#if time !== null}
        <CardRow icon={Clock} label="Created">{formatFullDate(time)}</CardRow>
        <CardRow icon={Hourglass} label="Age">{relativeTime(time)}</CardRow>
      {/if}
    </div>
    <div class="vc-foot">
      <span></span>
      <CopyButton text={value} />
    </div>
  {/snippet}
</HoverCard>
