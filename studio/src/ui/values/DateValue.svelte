<script lang="ts">
  import HoverCard from "./HoverCard.svelte";
  import CardRow from "./CardRow.svelte";
  import Meta from "./Meta.svelte";
  import { altHeld } from "../lib/modifiers.js";
  import { Calendar, Globe, Hourglass } from "../lib/icons.js";
  import { formatCompactDate, formatFullDate, relativeTime } from "../lib/values.ts";

  interface Props {
    value: unknown;
    reveal?: boolean;
  }

  let { value, reveal = false }: Props = $props();

  const time = $derived.by((): number | null => {
    let raw: unknown = value;
    if (raw && typeof raw === "object" && "$date" in raw) raw = (raw as { $date: unknown }).$date;
    if (raw && typeof raw === "object" && "$numberLong" in raw) raw = Number((raw as { $numberLong: string }).$numberLong);
    const date = raw instanceof Date ? raw : new Date(raw as string | number);
    const ms = date.getTime();
    return Number.isNaN(ms) ? null : ms;
  });

  const future = $derived(time !== null && time > Date.now());
</script>

{#if time === null}
  <span class="date invalid">{String(value)}</span>
{:else}
  <HoverCard label={formatFullDate(time)}>
    <span class="date num">
      {formatCompactDate(time)}
      {#if future}<span class="ahead">{relativeTime(time)}</span>{/if}
    </span>
    {#if reveal && $altHeld && !future}<Meta text={relativeTime(time)} />{/if}
    {#snippet card()}
      <div class="vc-body">
        <CardRow icon={Calendar} label="UTC">{formatFullDate(time)}</CardRow>
        <CardRow icon={Globe} label="Local">{new Date(time).toLocaleString()}</CardRow>
        <div class="vc-sep"></div>
        <CardRow icon={Hourglass} label={future ? "Ahead" : "Ago"}>{relativeTime(time)}</CardRow>
      </div>
    {/snippet}
  </HoverCard>
{/if}

<style>
  .date {
    overflow: hidden;
    color: var(--text);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .ahead {
    margin-left: 6px;
    color: var(--accent);
    font-size: var(--text-xs);
  }

  .invalid {
    color: var(--danger);
  }
</style>
