<script>
  import Tooltip from "../controls/Tooltip.svelte";
  import { formatCount } from "../lib/values.ts";

  let { months, scale = 1, unit = "document" } = $props();

  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

  const largest = $derived(Math.max(1, ...months.map((month) => month.count)));
  const step = $derived(Math.max(1, Math.ceil(months.length / 12)));

  function parts(key) {
    const [year, month] = key.split("-").map(Number);
    return { year, month: MONTHS[month - 1], january: month === 1 };
  }

  function estimate(count) {
    const value = Math.round(count * scale);
    return `${scale > 1 ? "about " : ""}${formatCount(value)} ${unit}${value === 1 ? "" : "s"}`;
  }
</script>

<div class="columns" style="--count: {months.length}">
  {#each months as month, index (month.month)}
    {@const tick = parts(month.month)}
    {@const shown = index % step === 0 || index === months.length - 1}
    <Tooltip text="{tick.month} {tick.year}: {estimate(month.count)}">
      <span class="column" class:empty={month.count === 0}>
        <span class="track"><span class="fill" style="--ratio: {month.count / largest}"></span></span>
        <span class="tick mono" class:shown>{tick.month}</span>
        <span class="year mono" class:shown={index === 0 || (tick.january && shown)}>{tick.year}</span>
      </span>
    </Tooltip>
  {/each}
</div>

<style>
  .columns {
    display: grid;
    grid-template-columns: repeat(var(--count), minmax(0, 72px));
    gap: 3px;
    align-items: end;
  }

  .columns :global(.tip-trigger) {
    display: flex;
    min-width: 0;
  }

  .column {
    display: flex;
    flex-direction: column;
    gap: 2px;
    width: 100%;
    min-width: 0;
  }

  .track {
    display: flex;
    align-items: flex-end;
    height: 110px;
    margin-bottom: 4px;
    background: color-mix(in srgb, var(--card-border) 30%, transparent);
  }

  .fill {
    width: 100%;
    height: calc(var(--ratio) * 100%);
    min-height: 1px;
    background: var(--text);
  }

  .empty .fill {
    min-height: 0;
  }

  .tick,
  .year {
    color: var(--text-faint);
    font-size: 10px;
    line-height: 12px;
    white-space: nowrap;
    visibility: hidden;
  }

  .year {
    color: var(--text-muted);
  }

  .tick.shown,
  .year.shown {
    visibility: visible;
  }
</style>
