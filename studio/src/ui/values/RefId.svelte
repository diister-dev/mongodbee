<script lang="ts">
  import HoverCard from "./HoverCard.svelte";
  import CardRow from "./CardRow.svelte";
  import CopyButton from "./CopyButton.svelte";
  import IdChunks from "./IdChunks.svelte";
  import Meta from "./Meta.svelte";
  import TypeTag from "./TypeTag.svelte";
  import Icon from "../Icon.svelte";
  import { altHeld } from "../lib/modifiers.js";
  import { ArrowUpRight, Clock, Hourglass, Layers, Tag, Type } from "../lib/icons.js";
  import { labels, requestLabel } from "../lib/labels.js";
  import { useStudio } from "../lib/studio.js";
  import {
    formatCompactDate,
    formatFullDate,
    objectIdTime,
    parseTypedId,
    relativeTime,
    ulidTime,
  } from "../lib/values.ts";

  interface Props {
    value: string;
    target?: string;
    self?: boolean;
    reveal?: boolean;
    bare?: boolean;
  }

  let { value, target, self = false, reveal = false, bare = false }: Props = $props();

  const studio = useStudio();
  const parsed = $derived(parseTypedId(value));
  const prefix = $derived(parsed?.prefix ?? target ?? "");
  const id = $derived(parsed?.id ?? value);
  const time = $derived(ulidTime(id) ?? objectIdTime(id));
  const collection = $derived(self ? undefined : studio.resolveType(prefix));
  const labelled = $derived(Boolean(collection) && typeof value === "string" && /^[A-Za-z][A-Za-z0-9_.-]*:[A-Za-z0-9][A-Za-z0-9_-]*$/.test(value));
  const named = $derived(labelled ? ($labels[value] ?? null) : null);
  const showName = $derived(Boolean(named) && !$altHeld);

  $effect(() => {
    if (labelled) requestLabel(value);
  });

  function open(event: MouseEvent) {
    event.stopPropagation();
    if (collection) studio.openDocument(collection, JSON.stringify(value), prefix);
  }
</script>

<HoverCard label="{prefix} reference {value}">
  {#if collection}
    <button class="ref linked" type="button" onclick={open}>
      {#if !bare}<span class="prefix mono">{prefix}</span>{/if}
      {#if showName}
        <span class="name">{named?.label}</span>
      {:else}
        <IdChunks value={id} />
      {/if}
      <span class="go"><Icon icon={ArrowUpRight} size={12} /></span>
    </button>
  {:else}
    <span class="ref">
      {#if prefix && !bare}<span class="prefix mono">{prefix}</span>{/if}
      <IdChunks value={id} />
    </span>
  {/if}
  {#if reveal && $altHeld && time !== null}<Meta text={formatCompactDate(time)} />{/if}
  {#snippet card()}
    <div class="vc-body">
      <div class="vc-title mono">{value}</div>
      <div class="vc-sep"></div>
      {#if named}
        <CardRow icon={Type} label="Name"><span>{named.label}</span> <span class="faint mono">{named.field}</span></CardRow>
      {/if}
      {#if prefix}
        <CardRow icon={Tag} label="Type"><TypeTag name={prefix} /></CardRow>
      {/if}
      <CardRow icon={Layers} label="Collection">
        {#if collection}<span class="mono">{collection}</span>{:else}<span class="faint">{self ? "This document" : "Not browsable"}</span>{/if}
      </CardRow>
      {#if time !== null}
        <div class="vc-sep"></div>
        <CardRow icon={Clock} label="Created">{formatFullDate(time)}</CardRow>
        <CardRow icon={Hourglass} label="Age">{relativeTime(time)}</CardRow>
      {/if}
    </div>
    <div class="vc-foot">
      {#if collection}
        <button class="vc-action" type="button" onclick={open}>Open document</button>
      {:else}
        <span></span>
      {/if}
      <CopyButton text={value} />
    </div>
  {/snippet}
</HoverCard>

<style>
  .ref {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    max-width: 100%;
    padding: 0;
    border: 0;
    background: none;
    color: inherit;
    font: inherit;
    text-align: left;
  }

  .prefix {
    flex: none;
    padding: 0 5px;
    border: 1px solid var(--card-border);
    border-radius: 0;
    background: var(--frame);
    color: var(--text-muted);
    font-size: 11px;
    line-height: 16px;
  }

  .name {
    min-width: 0;
    overflow: hidden;
    color: var(--text);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .go {
    display: inline-flex;
    flex: none;
    color: var(--text-faint);
    transition: color var(--dur-quick) var(--ease);
  }

  @media (hover: hover) and (pointer: fine) {
    .linked:hover .go {
      color: var(--accent);
    }

    .linked:hover :global(.chunks) {
      text-decoration: underline;
      text-decoration-color: var(--card-border);
      text-underline-offset: 3px;
    }
  }
</style>
