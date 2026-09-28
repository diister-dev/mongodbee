<script lang="ts">
  import HoverCard from "./HoverCard.svelte";
  import CardRow from "./CardRow.svelte";
  import Enum from "./Enum.svelte";
  import Value from "./Value.svelte";
  import { Braces, Split } from "../lib/icons.js";

  interface VariantNode {
    discriminator?: string;
    options?: Array<{ entries?: Record<string, { literal?: unknown }> }>;
  }

  interface Props {
    value: Record<string, unknown>;
    discriminator: string;
    node?: VariantNode;
    field?: string;
    onopen?: () => void;
  }

  let { value, discriminator, node, field = "", onopen }: Props = $props();

  const tag = $derived(String(value[discriminator]));
  const options = $derived(
    (node?.options ?? []).map((option) => option.entries?.[discriminator]?.literal).filter((v) => v !== undefined),
  );
  const rest = $derived(Object.entries(value).filter(([key]) => key !== discriminator));
</script>

<HoverCard label="{field} {tag}">
  <span class="variant">
    {#if onopen}
      <button class="open" type="button" onclick={(event) => { event.stopPropagation(); onopen?.(); }}>
        <Enum value={tag} options={options.length > 0 ? options : undefined} field="{field}.{discriminator}" />
      </button>
    {:else}
      <Enum value={tag} options={options.length > 0 ? options : undefined} field="{field}.{discriminator}" />
    {/if}
    {#if rest.length > 0}<span class="more num">+{rest.length}</span>{/if}
  </span>
  {#snippet card()}
    <div class="vc-body">
      <CardRow icon={Split} label={discriminator}>
        <Enum value={tag} options={options.length > 0 ? options : undefined} field="{field}.{discriminator}" />
      </CardRow>
      {#if rest.length > 0}
        <div class="vc-sep"></div>
        {#each rest as [key, inner] (key)}
          <CardRow icon={Braces} label={key}><Value value={inner} field={key} compact /></CardRow>
        {/each}
      {/if}
    </div>
  {/snippet}
</HoverCard>

<style>
  .variant {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
  }

  .open {
    display: inline-flex;
    min-width: 0;
    padding: 0;
    border: 0;
    background: none;
    color: inherit;
    font: inherit;
  }

  .more {
    color: var(--text-faint);
    font-size: 11px;
  }
</style>
