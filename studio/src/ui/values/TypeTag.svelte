<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";
  import { hueFor } from "../lib/values.ts";

  interface Props {
    name: string;
    title?: string;
  }

  let { name, title }: Props = $props();

  const hue = $derived(hueFor(name));
</script>

<Tooltip text={title ?? name} overflow={!title}>
  <span class="type-tag mono" style="--tag-text: {hue.text}; --tag-tint: {hue.tint}; --tag-dot: {hue.dot}">
    <span class="dot"></span>
    <span class="name">{name}</span>
  </span>
</Tooltip>

<style>
  .type-tag {
    display: inline-flex;
    flex: none;
    align-items: center;
    gap: 5px;
    max-width: 100%;
    height: 20px;
    padding: 0 7px 0 6px;
    border: 1px solid color-mix(in srgb, var(--tag-dot) 45%, transparent);
    border-radius: var(--radius-control);
    background: var(--tag-tint);
    color: var(--tag-text);
    line-height: 18px;
    white-space: nowrap;
    vertical-align: middle;
  }

  .dot {
    flex: none;
    width: 6px;
    height: 6px;
    background: var(--tag-dot);
  }

  .name {
    overflow: hidden;
    text-overflow: ellipsis;
  }
</style>
