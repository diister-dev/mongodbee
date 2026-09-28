<script lang="ts">
  import Icon from "../Icon.svelte";
  import Tooltip from "../controls/Tooltip.svelte";
  import CopyButton from "./CopyButton.svelte";
  import { Mail } from "../lib/icons.js";
  import { middleTruncate } from "../lib/values.ts";

  interface Props {
    value: string;
    max?: number;
  }

  let { value, max = 30 }: Props = $props();

  const shown = $derived(middleTruncate(value, max));
</script>

<span class="contact">
  <span class="glyph"><Icon icon={Mail} size={13} /></span>
  <Tooltip text={value} overflow={shown === value}><span class="text">{shown}</span></Tooltip>
  <span class="copy"><CopyButton text={value} label="Copy email" compact /></span>
</span>

<style>
  .contact {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    position: relative;
    min-width: 0;
    max-width: 100%;
    vertical-align: middle;
  }

  .glyph {
    display: inline-flex;
    flex: none;
    color: var(--text-faint);
  }

  .text {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .copy {
    position: absolute;
    top: 50%;
    right: 0;
    display: inline-flex;
    padding-left: 14px;
    background: linear-gradient(to right, transparent, var(--card) 45%);
    opacity: 0;
    transform: translateY(-50%);
    transition: opacity var(--dur-quick) var(--ease-enter);
  }

  .contact:focus-within .copy {
    opacity: 1;
  }

  @media (hover: hover) and (pointer: fine) {
    .contact:hover .copy {
      opacity: 1;
    }
  }
</style>
