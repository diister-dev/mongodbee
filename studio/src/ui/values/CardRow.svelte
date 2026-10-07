<script lang="ts">
  import type { Snippet } from "svelte";
  import Icon from "../Icon.svelte";

  interface Props {
    icon: unknown;
    label: string;
    children: Snippet;
    title?: string;
  }

  let { icon, label, children, title }: Props = $props();
</script>

<div class="vc-row">
  <span class="vc-icon"><Icon {icon} size={13} /></span>
  <span class="vc-label">{label}</span>
  <span class="vc-value" {title}>{@render children()}</span>
</div>
