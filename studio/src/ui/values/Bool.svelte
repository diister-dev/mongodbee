<script lang="ts">
  import Icon from "../Icon.svelte";
  import Tooltip from "../controls/Tooltip.svelte";
  import { Check, Minus } from "../lib/icons.js";

  interface Props {
    value: boolean;
  }

  let { value }: Props = $props();
</script>

<Tooltip text={value ? "true" : "false"}>
  <span class="bool" class:on={value} role="img" aria-label={value ? "true" : "false"}>
    <Icon icon={value ? Check : Minus} size={14} stroke={value ? 2 : 1.5} />
  </span>
</Tooltip>

<style>
  .bool {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    border-radius: 0;
    color: var(--text-faint);
    vertical-align: middle;
  }

  .bool.on {
    background: color-mix(in srgb, var(--success) 12%, transparent);
    color: var(--success);
  }
</style>
