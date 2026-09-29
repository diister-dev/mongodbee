<script>
  import Icon from "../Icon.svelte";
  import { Check, Minus } from "../lib/icons.js";

  let { value, label, onchange } = $props();
</script>

<button
  type="button"
  role="switch"
  class="toggle press"
  class:on={value === true}
  aria-checked={value === true}
  aria-label={label}
  onclick={() => onchange(value !== true)}
>
  <span class="box"><Icon icon={value === true ? Check : Minus} size={13} stroke={value === true ? 2 : 1.5} /></span>
  <span class="text mono">{value === true ? "true" : "false"}</span>
</button>

<style>
  .toggle {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    height: 26px;
    padding: 0 10px 0 4px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    background: var(--card);
    color: var(--text-muted);
    font-size: 12px;
  }

  .box {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    background: var(--frame);
    color: var(--text-faint);
  }

  .toggle.on {
    border-color: color-mix(in srgb, var(--success) 45%, var(--card-border));
    color: var(--text);
  }

  .toggle.on .box {
    background: color-mix(in srgb, var(--success) 14%, transparent);
    color: var(--success);
  }

  .toggle:focus-visible {
    outline: none;
    box-shadow: 0 0 0 3px var(--accent-ring);
  }

  @media (hover: hover) and (pointer: fine) {
    .toggle:hover {
      border-color: var(--text-faint);
    }
  }
</style>
