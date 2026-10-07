<script>
  import CopyButton from "./values/CopyButton.svelte";

  let { command } = $props();
</script>

<span class="command">
  <span class="prompt">$</span>
  <code class="mono">{command}</code>
  <CopyButton text={command} label="Copy command" compact />
</span>

<style>
  .command {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    height: 28px;
    padding: 0 4px 0 10px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    background: var(--card);
    white-space: nowrap;
  }

  .prompt {
    color: var(--text-faint);
    font-family: var(--font-mono);
    font-size: var(--text-xs);
  }

  code {
    color: var(--text);
  }
</style>
