<script>
  let { label = "computed" } = $props();
</script>

<span class="system-tag">{label}</span>

<style>
  .system-tag {
    display: inline-flex;
    flex: none;
    align-items: center;
    height: 16px;
    padding: 0 4px;
    border: 1px dashed var(--card-border);
    color: var(--text-faint);
    font-family: var(--font-mono);
    font-size: 10px;
    font-weight: 400;
    line-height: 1;
    white-space: nowrap;
  }
</style>
