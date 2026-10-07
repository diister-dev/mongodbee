<script>
  let { name } = $props();
</script>

<span class="ref-prefix mono">{name}</span>

<style>
  .ref-prefix {
    flex: none;
    padding: 0 5px;
    border: 1px solid var(--card-border);
    background: var(--frame);
    color: var(--text-muted);
    font-size: 11px;
    line-height: 16px;
  }
</style>
