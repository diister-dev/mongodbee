<script>
  import Icon from "./Icon.svelte";

  let { icon, title, hint, children } = $props();
</script>

<div class="empty">
  {#if icon}<span class="glyph" aria-hidden="true"><Icon {icon} size={18} /></span>{/if}
  <p class="title">{title}</p>
  {#if hint}<p class="hint">{hint}</p>{/if}
  {#if children}<div class="actions">{@render children()}</div>{/if}
</div>

<style>
  .empty {
    display: flex;
    flex: 1;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 6px;
    padding: 48px 16px;
    text-align: center;
  }

  .glyph {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 36px;
    height: 36px;
    margin-bottom: 4px;
    border: 1px dashed var(--card-border);
    background: var(--frame);
    color: var(--text-faint);
  }

  p {
    max-width: 440px;
    margin: 0;
  }

  .title {
    color: var(--text);
    font-weight: 600;
  }

  .hint {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }

  .actions {
    display: flex;
    flex-wrap: wrap;
    justify-content: center;
    gap: 8px;
    margin-top: 8px;
  }
</style>
