<script lang="ts">
  interface Props {
    text: string;
  }

  let { text }: Props = $props();
</script>

<span class="meta num">{text}</span>

<style>
  .meta {
    position: absolute;
    top: 50%;
    right: 6px;
    z-index: 1;
    padding: 1px 6px 1px 18px;
    border-radius: 0;
    background: linear-gradient(to right, transparent, var(--row-bg, var(--card)) 14px);
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: 11px;
    line-height: 16px;
    white-space: nowrap;
    transform: translateY(-50%);
    pointer-events: none;
  }
</style>
