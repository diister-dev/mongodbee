<script>
  let { tone = "neutral", label, hollow = false, stamped = false } = $props();
</script>

<span class="status">
  <span class="mark {tone}" class:hollow class:stamped></span>
  {#if label}<span class="label">{label}</span>{/if}
</span>

<style>
  .status {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: var(--text-muted);
    font-size: var(--text-sm);
    white-space: nowrap;
  }

  .mark {
    --tone: var(--text-faint);
    flex: none;
    width: 7px;
    height: 7px;
    background: var(--tone);
  }

  .mark.stamped {
    transition:
      transform 120ms var(--ease-out),
      opacity 120ms var(--ease-out);
  }

  @starting-style {
    .mark.stamped {
      opacity: 0;
      transform: scale(0.6);
    }
  }

  .mark.success {
    --tone: var(--success);
  }

  .mark.warning {
    --tone: var(--warning);
  }

  .mark.danger {
    --tone: var(--danger);
  }

  .mark.live {
    --tone: var(--accent);
  }

  .mark.hollow {
    background: var(--card);
    box-shadow: inset 0 0 0 1.5px var(--tone);
  }

  @media (prefers-reduced-motion: reduce) {
    @starting-style {
      .mark.stamped {
        transform: none;
      }
    }
  }
</style>
