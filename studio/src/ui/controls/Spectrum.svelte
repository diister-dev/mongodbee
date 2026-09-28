<script>
  let { size = 5, gap = 2, loading = false, vertical = false, label } = $props();
</script>

<span
  class="spectrum"
  class:loading
  class:vertical
  style="--cell: {size}px; --gap: {gap}px"
  role={label ? "img" : undefined}
  aria-label={label}
  aria-hidden={label ? undefined : "true"}
>
  <i></i><i></i><i></i><i></i><i></i>
</span>

<style>
  .spectrum {
    display: inline-grid;
    flex: none;
    grid-auto-flow: column;
    gap: var(--gap);
  }

  .spectrum.vertical {
    grid-auto-flow: row;
  }

  i {
    width: var(--cell);
    height: var(--cell);
    background: var(--pixel);
  }

  i:nth-child(1) {
    --pixel: var(--spectrum-1);
  }

  i:nth-child(2) {
    --pixel: var(--spectrum-2);
  }

  i:nth-child(3) {
    --pixel: var(--spectrum-3);
  }

  i:nth-child(4) {
    --pixel: var(--spectrum-4);
  }

  i:nth-child(5) {
    --pixel: var(--spectrum-5);
  }

  .loading i {
    background: var(--card-border);
    animation: spectrum-step 750ms steps(1, end) infinite;
  }

  .loading i:nth-child(2) {
    animation-delay: 150ms;
  }

  .loading i:nth-child(3) {
    animation-delay: 300ms;
  }

  .loading i:nth-child(4) {
    animation-delay: 450ms;
  }

  .loading i:nth-child(5) {
    animation-delay: 600ms;
  }

  @keyframes spectrum-step {
    0% {
      background: var(--pixel);
    }
    40% {
      background: var(--card-border);
    }
  }

  @media (prefers-reduced-motion: reduce) {
    .loading i {
      animation: none;
      background: var(--pixel);
    }
  }
</style>
