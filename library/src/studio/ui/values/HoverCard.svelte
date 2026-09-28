<script lang="ts">
  import type { Snippet } from "svelte";
  import { claimCard, hoverWarmth, releaseCard } from "../lib/warm.js";
  import { rise } from "../lib/motion.ts";

  interface Props {
    children: Snippet;
    card: Snippet;
    label?: string;
    block?: boolean;
  }

  let { children, card, label, block = false }: Props = $props();

  const OPEN_DELAY = 300;
  const CLOSE_DELAY = 120;
  const WIDTH = 300;

  let anchor: HTMLElement | undefined = $state();
  let panel: HTMLElement | undefined = $state();
  let open = $state(false);
  let left = $state(0);
  let top = $state(0);
  let from: "above" | "below" | "left" = $state("above");
  let anchorBottom = $state(false);
  let instant = $state(false);
  let timer: ReturnType<typeof setTimeout> | undefined;

  function place() {
    if (!anchor) return;
    const rect = anchor.getBoundingClientRect();
    const roomRight = window.innerWidth - rect.right - 12;
    if (roomRight >= WIDTH) {
      from = "left";
      left = rect.right + 10;
      const fitsBelow = rect.top - 6 + 200 < window.innerHeight;
      anchorBottom = !fitsBelow;
      top = fitsBelow ? rect.top - 6 : window.innerHeight - rect.bottom - 6;
      return;
    }
    left = Math.max(8, Math.min(rect.left - 6, window.innerWidth - WIDTH - 8));
    const below = rect.bottom + 6 + 180 < window.innerHeight;
    from = below ? "above" : "below";
    anchorBottom = !below;
    top = below ? rect.bottom + 6 : window.innerHeight - rect.top + 6;
  }

  function closeNow() {
    clearTimeout(timer);
    instant = true;
    setOpen(false);
  }

  function setOpen(next: boolean) {
    if (next === open) return;
    open = next;
    if (next) {
      claimCard(closeNow);
      hoverWarmth.opened();
    } else {
      releaseCard(closeNow);
      hoverWarmth.closed(performance.now());
    }
  }

  function show() {
    clearTimeout(timer);
    const warm = hoverWarmth.isWarm(performance.now());
    timer = setTimeout(
      () => {
        instant = warm;
        place();
        setOpen(true);
      },
      warm ? 0 : OPEN_DELAY,
    );
  }

  function hide() {
    clearTimeout(timer);
    timer = setTimeout(() => setOpen(false), CLOSE_DELAY);
  }

  function keep() {
    clearTimeout(timer);
  }

  $effect(() => {
    if (!open) return;
    const close = (event: Event) => {
      if (event.type === "scroll" && panel && panel.contains(event.target as Node)) return;
      clearTimeout(timer);
      setOpen(false);
    };
    window.addEventListener("scroll", close, true);
    window.addEventListener("hashchange", close);
    return () => {
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("hashchange", close);
    };
  });

  $effect(() => () => {
    clearTimeout(timer);
    if (open) {
      releaseCard(closeNow);
      hoverWarmth.closed(performance.now());
    }
  });
</script>

<span
  class="trigger"
  class:block
  bind:this={anchor}
  aria-label={label}
  onmouseenter={show}
  onmouseleave={hide}
  onfocusin={show}
  onfocusout={hide}
  role="group"
>
  {@render children()}
</span>
{#if open}
  <div
    class="card"
    bind:this={panel}
    style="left: {left}px; {anchorBottom ? 'bottom' : 'top'}: {top}px; transform-origin: {anchorBottom
      ? 'bottom left'
      : 'top left'}"
    role="tooltip"
    onmouseenter={keep}
    onmouseleave={hide}
    in:rise={{ from, duration: instant ? 0 : 150 }}
    out:rise={{ from, exit: true, duration: instant ? 0 : 100 }}
  >
    {@render card()}
  </div>
{/if}

<style>
  .trigger {
    display: inline-flex;
    align-items: center;
    min-width: 0;
    max-width: 100%;
    vertical-align: middle;
  }

  .trigger.block {
    display: flex;
  }

  .card {
    position: fixed;
    z-index: 30;
    width: 300px;
    padding: 3px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-panel);
    background: var(--frame);
    box-shadow: var(--shadow-overlay);
    color: var(--text);
    font-family: var(--font-sans);
    font-size: var(--text-xs);
    font-weight: 400;
    line-height: 18px;
    white-space: normal;
    cursor: default;
  }
</style>
