<script>
  import { tick } from "svelte";
  import { claimTip, releaseTip, tipWarmth } from "../lib/warm.js";

  let {
    text = "",
    kbd,
    overflow = false,
    block = false,
    disabled = false,
    side = "top",
    children,
    tip,
  } = $props();

  const OPEN_DELAY = 500;
  const id = `tip-${Math.random().toString(36).slice(2, 8)}`;

  let anchor = $state();
  let panel = $state();
  let open = $state(false);
  let instant = $state(false);
  let below = $state(false);
  let left = $state(0);
  let top = $state(0);
  let timer;

  function truncated() {
    if (!anchor) return false;
    const nodes = [anchor, ...anchor.querySelectorAll("*")];
    return nodes.some((node) => node.scrollWidth > node.clientWidth + 1);
  }

  async function place() {
    if (!anchor) return;
    const rect = anchor.getBoundingClientRect();
    if (side === "right") {
      left = rect.right + 8;
      top = rect.top + rect.height / 2;
      return;
    }
    below = rect.top < 44;
    left = rect.left + rect.width / 2;
    top = below ? rect.bottom + 6 : rect.top - 6;
    await tick();
    if (!panel) return;
    const width = panel.offsetWidth;
    left = Math.min(Math.max(left, 8 + width / 2), window.innerWidth - 8 - width / 2);
  }

  function closeNow() {
    clearTimeout(timer);
    if (!open) return;
    open = false;
    releaseTip(closeNow);
    tipWarmth.closed(performance.now());
  }

  function show() {
    if (disabled || (!text && !tip)) return;
    if (overflow && !truncated()) return;
    clearTimeout(timer);
    const warm = tipWarmth.isWarm(performance.now());
    timer = setTimeout(
      () => {
        instant = warm;
        claimTip(closeNow);
        tipWarmth.opened();
        open = true;
        place();
      },
      warm ? 0 : OPEN_DELAY,
    );
  }

  function onFocus(event) {
    if (event.target?.matches?.(":focus-visible")) show();
  }

  $effect(() => {
    if (!open) return;
    const close = () => closeNow();
    window.addEventListener("scroll", close, true);
    window.addEventListener("pointerdown", close, true);
    window.addEventListener("keydown", close, true);
    return () => {
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("pointerdown", close, true);
      window.removeEventListener("keydown", close, true);
    };
  });

  $effect(() => () => {
    clearTimeout(timer);
    if (open) {
      releaseTip(closeNow);
      tipWarmth.closed(performance.now());
    }
  });
</script>

<span
  class="tip-trigger"
  class:block
  bind:this={anchor}
  onmouseenter={show}
  onmouseleave={closeNow}
  onfocusin={onFocus}
  onfocusout={closeNow}
  role="presentation"
>
  {@render children()}
</span>
{#if open}
  <span
    bind:this={panel}
    {id}
    class="tip"
    class:instant
    class:below={below && side !== "right"}
    class:right={side === "right"}
    role="tooltip"
    style="left: {left}px; top: {top}px"
  >
    {#if tip}{@render tip()}{:else}<span class="tip-text">{text}</span>{/if}
    {#if kbd}<kbd class="kbd tip-kbd">{kbd}</kbd>{/if}
  </span>
{/if}

<style>
  .tip-trigger {
    display: inline-flex;
    align-items: center;
    min-width: 0;
    max-width: 100%;
    vertical-align: middle;
  }

  .tip-trigger.block {
    display: flex;
  }

  .tip {
    position: fixed;
    z-index: 50;
    display: inline-flex;
    align-items: center;
    gap: 8px;
    width: max-content;
    max-width: min(420px, calc(100vw - 16px));
    padding: 4px 8px;
    border-radius: var(--radius-control);
    background: var(--tooltip-bg);
    box-shadow: 0 8px 20px -10px rgb(20 26 21 / 0.45);
    color: var(--tooltip-text);
    font-family: var(--font-sans);
    font-size: var(--text-xs);
    font-weight: 400;
    line-height: 18px;
    white-space: pre-line;
    overflow-wrap: anywhere;
    pointer-events: none;
    translate: -50% -100%;
    transform-origin: bottom center;
    transition:
      opacity var(--dur-tip) var(--ease-out),
      transform var(--dur-tip) var(--ease-out);
  }

  @media (prefers-reduced-motion: reduce) {
    .tip {
      transition: opacity var(--dur-tip) var(--ease-out);
    }
  }

  .tip.below {
    translate: -50% 0;
    transform-origin: top center;
  }

  .tip.right {
    translate: 0 -50%;
    transform-origin: left center;
  }

  @starting-style {
    .tip:not(.instant) {
      opacity: 0;
      transform: scale(0.97);
    }
  }

  .tip.instant {
    transition: none;
  }

  .tip-kbd {
    border-color: rgb(255 255 255 / 0.18);
    background: rgb(255 255 255 / 0.08);
    color: var(--tooltip-text);
  }
</style>
