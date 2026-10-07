<script>
  import { tick } from "svelte";
  import Icon from "../Icon.svelte";
  import { Check, ChevronDown } from "../lib/icons.js";
  import { placeUnder } from "../lib/anchor.ts";
  import { moveSelection } from "../lib/interaction.ts";

  let { value = $bindable(), options, label, onchange, size = "md", disabled = false } = $props();

  let open = $state(false);
  let active = $state(0);
  let trigger = $state();
  let list = $state();
  let place = $state({ left: 0, top: 0, bottom: 0, above: false, minWidth: 160 });
  let instant = $state(false);
  const id = `select-${Math.random().toString(36).slice(2, 8)}`;

  const current = $derived(options.find((option) => option.value === value) ?? options[0]);

  async function show(viaKeyboard = false) {
    if (disabled || !trigger) return;
    instant = viaKeyboard;
    const index = options.findIndex((option) => option.value === value);
    active = index >= 0 ? index : 0;
    measure();
    open = true;
    await tick();
    list?.focus({ preventScroll: true });
  }

  function measure() {
    if (!trigger) return;
    place = placeUnder(
      trigger.getBoundingClientRect(),
      { width: window.innerWidth, height: window.innerHeight },
      Math.min(268, options.length * 30 + 10),
    );
  }

  $effect(() => {
    if (!open) return;
    const follow = (event) => {
      if (event.type === "scroll" && list?.contains(event.target)) return;
      measure();
    };
    window.addEventListener("scroll", follow, true);
    window.addEventListener("resize", follow);
    return () => {
      window.removeEventListener("scroll", follow, true);
      window.removeEventListener("resize", follow);
    };
  });

  function close(refocus = true) {
    open = false;
    if (refocus) trigger?.focus();
  }

  function choose(option) {
    value = option.value;
    onchange?.(option.value);
    close();
  }

  function onTriggerKey(event) {
    if (event.key === "ArrowDown" || event.key === "ArrowUp" || event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      show(true);
    }
  }

  function onListKey(event) {
    if (event.key === "Escape" || event.key === "Tab") {
      event.preventDefault();
      close();
      return;
    }
    const action =
      event.key === "ArrowDown"
        ? { type: "next" }
        : event.key === "ArrowUp"
          ? { type: "previous" }
          : event.key === "Home"
            ? { type: "first" }
            : event.key === "End"
              ? { type: "last" }
              : null;
    if (action) {
      event.preventDefault();
      active = moveSelection(active, options.length, action) ?? 0;
      return;
    }
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      choose(options[active]);
    }
  }

  function onOutside(event) {
    if (!open) return;
    if (trigger?.contains(event.target) || list?.contains(event.target)) return;
    close(false);
  }
</script>

<svelte:window onpointerdown={onOutside} />

<span class="select" class:small={size === "sm"}>
  <button
    bind:this={trigger}
    type="button"
    class="control trigger"
    aria-haspopup="listbox"
    aria-expanded={open}
    aria-controls={id}
    aria-label={label}
    {disabled}
    onclick={(event) => (open ? close() : show(event.detail === 0))}
    onkeydown={onTriggerKey}
  >
    <span class="current">{current?.label ?? ""}</span>
    <span class="chevron" class:open><Icon icon={ChevronDown} size={14} /></span>
  </button>
  {#if open}
    <div
      bind:this={list}
      {id}
      class="popover-surface popover"
      class:above={place.above}
      class:instant
      style="left: {place.left}px; {place.above ? `bottom: ${place.bottom}px` : `top: ${place.top}px`}; min-width: {place.minWidth}px"
      role="listbox"
      tabindex="-1"
      aria-label={label}
      aria-activedescendant="{id}-{active}"
      onkeydown={onListKey}
    >
      <div class="scroller">
        {#each options as option, index (option.value)}
          <div
            id="{id}-{index}"
            class="option"
            class:active={index === active}
            role="option"
            tabindex="-1"
            aria-selected={option.value === value}
            onpointermove={() => (active = index)}
            onclick={() => choose(option)}
            onkeydown={() => {}}
          >
            <span class="label">{option.label}</span>
            {#if option.hint}<span class="hint">{option.hint}</span>{/if}
            {#if option.value === value}<span class="tick"><Icon icon={Check} size={13} /></span>{/if}
          </div>
        {/each}
      </div>
    </div>
  {/if}
</span>

<style>
  .select {
    position: relative;
    display: inline-flex;
  }

  .trigger {
    justify-content: space-between;
    gap: 8px;
    min-width: 0;
    padding-right: 8px;
  }

  .small .trigger {
    height: 28px;
    font-size: var(--text-xs);
  }

  .current {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .chevron {
    display: inline-flex;
    color: var(--text-faint);
  }

  .popover {
    position: fixed;
    z-index: 45;
    transform-origin: top left;
  }

  .popover.above {
    transform-origin: bottom left;
  }

  .scroller {
    max-height: 258px;
    padding: 4px;
    overflow-y: auto;
    overscroll-behavior: contain;
  }

  .option {
    display: flex;
    align-items: center;
    gap: 10px;
    min-height: 30px;
    padding: 0 8px;
    border-radius: var(--radius-control);
    color: var(--text);
    font-size: var(--text-sm);
    cursor: pointer;
    white-space: nowrap;
  }

  .option.active {
    background: var(--select-bg);
  }

  .hint {
    color: var(--text-faint);
    font-size: var(--text-xs);
  }

  .tick {
    display: inline-flex;
    margin-left: auto;
    color: var(--accent);
  }
</style>
