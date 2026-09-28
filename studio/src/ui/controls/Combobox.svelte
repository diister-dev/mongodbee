<script>
  import { tick } from "svelte";
  import Icon from "../Icon.svelte";
  import { Check, ChevronDown } from "../lib/icons.js";
  import { placeUnder } from "../lib/anchor.ts";
  import { moveSelection, rankOptions } from "../lib/interaction.ts";

  let {
    value = $bindable(),
    options,
    label,
    placeholder = "",
    free = false,
    mono = false,
    width = "200px",
    empty = "Nothing matches",
    onchange,
    inputRef = $bindable(),
    keyshortcuts,
    onsearch,
    ontext,
    invalid = false,
    onkeydown,
    autoselect = true,
    loading = false,
    openOnFocus = true,
  } = $props();

  let open = $state(false);
  let text = $state("");
  let dirty = $state(false);
  let active = $state(null);
  let list = $state();
  let root = $state();
  let place = $state({ left: 0, top: 0, bottom: 0, above: false, minWidth: 220 });
  let instant = $state(false);
  let pointer = false;
  const id = `combo-${Math.random().toString(36).slice(2, 8)}`;

  function measure() {
    if (!inputRef) return;
    place = placeUnder(
      inputRef.getBoundingClientRect(),
      { width: window.innerWidth, height: window.innerHeight },
      290,
      { minWidth: 220 },
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

  const current = $derived(options.find((option) => option.value === value));
  const visible = $derived(rankOptions(options, dirty ? text : ""));
  const shown = $derived(open ? text : (current?.label ?? (free ? (value ?? "") : "")));
  const activeOption = $derived(active === null ? undefined : visible[active]);
  const idle = $derived(autoselect ? 0 : null);

  async function show({ select = true } = {}) {
    if (open) return;
    instant = !pointer;
    pointer = false;
    measure();
    text = current?.label ?? (free ? (value ?? "") : "");
    dirty = false;
    onsearch?.("");
    const index = visible.findIndex((option) => option.value === value);
    active = index >= 0 ? index : idle;
    open = true;
    await tick();
    if (select) inputRef?.select();
    reveal();
  }

  function close() {
    open = false;
    dirty = false;
  }

  function commit(next) {
    value = next;
    onchange?.(next);
    close();
  }

  async function reveal() {
    await tick();
    list?.querySelector(`[data-index="${active}"]`)?.scrollIntoView({ block: "nearest" });
  }

  function onInput(event) {
    const typed = event.currentTarget.value;
    if (!open) show({ select: false });
    text = typed;
    dirty = true;
    active = idle;
    onsearch?.(typed);
    ontext?.(typed);
  }

  function onKey(event) {
    if (event.key === " " && (event.ctrlKey || event.metaKey)) {
      event.preventDefault();
      if (open) close();
      else show();
      return;
    }
    if (event.key === "Escape") {
      if (!open) {
        onkeydown?.(event, value ?? "");
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      close();
      return;
    }
    const action =
      event.key === "ArrowDown"
        ? { type: "next" }
        : event.key === "ArrowUp"
          ? { type: "previous" }
          : open && event.key === "Home" && !dirty
            ? { type: "first" }
            : open && event.key === "End" && !dirty
              ? { type: "last" }
              : null;
    if (action) {
      event.preventDefault();
      if (!open) {
        show();
        return;
      }
      active = moveSelection(active, visible.length, action) ?? 0;
      reveal();
      return;
    }
    if (event.key === "Enter") {
      if (!open) {
        onkeydown?.(event, value ?? "");
        return;
      }
      event.preventDefault();
      if (free && dirty && text.trim() === "" && !activeOption) commit("");
      else if (activeOption) commit(activeOption.value);
      else if (free) commit(text.trim());
      onkeydown?.(event, value ?? "");
      return;
    }
    if (event.key === "Tab" && open) close();
    onkeydown?.(event, open ? text : (value ?? ""));
  }

  function onBlur(event) {
    if (root?.contains(event.relatedTarget)) return;
    if (!open) return;
    if (free && dirty && text.trim() !== (value ?? "")) {
      commit(text.trim());
      return;
    }
    close();
  }

  function onOutside(event) {
    if (!open || root?.contains(event.target)) return;
    close();
  }
</script>

<svelte:window onpointerdown={onOutside} />

<span class="combobox" bind:this={root} style="--combo-width: {width}">
  <input
    bind:this={inputRef}
    class="control input"
    class:invalid
    class:mono
    role="combobox"
    aria-expanded={open}
    aria-controls={id}
    aria-autocomplete="list"
    aria-activedescendant={open && activeOption ? `${id}-${active}` : undefined}
    aria-label={label}
    aria-keyshortcuts={keyshortcuts}
    {placeholder}
    value={shown}
    spellcheck="false"
    autocomplete="off"
    onpointerdown={() => (pointer = true)}
    onfocus={() => { if (openOnFocus) show(); }}
    onclick={show}
    oninput={onInput}
    onkeydown={onKey}
    onblur={onBlur}
  />
  <span class="chevron" class:open aria-hidden="true"><Icon icon={ChevronDown} size={14} /></span>
  {#if open}
    <div bind:this={list} {id} class="popover-surface popover"
      class:above={place.above}
      class:instant
      style="left: {place.left}px; {place.above ? `bottom: ${place.bottom}px` : `top: ${place.top}px`}; min-width: {place.minWidth}px"
      role="listbox" aria-label={label}>
      <div class="scroller">
        {#each visible as option, index (option.value)}
          {#if option.group && (index === 0 || visible[index - 1].group !== option.group)}
            <div class="group-label" role="presentation">{option.group}</div>
          {/if}
          <div
            id="{id}-{index}"
            class="option"
            class:active={index === active}
            role="option"
            tabindex="-1"
            aria-selected={option.value === value}
            data-index={index}
            onpointermove={() => (active = index)}
            onpointerdown={(event) => event.preventDefault()}
            onclick={() => commit(option.value)}
            onkeydown={() => {}}
          >
            {#if option.icon}<span class="icon"><Icon icon={option.icon} size={13} /></span>{/if}
            <span class="label" class:mono>{option.label}</span>
            {#if option.hint}<span class="hint">{option.hint}</span>{/if}
            {#if option.value === value}<span class="tick"><Icon icon={Check} size={13} /></span>{/if}
          </div>
        {:else}
          <div class="empty">
            {#if loading}
              <span class="shimmer">Looking for values</span>
            {:else if free && text.trim()}
              <span>Press <kbd class="kbd">↵</kbd> to use <span class="mono">{text.trim()}</span></span>
            {:else}
              {empty}
            {/if}
          </div>
        {/each}
      </div>
    </div>
  {/if}
</span>

<style>
  .combobox {
    position: relative;
    display: inline-flex;
    width: var(--combo-width);
  }

  .input {
    width: 100%;
    padding-right: 28px;
  }

  .input.invalid {
    border-color: var(--danger);
  }

  .input.mono {
    font-family: var(--font-mono);
    font-size: 12.5px;
  }

  .chevron {
    position: absolute;
    top: 50%;
    right: 8px;
    display: inline-flex;
    color: var(--text-faint);
    pointer-events: none;
    translate: 0 -50%;
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
    max-height: 280px;
    padding: 4px;
    overflow-y: auto;
    overscroll-behavior: contain;
  }

  .group-label {
    padding: 8px 8px 4px;
    color: var(--text-faint);
    font-size: var(--text-xs);
  }

  .group-label:first-child {
    padding-top: 4px;
  }

  .option {
    display: flex;
    align-items: center;
    gap: 8px;
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

  .icon {
    display: inline-flex;
    color: var(--text-faint);
  }

  .option.active .icon {
    color: var(--accent);
  }

  .label.mono {
    font-family: var(--font-mono);
    font-size: 12.5px;
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

  .empty {
    padding: 10px 8px;
    color: var(--text-muted);
    font-size: var(--text-xs);
  }
</style>
