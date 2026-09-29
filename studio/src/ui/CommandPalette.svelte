<script>
  import { onMount, tick } from "svelte";
  import Icon from "./Icon.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import {
    Box,
    CornerDownLeft,
    FileCode,
    GitBranch,
    GitCompare,
    Hash,
    History,
    LayoutGrid,
    Layers,
    List,
    Play,
    Search,
  } from "./lib/icons.js";
  import { requestCheck } from "./lib/check-store.js";
  import { defaultTab, kindLabel } from "./lib/format.js";
  import { matchScore, moveSelection } from "./lib/interaction.ts";
  import { quiet } from "./lib/motion.ts";
  import { modal } from "./lib/modal.ts";
  import { resolveReference } from "./lib/refs.ts";
  import { isObjectIdHex, isUlid, parseTypedId } from "./lib/values.ts";

  let { overview, migrations, navigate, onclose } = $props();

  let query = $state("");
  let active = $state(0);
  let input = $state();
  let list = $state();

  function idCommands(text) {
    const value = text.trim();
    if (!value || value.includes(" ")) return [];
    const typed = parseTypedId(value);
    if (typed) {
      const resolved = resolveReference(overview?.collections, typed.prefix);
      if (!resolved) return [];
      const { collection } = resolved;
      return [
        {
          group: "Document",
          icon: Hash,
          label: value,
          hint: `open in ${collection}`,
          mono: true,
          run: () => navigate({ view: "collection", collection, tab: "data", type: resolved.type ?? "", open: JSON.stringify(value) }),
        },
      ];
    }
    if (!isObjectIdHex(value) && !isUlid(value) && value.length < 8) return [];
    const id = isObjectIdHex(value) ? JSON.stringify({ $oid: value }) : JSON.stringify(value);
    return (overview?.collections ?? [])
      .filter((c) => c.exists && c.kind !== "internal")
      .slice(0, 6)
      .map((c) => ({
        group: "Document",
        icon: Hash,
        label: value,
        hint: `look up in ${c.name}`,
        mono: true,
        run: () => navigate({ view: "collection", collection: c.name, tab: "data", open: id }),
      }));
  }

  const commands = $derived.by(() => {
    const all = [
      { group: "Views", icon: LayoutGrid, label: "Overview", run: () => navigate({ view: "home" }) },
      { group: "Views", icon: History, label: "Migrations", run: () => navigate({ view: "migrations" }) },
      {
        group: "Actions",
        icon: Play,
        label: "Run check",
        hint: "dry run, in memory",
        run: () => {
          requestCheck();
          navigate({ view: "migrations", section: "check" });
        },
      },
      { group: "Actions", icon: List, label: "Show plan", hint: "pending migrations", run: () => navigate({ view: "migrations", section: "plan" }) },
      { group: "Actions", icon: GitCompare, label: "Show drift", hint: "schemas, validators, indexes", run: () => navigate({ view: "migrations", section: "drift" }) },
      { group: "Actions", icon: History, label: "Show history", hint: "applied runs", run: () => navigate({ view: "migrations", section: "history" }) },
      { group: "Views", icon: FileCode, label: "Value components", run: () => navigate({ view: "gallery" }) },
    ];
    for (const c of overview?.collections ?? []) {
      all.push({
        group: "Collections",
        icon: Layers,
        label: c.name,
        hint: kindLabel(c.kind),
        mono: true,
        run: () => navigate({ view: "collection", collection: c.name, tab: defaultTab(c.kind) }),
      });
      if (c.kind !== "collection") {
        for (const t of c.types.filter((t) => !t.meta)) {
          all.push({
            group: "Types",
            icon: Box,
            label: t.name,
            hint: c.name,
            type: t.name,
            run: () => navigate({ view: "collection", collection: c.name, tab: "data", type: t.name }),
          });
        }
      }
      for (const s of c.scopes?.top ?? []) {
        const scope = typeof s.scope === "string" ? s.scope : JSON.stringify(s.scope);
        all.push({
          group: "Scopes",
          icon: GitBranch,
          label: scope,
          hint: c.name,
          mono: true,
          run: () => navigate({ view: "collection", collection: c.name, tab: "data", scope }),
        });
      }
    }
    for (const m of migrations?.migrations ?? []) {
      all.push({
        group: "Migrations",
        icon: History,
        label: m.name,
        hint: m.status,
        run: () => navigate({ view: "migrations" }),
      });
    }
    const ranked = all
      .map((command) => ({
        command,
        score: Math.max(matchScore(query, command.label), matchScore(query, command.hint ?? "") * 0.6),
      }))
      .filter((entry) => entry.score > 0)
      .sort((a, b) => b.score - a.score)
      .map((entry) => entry.command);
    const byGroup = new Map();
    for (const command of ranked) {
      const bucket = byGroup.get(command.group) ?? [];
      if (bucket.length < (query ? 8 : 5)) bucket.push(command);
      byGroup.set(command.group, bucket);
    }
    const order = query ? [...byGroup.keys()] : ["Views", "Actions", "Collections", "Types", "Scopes", "Migrations"];
    return [...idCommands(query), ...order.flatMap((group) => byGroup.get(group) ?? [])];
  });

  $effect(() => {
    query;
    active = 0;
  });

  function run(command) {
    quiet();
    onclose();
    command?.run();
  }

  async function onKey(event) {
    if (event.key === "Escape") {
      event.preventDefault();
      onclose();
      return;
    }
    const action =
      event.key === "ArrowDown"
        ? { type: "next" }
        : event.key === "ArrowUp"
          ? { type: "previous" }
          : null;
    if (action) {
      event.preventDefault();
      active = moveSelection(active, commands.length, action) ?? 0;
      await tick();
      list?.querySelector(`[data-index="${active}"]`)?.scrollIntoView({ block: "nearest" });
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      run(commands[active]);
    }
  }

  onMount(() => {
    input?.focus();
  });
</script>

<dialog class="palette-root" data-palette aria-label="Jump to" use:modal={{ onclose, initialFocus: () => input }}>
  <button class="backdrop" aria-label="Close" tabindex="-1" onclick={onclose}></button>
  <div class="bezel palette">
    <div class="bezel-card card">
      <label class="search">
        <Icon icon={Search} size={16} />
        <input
          bind:this={input}
          bind:value={query}
          onkeydown={onKey}
          placeholder="Jump to a collection, type, scope, migration or paste a document id"
          aria-label="Search"
          spellcheck="false"
          autocomplete="off"
        />
      </label>
      <div class="list" bind:this={list} role="listbox">
        {#each commands as command, index (command.group + command.label + (command.hint ?? "") + index)}
          {#if index === 0 || commands[index - 1].group !== command.group}
            <div class="group-label">{command.group}</div>
          {/if}
          <div
            class="item"
            class:active={index === active}
            role="option"
            tabindex="-1"
            aria-selected={index === active}
            data-index={index}
            onmousemove={() => (active = index)}
            onclick={() => run(command)}
            onkeydown={(event) => event.key === "Enter" && run(command)}
          >
            <span class="icon"><Icon icon={command.icon} size={14} /></span>
            {#if command.type}
              <TypeTag name={command.type} />
            {:else}
              <span class="label" class:mono={command.mono}>{command.label}</span>
            {/if}
            {#if command.hint}<span class="hint">{command.hint}</span>{/if}
            {#if index === active}<span class="enter"><Icon icon={CornerDownLeft} size={13} /></span>{/if}
          </div>
        {:else}
          <div class="empty">Nothing matches <span class="mono">{query}</span></div>
        {/each}
      </div>
    </div>
    <footer class="bezel-foot foot">
      <span class="keys">
        <span><kbd class="kbd">↑</kbd><kbd class="kbd">↓</kbd> move</span>
        <span><kbd class="kbd">↵</kbd> open</span>
        <span><kbd class="kbd">esc</kbd> close</span>
      </span>
    </footer>
  </div>
</dialog>

<style>
  .palette-root {
    position: fixed;
    inset: 0;
    width: 100%;
    max-width: none;
    height: 100%;
    max-height: none;
    margin: 0;
    border: 0;
    background: transparent;
    color: inherit;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 14vh 16px 16px;
  }

  .palette-root::backdrop {
    background: transparent;
  }

  .backdrop {
    position: absolute;
    inset: 0;
    border: 0;
    background: var(--scrim);
    cursor: default;
  }

  .palette {
    position: relative;
    width: min(620px, 100%);
    max-height: 70vh;
    box-shadow: var(--shadow-overlay);
  }

  .card {
    min-height: 0;
  }

  .search {
    display: flex;
    align-items: center;
    gap: 10px;
    height: 48px;
    padding: 0 14px;
    border-bottom: 1px solid var(--hairline);
    color: var(--text-faint);
  }

  .search input {
    flex: 1;
    min-width: 0;
    border: 0;
    outline: none;
    background: none;
    color: var(--text);
    font-size: var(--text-md);
  }

  .search input::placeholder {
    color: var(--text-faint);
  }

  .list {
    display: flex;
    flex-direction: column;
    padding: 4px;
    overflow-y: auto;
  }

  .group-label {
    padding: 10px 10px 4px;
    color: var(--text-faint);
    font-family: var(--font-mono);
    font-size: 11px;
    letter-spacing: 0.02em;
  }

  .item {
    position: relative;
    display: flex;
    align-items: center;
    gap: 10px;
    min-height: 34px;
    padding: 0 10px;
    border-radius: var(--radius-control);
    cursor: pointer;
  }

  .item.active {
    background: var(--select-bg);
    box-shadow: inset 2px 0 0 var(--select-edge);
  }

  .icon {
    display: inline-flex;
    color: var(--text-faint);
  }

  .item.active .icon {
    color: var(--accent);
  }

  .label {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .label.mono {
    font-size: 12.5px;
  }

  .hint {
    overflow: hidden;
    color: var(--text-faint);
    font-size: var(--text-xs);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .enter {
    display: inline-flex;
    margin-left: auto;
    color: var(--text-faint);
  }

  .empty {
    padding: 24px 12px;
    color: var(--text-muted);
    text-align: center;
  }

  .foot {
    min-height: 36px;
  }

  .keys {
    display: flex;
    gap: 14px;
    color: var(--text-faint);
    font-size: 11.5px;
  }

  .keys span {
    display: inline-flex;
    align-items: center;
    gap: 3px;
  }
</style>
