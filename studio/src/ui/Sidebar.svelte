<script>
  import { defaultTab, formatNumber, GROUPS, isTypedKind } from "./lib/format.js";
  import {
    Box,
    CircleDashed,
    Database,
    FileCode,
    GitBranch,
    History,
    Layers,
    LayoutGrid,
    Search,
    Settings,
    X,
  } from "./lib/icons.js";
  import Icon from "./Icon.svelte";
  import Logo from "./Logo.svelte";
  import Status from "./Status.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import { hueFor } from "./lib/values.ts";
  import { useStudio } from "./lib/studio.js";

  let { meta, overview, migrations, route, navigate, onsearch, unreachable = null, compact = false } = $props();

  const studio = useStudio();

  const FILTER_THRESHOLD = 10;

  const KIND_ICON = {
    collection: Database,
    multiCollection: Layers,
    scopedMultiCollection: GitBranch,
    multiModelInstance: Box,
    undeclared: CircleDashed,
    internal: Settings,
  };

  let query = $state("");

  const collections = $derived(overview?.collections ?? []);
  const showFilter = $derived(collections.length > FILTER_THRESHOLD || query !== "");

  const groups = $derived.by(() => {
    const needle = query.trim().toLowerCase();
    const visible = collections.filter((c) => !needle || c.name.toLowerCase().includes(needle));
    return GROUPS.map((group) => ({
      ...group,
      items: visible.filter((c) => c.kind === group.kind),
    })).filter((group) => group.items.length > 0);
  });

  const pending = $derived(migrations?.pending ?? 0);
</script>

{#if compact}
<aside class="icon-rail" aria-label="Studio">
  <div class="rail-brand"><span class="brand-tile"><Logo size={18} /></span></div>
  <nav class="rail-scroll" aria-label="Studio">
    <Tooltip text="Search" kbd="⌘K" side="right">
      <button class="rail-item" aria-label="Search" aria-keyshortcuts="Meta+K Control+K" onclick={onsearch}>
        <Icon icon={Search} />
      </button>
    </Tooltip>
    <Tooltip text="Overview" side="right">
      <button
        class="rail-item"
        class:active={route.view === "home"}
        aria-label="Overview"
        aria-current={route.view === "home" ? "page" : undefined}
        onclick={() => navigate({ view: "home" })}
      >
        <Icon icon={LayoutGrid} />
      </button>
    </Tooltip>
    <Tooltip text={pending > 0 ? `Migrations, ${pending} pending` : "Migrations"} side="right">
      <button
        class="rail-item"
        class:active={route.view === "migrations"}
        aria-label={pending > 0 ? `Migrations, ${pending} pending` : "Migrations"}
        aria-current={route.view === "migrations" ? "page" : undefined}
        onclick={() => navigate({ view: "migrations" })}
      >
        <Icon icon={History} />
        {#if pending > 0}<span class="rail-dot" aria-hidden="true"></span>{/if}
      </button>
    </Tooltip>
    <span class="rail-rule" aria-hidden="true"></span>
    {#each collections as collection (collection.name)}
      {@const selected = route.view === "collection" && route.collection === collection.name}
      <Tooltip
        text={`${collection.name}, ${formatNumber(collection.total)} ${collection.total === 1 ? "document" : "documents"}`}
        side="right"
      >
        <button
          class="rail-item"
          class:active={selected}
          class:dim={collection.kind === "internal" || !collection.exists}
          aria-label={collection.name}
          aria-current={selected ? "page" : undefined}
          onclick={() =>
            navigate({ view: "collection", collection: collection.name, tab: selected ? route.tab : defaultTab(collection.kind) })}
        >
          <Icon icon={KIND_ICON[collection.kind] ?? CircleDashed} size={15} />
        </button>
      </Tooltip>
    {/each}
  </nav>
  <footer class="rail-foot">
    <Tooltip text="Value components" side="right">
      <button
        class="rail-item"
        class:active={route.view === "gallery"}
        aria-label="Value components"
        onclick={() => navigate({ view: "gallery" })}
      >
        <Icon icon={FileCode} />
      </button>
    </Tooltip>
    <Tooltip
      text={unreachable ??
        (meta?.write
          ? (studio.write.reason ?? `Write enabled: edits go to ${meta?.database ?? "the database"}`)
          : `Read-only: the studio never writes to ${meta?.database ?? "the database"}`)}
      side="right"
    >
      <span
        class="rail-status"
        role="img"
        aria-label={unreachable ? "Unreachable" : meta?.write ? "Write enabled" : "Read-only"}
      >
        <Status tone={unreachable ? "danger" : meta?.write ? "warning" : "live"} />
      </span>
    </Tooltip>
  </footer>
</aside>
{:else}
<aside class="rail">
  <header class="brand">
    <span class="brand-tile"><Logo size={18} /></span>
    <span class="brand-text">
      <span class="brand-name">mongodbee <span class="brand-product">studio</span></span>
      <Tooltip text={meta?.database ?? ""} overflow>
        <span class="brand-db mono">{meta?.database ?? ""}</span>
      </Tooltip>
    </span>
  </header>

  <nav class="scroll" aria-label="Studio">
    <div class="block">
      <button class="item" onclick={onsearch} aria-keyshortcuts="Meta+K Control+K">
        <span class="lead"><Icon icon={Search} /></span>
        <span class="label">Search</span>
        <kbd class="kbd">⌘K</kbd>
      </button>
      <button
        class="item"
        class:active={route.view === "home"}
        aria-current={route.view === "home" ? "page" : undefined}
        onclick={() => navigate({ view: "home" })}
      >
        <span class="lead"><Icon icon={LayoutGrid} /></span>
        <span class="label">Overview</span>
      </button>
      <button
        class="item"
        class:active={route.view === "migrations"}
        aria-current={route.view === "migrations" ? "page" : undefined}
        onclick={() => navigate({ view: "migrations" })}
      >
        <span class="lead"><Icon icon={History} /></span>
        <span class="label">Migrations</span>
        {#if pending > 0}
          <span class="pending num">{pending} pending</span>
        {/if}
      </button>
    </div>

    {#if showFilter}
      <label class="search">
        <Icon icon={Search} size={14} />
        <input
          placeholder="Filter collections"
          bind:value={query}
          aria-label="Filter collections"
          spellcheck="false"
          autocomplete="off"
          onkeydown={(event) => {
            if (event.key === "Escape" && query) {
              event.preventDefault();
              query = "";
            }
          }}
        />
        {#if query}
          <button class="clear press" type="button" aria-label="Clear the filter" onclick={() => (query = "")}>
            <Icon icon={X} size={12} />
          </button>
        {/if}
      </label>
    {/if}

    {#if query && groups.length === 0}
      <div class="no-match">No collection matches <span class="mono">{query}</span></div>
    {/if}

    {#each groups as group (group.kind)}
      <div class="block">
        <div class="group-label">{group.label}</div>
        {#each group.items as collection (collection.name)}
          {@const selected = route.view === "collection" && route.collection === collection.name}
          <button
            class="item"
            class:active={selected && !route.type}
            class:parent={selected && route.type}
            class:dim={collection.kind === "internal" || !collection.exists}
            aria-current={selected && !route.type ? "page" : undefined}
            onclick={() =>
              navigate({ view: "collection", collection: collection.name, tab: selected ? route.tab : defaultTab(collection.kind) })}
          >
            <span class="lead"><Icon icon={KIND_ICON[collection.kind] ?? CircleDashed} size={15} /></span>
            <Tooltip
              text={collection.exists ? collection.name : `${collection.name} is not created yet`}
              overflow={collection.exists}
            >
              <span class="label mono">{collection.name}</span>
            </Tooltip>
            <span class="count num">{formatNumber(collection.total)}</span>
          </button>
          {#if selected && isTypedKind(collection.kind)}
            <div class="types">
              {#each collection.types.filter((t) => !t.meta) as type (type.name)}
                <button
                  class="item sub"
                  class:active={route.type === type.name}
                  aria-current={route.type === type.name ? "page" : undefined}
                  onclick={() =>
                    navigate({ view: "collection", collection: collection.name, tab: route.tab === "summary" ? "data" : route.tab, type: type.name })}
                >
                  <span class="type-dot" style="background: {hueFor(type.name).dot}"></span>
                  <span class="label mono">{type.name}</span>
                  {#if !type.declared}
                    <span class="undeclared">undeclared</span>
                  {/if}
                  <span class="count num">{formatNumber(type.count)}</span>
                </button>
              {/each}
            </div>
          {/if}
        {/each}
      </div>
    {/each}
  </nav>

  <footer class="foot">
    <button
      class="item quiet"
      class:active={route.view === "gallery"}
      onclick={() => navigate({ view: "gallery" })}
    >
      <span class="lead"><Icon icon={FileCode} /></span>
      <span class="label">Value components</span>
    </button>
    <div class="connection">
      {#if unreachable}
        <Tooltip text={unreachable}><Status tone="danger" label="Unreachable" /></Tooltip>
      {:else if meta?.write}
        <Tooltip text={studio.write.reason ?? `Edits, creations and deletions go to ${meta?.database ?? "the database"}`}>
          <Status tone="warning" hollow={!studio.write.enabled} label={studio.write.enabled ? "Write enabled" : "Write paused"} />
        </Tooltip>
      {:else}
        <Tooltip text="The studio only reads; it never writes to {meta?.database ?? 'the database'}">
          <Status tone="live" label="Read-only" />
        </Tooltip>
      {/if}
      {#if meta?.version}<span class="version mono">v{meta.version}</span>{/if}
    </div>
  </footer>
</aside>
{/if}

<style>
  .rail {
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  .icon-rail {
    display: flex;
    flex-direction: column;
    align-items: center;
    min-height: 0;
    border-right: 1px solid var(--frame-border);
  }

  .rail-brand {
    display: flex;
    flex: none;
    align-items: center;
    justify-content: center;
    height: 60px;
  }

  .rail-scroll {
    display: flex;
    flex: 1;
    flex-direction: column;
    align-items: center;
    gap: 2px;
    width: 100%;
    min-height: 0;
    padding: 6px 0 16px;
    overflow-y: auto;
    overscroll-behavior: contain;
    scrollbar-width: none;
  }

  .rail-scroll::-webkit-scrollbar {
    display: none;
  }

  .rail-item {
    position: relative;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 34px;
    height: 32px;
    padding: 0;
    border: 1px solid transparent;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-faint);
    transition: transform var(--dur-press) var(--ease-out);
  }

  .rail-item:active {
    transform: scale(0.97);
  }

  .rail-item.active {
    border-color: var(--card-border);
    background: var(--card);
    color: var(--accent);
    box-shadow: var(--card-shadow);
  }

  .rail-item.active::before {
    position: absolute;
    top: 50%;
    left: -9px;
    width: 2px;
    height: 14px;
    background: var(--select-edge);
    content: "";
    translate: 0 -50%;
  }

  .rail-item.dim {
    opacity: 0.55;
  }

  .rail-dot {
    position: absolute;
    top: 6px;
    right: 6px;
    width: 6px;
    height: 6px;
    border: 1.5px solid var(--frame);
    border-radius: 0;
    background: var(--warning);
    box-sizing: content-box;
  }

  .rail-rule {
    flex: none;
    width: 18px;
    height: 1px;
    margin: 8px 0;
    background: var(--frame-border);
  }

  .rail-foot {
    display: flex;
    flex: none;
    flex-direction: column;
    align-items: center;
    gap: 6px;
    width: 100%;
    padding: 8px 0 14px;
    border-top: 1px solid var(--frame-border);
  }

  .rail-status {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 34px;
    height: 24px;
  }

  @media (hover: hover) and (pointer: fine) {
    .rail-item:not(.active):hover {
      background: var(--bg-active);
      color: var(--text);
    }
  }

  @media (prefers-reduced-motion: reduce) {
    .rail-item:active {
      transform: none;
    }
  }

  .brand {
    display: flex;
    flex: none;
    align-items: center;
    gap: 10px;
    height: 60px;
    padding: 0 14px 0 16px;
  }

  .brand-tile {
    display: inline-flex;
    flex: none;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-card);
    background: var(--card);
    box-shadow:
      0 0 0 3px var(--frame),
      0 0 0 4px var(--frame-border);
  }

  .brand-text {
    flex: 1;
  }

  .brand-product {
    color: var(--text-faint);
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 400;
    letter-spacing: 0.02em;
  }

  .brand-text {
    display: flex;
    flex-direction: column;
    min-width: 0;
    line-height: 16px;
  }

  .brand-name {
    font-size: var(--text-md);
    font-weight: 600;
    letter-spacing: -0.02em;
  }

  .brand-db {
    overflow: hidden;
    color: var(--text-muted);
    font-size: 11.5px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .scroll {
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 18px;
    padding: 6px 10px 20px 12px;
    overflow-y: auto;
    overscroll-behavior: contain;
  }

  .block {
    display: flex;
    flex-direction: column;
    gap: 1px;
  }

  .group-label {
    padding: 0 9px 5px;
    color: var(--text-faint);
    font-family: var(--font-mono);
    font-size: 11px;
    letter-spacing: 0.02em;
  }

  .search {
    display: flex;
    align-items: center;
    gap: 8px;
    height: 30px;
    padding: 0 9px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    background: var(--card);
    color: var(--text-faint);
    cursor: text;
    transition:
      border-color var(--dur-quick) var(--ease),
      box-shadow var(--dur-quick) var(--ease);
  }

  .search:focus-within {
    border-color: var(--text);
    box-shadow: 0 0 0 2px var(--accent-ring);
  }

  .search input {
    flex: 1;
    min-width: 0;
    padding: 0;
    border: 0;
    outline: none;
    background: none;
    color: var(--text);
    font-size: var(--text-sm);
  }

  .search input::placeholder {
    color: var(--text-faint);
  }

  .clear {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    margin-right: -4px;
    padding: 0;
    border: 0;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-faint);
  }

  .no-match {
    padding: 0 9px;
    color: var(--text-muted);
    font-size: var(--text-xs);
  }

  .item {
    position: relative;
    display: flex;
    align-items: center;
    gap: 10px;
    width: 100%;
    height: 30px;
    padding: 0 9px;
    border: 1px solid transparent;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-muted);
    font-size: var(--text-sm);
    text-align: left;
  }

  .item.active::before {
    position: absolute;
    top: 50%;
    left: -7px;
    width: 2px;
    height: 14px;
    background: var(--select-edge);
    content: "";
    translate: 0 -50%;
  }

  .item :global(.tip-trigger) {
    flex: 1;
    min-width: 0;
  }

  .item.active {
    border-color: var(--card-border);
    background: var(--card);
    color: var(--text);
    box-shadow: var(--card-shadow);
  }

  .item.parent {
    color: var(--text);
  }

  .item.dim .label {
    color: var(--text-faint);
  }

  .lead {
    display: inline-flex;
    color: var(--text-faint);
  }

  .item.active .lead {
    color: var(--accent);
  }

  .label {
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .label.mono {
    font-size: var(--text-xs);
  }

  .count {
    color: var(--text-faint);
    font-size: var(--text-xs);
  }

  .item .kbd {
    height: 17px;
    font-size: 10.5px;
  }

  .pending {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    color: var(--warning);
    font-family: var(--font-mono);
    font-size: 10.5px;
    font-weight: 500;
  }

  .pending::before {
    width: 6px;
    height: 6px;
    box-shadow: inset 0 0 0 1.5px currentColor;
    content: "";
  }

  .undeclared {
    color: var(--warning);
    font-size: var(--text-xs);
  }

  .types {
    display: flex;
    flex-direction: column;
    gap: 1px;
    margin: 2px 0 4px 17px;
    padding-left: 10px;
    border-left: 1px solid var(--frame-border);
  }

  .item.sub {
    height: 28px;
    gap: 8px;
  }

  .type-dot {
    flex: none;
    width: 7px;
    height: 7px;
  }

  .foot {
    display: flex;
    flex: none;
    flex-direction: column;
    gap: 6px;
    padding: 8px 10px 12px 12px;
    border-top: 1px solid var(--frame-border);
  }

  .item.quiet {
    height: 28px;
    font-size: var(--text-xs);
  }

  .connection {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    min-height: 26px;
    padding: 0 9px;
  }

  .connection :global(.status) {
    font-size: var(--text-xs);
    white-space: nowrap;
  }

  .version {
    min-width: 0;
    overflow: hidden;
    color: var(--text-faint);
    font-size: 11px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  @media (hover: hover) and (pointer: fine) {
    .item:not(.active):hover {
      background: var(--bg-active);
      color: var(--text);
    }

    .clear:hover {
      background: var(--bg-active);
      color: var(--text);
    }
  }
</style>
