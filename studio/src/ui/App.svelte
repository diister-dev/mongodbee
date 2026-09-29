<script>
  import { onMount } from "svelte";
  import { api } from "./lib/api.js";
  import { ChevronRight, PanelLeft, RefreshCw } from "./lib/icons.js";
  import Icon from "./Icon.svelte";
  import Spectrum from "./controls/Spectrum.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import Sidebar from "./Sidebar.svelte";
  import { hueFor } from "./lib/values.ts";
  import Home from "./Home.svelte";
  import CollectionView from "./CollectionView.svelte";
  import MigrationsView from "./MigrationsView.svelte";
  import Gallery from "./Gallery.svelte";
  import { provideStudio } from "./lib/studio.js";
  import { resolveReference } from "./lib/refs.ts";
  import { forgetLabels } from "./lib/labels.js";
  import ErrorState from "./ErrorState.svelte";
  import { trackModifiers } from "./lib/modifiers.js";
  import { fade } from "./lib/motion.ts";
  import CommandPalette from "./CommandPalette.svelte";

  const SECTION_LABEL = { timeline: "Timeline", plan: "Plan", check: "Check", drift: "Drift", history: "History" };

  let meta = $state(null);
  let overview = $state(null);
  let migrations = $state(null);
  let error = $state(null);
  let loading = $state(false);
  let generation = $state(0);
  let route = $state(parseHash());

  const current = $derived(
    route.view === "collection" ? overview?.collections.find((c) => c.name === route.collection) : undefined,
  );

  function parseHash() {
    const hash = window.location.hash.replace(/^#\/?/, "");
    const [path, query = ""] = hash.split("?");
    const parts = path.split("/").filter(Boolean).map(decodeURIComponent);
    const params = new URLSearchParams(query);
    if (parts[0] === "migrations") return { view: "migrations", section: parts[1] ?? "timeline" };
    if (parts[0] === "dev" && parts[1] === "values") return { view: "gallery" };
    if (parts[0] === "c" && parts[1]) {
      return {
        view: "collection",
        collection: parts[1],
        tab: parts[2] ?? "data",
        type: params.get("type") ?? "",
        scope: params.get("scope") ?? "",
        open: params.get("open") ?? "",
        where: params.getAll("w"),
      };
    }
    return { view: "home" };
  }

  function navigate(next) {
    let hash = "#/";
    if (next.view === "migrations") {
      hash = next.section && next.section !== "timeline" ? `#/migrations/${next.section}` : "#/migrations";
    }
    if (next.view === "gallery") hash = "#/dev/values";
    if (next.view === "collection") {
      hash = `#/c/${encodeURIComponent(next.collection)}/${next.tab ?? "data"}`;
      const params = new URLSearchParams();
      if (next.type) params.set("type", next.type);
      if (next.scope) params.set("scope", next.scope);
      if (next.open) params.set("open", next.open);
      for (const condition of next.where ?? []) params.append("w", condition);
      const query = params.toString();
      if (query) hash += `?${query}`;
    }
    if (window.location.hash !== hash) window.location.hash = hash;
    else route = parseHash();
  }

  function resolveType(prefix) {
    return resolveReference(overview?.collections, prefix)?.collection;
  }

  provideStudio({
    resolveType,
    openDocument(collection, id, type) {
      const resolved = resolveReference(overview?.collections, type);
      const typed = resolved?.collection === collection ? (resolved.type ?? "") : "";
      navigate({ view: "collection", collection, tab: "data", type: typed, open: id });
    },
    get openScope() {
      if (route.view !== "collection") return undefined;
      const target = overview?.collections.find((c) => c.name === route.collection);
      if (target?.kind !== "scopedMultiCollection") return undefined;
      return (scope) =>
        navigate({ view: "collection", collection: route.collection, tab: "data", type: route.type, scope });
    },
    get write() {
      if (!meta?.write) return { enabled: false, reason: null };
      if (meta.schemasSource !== "project") {
        return { enabled: false, reason: "Editing needs the project's schemas.ts, which could not be loaded" };
      }
      if ((migrations?.pending ?? 0) > 0) {
        return {
          enabled: false,
          reason: `Editing is paused while ${migrations.pending} migration${migrations.pending === 1 ? " is" : "s are"} pending`,
        };
      }
      return { enabled: true, reason: null };
    },
    refreshOverview() {
      load();
    },
    reference(prefix) {
      return resolveReference(overview?.collections, prefix);
    },
  });

  let loadError = $state(null);

  async function load() {
    loading = true;
    error = null;
    loadError = null;
    try {
      const [nextMeta, nextOverview, nextMigrations] = await Promise.all([
        api("/api/meta"),
        api("/api/overview"),
        api("/api/migrations"),
      ]);
      if (meta?.buildId && nextMeta.buildId && meta.buildId !== nextMeta.buildId) {
        window.location.reload();
        return;
      }
      meta = nextMeta;
      overview = nextOverview;
      migrations = nextMigrations;
      generation += 1;
    } catch (e) {
      error = e.message;
      loadError = e;
    } finally {
      loading = false;
    }
  }

  async function checkBuild() {
    try {
      const next = await api("/api/meta");
      if (meta?.buildId && next.buildId && meta.buildId !== next.buildId) window.location.reload();
    } catch {
      error = error ?? "The studio server is not reachable";
    }
  }

  let paletteOpen = $state(false);

  const viewKey = $derived(
    route.view === "collection" && current ? `c:${current.name}:${generation}` : `${route.view}:${generation}`,
  );

  const SIDEBAR_KEY = "mongodbee-studio:sidebar";

  let collapsed = $state(readCollapsed());

  function readCollapsed() {
    try {
      return window.localStorage.getItem(SIDEBAR_KEY) === "collapsed";
    } catch {
      return false;
    }
  }

  function toggleSidebar() {
    collapsed = !collapsed;
    try {
      window.localStorage.setItem(SIDEBAR_KEY, collapsed ? "collapsed" : "open");
    } catch {}
  }

  const crumbs = $derived.by(() => {
    if (route.view === "migrations") {
      return [
        { label: "Migrations", to: { view: "migrations" } },
        { label: SECTION_LABEL[route.section] ?? "Timeline" },
      ];
    }
    if (route.view === "gallery") return [{ label: "Value components" }];
    if (route.view === "collection") {
      const list = [
        { label: "Collections", to: { view: "home" } },
        {
          label: route.collection,
          mono: true,
          to: { view: "collection", collection: route.collection, tab: route.tab },
        },
      ];
      if (route.type) list.push({ label: route.type, mono: true, dot: hueFor(route.type).dot });
      return list;
    }
    return [{ label: "Overview" }];
  });

  function onGlobalKey(event) {
    if (!(event.metaKey || event.ctrlKey) || event.altKey || event.shiftKey) return;
    const key = event.key.toLowerCase();
    if (key === "k") {
      event.preventDefault();
      paletteOpen = !paletteOpen;
    }
    if (key === "b") {
      event.preventDefault();
      toggleSidebar();
    }
  }

  onMount(() => {
    const stopTracking = trackModifiers();
    load();
    const onHash = () => (route = parseHash());
    const onFocus = () => checkBuild();
    window.addEventListener("hashchange", onHash);
    window.addEventListener("focus", onFocus);
    return () => {
      stopTracking();
      window.removeEventListener("hashchange", onHash);
      window.removeEventListener("focus", onFocus);
    };
  });
</script>

<div class="shell" class:collapsed>
  <Sidebar
    {meta}
    {overview}
    {migrations}
    {route}
    {navigate}
    compact={collapsed}
    unreachable={error}
    onsearch={() => (paletteOpen = true)}
  />
  <div class="main">
    <header class="topbar">
      <Tooltip text={collapsed ? "Expand the sidebar" : "Collapse the sidebar"} kbd="⌘B">
        <button
          class="control quiet icon"
          aria-label={collapsed ? "Expand the sidebar" : "Collapse the sidebar"}
          aria-keyshortcuts="Meta+B Control+B"
          aria-expanded={!collapsed}
          onclick={toggleSidebar}
        >
          <Icon icon={PanelLeft} size={16} />
        </button>
      </Tooltip>
      <span class="rule" aria-hidden="true"></span>
      <nav class="crumbs" aria-label="Breadcrumb">
        {#each crumbs as crumb, index (index)}
          {#if index > 0}<span class="sep" aria-hidden="true"><Icon icon={ChevronRight} size={13} /></span>{/if}
          {#if crumb.to && index < crumbs.length - 1}
            <button class="crumb press" onclick={() => navigate(crumb.to)}>
              {#if crumb.dot}<span class="crumb-dot" style="background: {crumb.dot}"></span>{/if}
              <span class:mono={crumb.mono}>{crumb.label}</span>
            </button>
          {:else}
            <span class="crumb current" aria-current="page">
              {#if crumb.dot}<span class="crumb-dot" style="background: {crumb.dot}"></span>{/if}
              <span class:mono={crumb.mono}>{crumb.label}</span>
            </span>
          {/if}
        {/each}
      </nav>
      <div class="actions">
        <Tooltip text="Reload schemas, counts and migrations">
          <button
            class="control quiet icon"
            aria-label="Reload"
            disabled={loading}
            onclick={() => {
              forgetLabels();
              load();
            }}
          >
            <span class="spin" class:spinning={loading}><Icon icon={RefreshCw} /></span>
          </button>
        </Tooltip>
      </div>
    </header>

    <main class="content">
      {#if error && !overview}
        <ErrorState error={loadError ?? error} title="The studio could not load" onretry={load} />
      {:else if !overview}
        <div class="empty-state">
          <Spectrum size={8} loading />
          <span class="muted">Loading the studio</span>
        </div>
      {:else}
        {#key viewKey}
          <div class="view" in:fade={{ duration: 150, blur: 3 }}>
            {#if route.view === "gallery"}
              <Gallery />
            {:else if route.view === "migrations"}
              <MigrationsView report={migrations} section={route.section} {navigate} />
            {:else if route.view === "collection"}
              {#if current}
                <CollectionView
                  collection={current}
                  tab={route.tab}
                  type={route.type}
                  scope={route.scope}
                  open={route.open}
                  where={route.where}
                  {navigate}
                />
              {:else}
                <div class="empty-state">No collection named <span class="mono">{route.collection}</span></div>
              {/if}
            {:else}
              <Home {meta} {overview} {migrations} {navigate} />
            {/if}
          </div>
        {/key}
      {/if}
    </main>
  </div>
</div>

{#if paletteOpen}
  <CommandPalette {overview} {migrations} {navigate} onclose={() => (paletteOpen = false)} />
{/if}

<svelte:window onkeydown={onGlobalKey} />

<style>
  .shell {
    display: grid;
    grid-template-columns: 240px minmax(0, 1fr);
    height: 100%;
    background: var(--frame);
  }

  .shell > :global(aside) {
    border-right: 1px solid var(--frame-border);
  }

  .shell.collapsed {
    grid-template-columns: 52px minmax(0, 1fr);
  }

  .shell.collapsed .topbar {
    padding-left: 8px;
  }

  .shell.collapsed .content {
    padding-left: 12px;
  }

  .rule {
    flex: none;
    width: 1px;
    height: 16px;
    margin: 0 4px 0 2px;
    background: var(--border-strong);
  }

  .crumb-dot {
    flex: none;
    width: 6px;
    height: 6px;
    margin-right: 6px;
  }

  .main {
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    background: var(--canvas);
  }

  .topbar {
    display: flex;
    flex: none;
    align-items: center;
    gap: 4px;
    height: 60px;
    padding: 0 16px 0 4px;
  }

  .crumbs {
    display: flex;
    align-items: center;
    gap: 0;
    min-width: 0;
  }

  .crumb {
    display: inline-flex;
    align-items: center;
    height: 28px;
    padding: 0 8px;
    border: 0;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-muted);
    font-size: var(--text-sm);
    white-space: nowrap;
    transition:
      background-color var(--dur-quick) var(--ease),
      color var(--dur-quick) var(--ease),
      transform var(--dur-press) var(--ease-out);
  }

  @media (hover: hover) and (pointer: fine) {
    button.crumb:hover {
      background: var(--bg-active);
      color: var(--text);
    }
  }

  .crumb.current {
    color: var(--text);
  }

  .crumb .mono {
    font-size: var(--text-sm);
  }

  .sep {
    display: inline-flex;
    color: var(--text-faint);
  }

  .actions {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-left: auto;
  }

  .spin {
    display: inline-flex;
  }

  .spin.spinning {
    animation: spin 700ms linear infinite;
  }

  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }

  @media (prefers-reduced-motion: reduce) {
    .spin.spinning {
      animation: none;
    }
  }

  .content {
    display: flex;
    flex: 1;
    flex-direction: column;
    min-height: 0;
    padding: 0 16px 16px 8px;
    overflow: hidden;
  }

  .view {
    display: flex;
    flex: 1;
    flex-direction: column;
    min-height: 0;
  }
</style>
