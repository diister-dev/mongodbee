<script>
  import { onMount } from "svelte";
  import { api, collectionPath, send } from "./lib/api.js";
  import { Check, Copy, X } from "./lib/icons.js";
  import DocumentEditor from "./DocumentEditor.svelte";
  import ErrorState from "./ErrorState.svelte";
  import Icon from "./Icon.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import JsonTree from "./JsonTree.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import Value from "./values/Value.svelte";
  import { quiet, slide } from "./lib/motion.ts";
  import { plural } from "./lib/format.js";

  let {
    collection,
    id,
    onclose,
    focusPath = [],
    fields,
    writable = false,
    create = null,
    onchanged,
    oncreated,
    ondeleted,
    fieldsFor = () => ({}),
  } = $props();

  let document = $state(null);
  let error = $state(null);
  let copied = $state(false);
  let mode = $state("view");
  let deleting = $state(false);
  let deleteError = $state(null);

  const editType = $derived(creating ? create?.type || undefined : typeof document?._type === "string" ? document._type : undefined);
  const editFields = $derived(fieldsFor(editType));

  async function validateValue(value) {
    const result = await send("POST", collectionPath(collection, "validate"), { type: editType, document: value });
    return result.issues ?? [];
  }

  const editScope = $derived(creating ? create?.scope || undefined : typeof document?._scope === "string" ? document._scope : undefined);

  async function suggestValues(path, text) {
    const field = path.filter((part) => typeof part === "string").join(".");
    if (!field) return [];
    const result = await api(collectionPath(collection, "values"), {
      field,
      q: text,
      type: editType,
      scope: editScope,
      limit: 12,
    });
    return result.values
      .filter((row) => typeof row.value === "string")
      .map((row) => ({ value: row.value, label: row.value, hint: `${row.count}${result.sampled ? "+" : ""}` }));
  }

  const creating = $derived(Boolean(create) && !id);

  const label = $derived.by(() => {
    if (!id) return "";
    try {
      const parsed = JSON.parse(id);
      return typeof parsed === "string" ? parsed : (parsed.$oid ?? id);
    } catch {
      return id;
    }
  });

  let attempt = $state(0);

  $effect(() => {
    const target = id;
    attempt;
    error = null;
    mode = "view";
    deleteError = null;
    if (!target) return;
    loading = true;
    api(collectionPath(collection, "document"), { id: target })
      .then((result) => {
        if (target === id) {
          document = result;
          shown = target;
        }
      })
      .catch((e) => {
        if (target === id) error = e;
      })
      .finally(() => {
        if (target === id) loading = false;
      });
  });

  let loading = $state(false);
  let shown = $state(null);

  function target() {
    const payload = { id };
    if (typeof document?._type === "string") payload.type = document._type;
    if (typeof document?._scope === "string") payload.scope = document._scope;
    return payload;
  }

  async function saveChange(change) {
    await send("PATCH", collectionPath(collection, "document"), {
      ...target(),
      set: change.set,
      unset: change.unset,
      expected: change.expected,
    });
    mode = "view";
    attempt++;
    onchanged?.();
  }

  async function createDocument(value) {
    const result = await send("POST", collectionPath(collection, "documents"), {
      type: create.type || undefined,
      scope: create.scope || undefined,
      document: value,
    });
    onchanged?.();
    oncreated?.(JSON.stringify(result.id?.$oid ? result.id : result.id));
  }

  async function removeDocument() {
    deleting = true;
    deleteError = null;
    try {
      const result = await send("DELETE", collectionPath(collection, "document"), { ...target(), confirm: label });
      onchanged?.();
      ondeleted?.({ label, token: result.restoreToken });
      onclose();
    } catch (e) {
      deleteError = e;
    } finally {
      deleting = false;
    }
  }

  const WIDTH_KEY = "mongodbee-studio:drawer-width";
  const DEFAULT_WIDTH = 540;
  const MIN_WIDTH = 360;
  const STEP = 24;

  let width = $state(readWidth());
  let resizing = $state(false);
  let viewport = $state(window.innerWidth);

  const maxWidth = $derived(Math.max(MIN_WIDTH, Math.min(viewport * 0.8, viewport - 24)));
  const applied = $derived(Math.round(Math.min(Math.max(width, MIN_WIDTH), maxWidth)));

  function readWidth() {
    try {
      const stored = Number(window.localStorage.getItem(WIDTH_KEY));
      return Number.isFinite(stored) && stored > 0 ? stored : DEFAULT_WIDTH;
    } catch {
      return DEFAULT_WIDTH;
    }
  }

  function saveWidth() {
    try {
      window.localStorage.setItem(WIDTH_KEY, String(applied));
    } catch {
      return;
    }
  }

  function setWidth(next) {
    width = Math.min(Math.max(next, MIN_WIDTH), maxWidth);
  }

  function startResize(event) {
    if (event.button !== 0) return;
    event.preventDefault();
    const handle = event.currentTarget;
    handle.setPointerCapture(event.pointerId);
    const startX = event.clientX;
    const startWidth = applied;
    resizing = true;
    const move = (e) => setWidth(startWidth + (startX - e.clientX));
    const end = () => {
      resizing = false;
      handle.removeEventListener("pointermove", move);
      handle.removeEventListener("pointerup", end);
      handle.removeEventListener("pointercancel", end);
      saveWidth();
    };
    handle.addEventListener("pointermove", move);
    handle.addEventListener("pointerup", end);
    handle.addEventListener("pointercancel", end);
  }

  function onHandleKey(event) {
    if (event.key === "ArrowLeft") setWidth(applied + STEP);
    else if (event.key === "ArrowRight") setWidth(applied - STEP);
    else if (event.key === "Home") setWidth(maxWidth);
    else if (event.key === "End") setWidth(MIN_WIDTH);
    else return;
    event.preventDefault();
    event.stopPropagation();
    saveWidth();
  }

  function resetWidth() {
    setWidth(DEFAULT_WIDTH);
    saveWidth();
  }

  async function copy() {
    try {
      await navigator.clipboard.writeText(JSON.stringify(document, null, 2));
      copied = true;
      setTimeout(() => (copied = false), 1200);
    } catch {
      copied = false;
    }
  }

  onMount(() => {
    const onKey = (event) => {
      if (event.key !== "Escape" || event.defaultPrevented) return;
      if (window.document.querySelector("[data-palette]")) return;
      quiet();
      onclose();
    };
    const onResize = () => (viewport = window.innerWidth);
    window.addEventListener("keydown", onKey);
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", onResize);
    };
  });
</script>

<aside
  class="bezel drawer"
  class:resizing
  aria-label="Document {label}"
  style:width="{applied}px"
  in:slide
  out:slide={{ exit: true }}
>
  <button
    type="button"
    class="handle"
    aria-label="Resize the document drawer, {applied} pixels wide"
    aria-keyshortcuts="ArrowLeft ArrowRight Home End"
    onpointerdown={startResize}
    onkeydown={onHandleKey}
    ondblclick={resetWidth}
  ></button>
  <div class="bezel-card inner">
  <header class="head">
    <div class="title">
      {#if creating}
        <span class="muted small">New document</span>
        <span class="id">
          {#if create.type}<TypeTag name={create.type} />{:else}<span class="mono">{collection}</span>{/if}
          {#if create.scope}<span class="faint small mono">in {create.scope}</span>{/if}
        </span>
      {:else}
        <span class="muted small">{mode === "edit" ? "Editing" : mode === "delete" ? "Deleting" : "Document"}</span>
        <span class="id"><Value value={label} field="_id" /></span>
      {/if}
    </div>
    {#if writable && document && !creating && mode === "view"}
      <button class="control" type="button" onclick={() => (mode = "edit")}>Edit</button>
      <button class="control quiet danger-text" type="button" onclick={() => (mode = "delete")}>Delete</button>
    {:else if mode === "delete" && document}
      <span class="inline-confirm" role="alertdialog" aria-label="Delete this document?">
        <span class="small">Delete?</span>
        <button class="control" type="button" onclick={() => (mode = "view")} disabled={deleting}>Keep</button>
        <button class="control danger-fill" type="button" onclick={removeDocument} disabled={deleting}>
          {deleting ? "Deleting" : "Delete"}
        </button>
      </span>
    {/if}
    <Tooltip text="Close" kbd="esc">
      <button class="control quiet icon" aria-label="Close" onclick={onclose}>
        <Icon icon={X} />
      </button>
    </Tooltip>
  </header>
  <div class="body" class:stale={loading && document} class:editing={creating || mode === "edit"}>
    {#if creating}
      <DocumentEditor
        mode="create"
        template={create.template ?? {}}
        fields={editFields}
        validate={validateValue}
        suggest={suggestValues}
        onsubmit={createDocument}
        oncancel={onclose}
      />
    {:else if mode === "edit" && document}
      <DocumentEditor
        mode="edit"
        original={document}
        fields={editFields}
        validate={validateValue}
        suggest={suggestValues}
        onsubmit={saveChange}
        oncancel={() => (mode = "view")}
        onreload={() => {
          mode = "view";
          attempt++;
        }}
      />
    {:else if deleteError}
      <ErrorState error={deleteError} compact title="Not deleted" />
    {:else if error}
      <ErrorState
        {error}
        compact
        title={error?.status === 404 ? `No document ${label} in ${collection}` : "The document could not be read"}
        onretry={error?.status === 404 ? undefined : () => attempt++}
      />
    {:else if !document}
      <div class="empty-state"><span class="shimmer">Loading document</span></div>
    {:else}
      {#key shown}
        <div class="tree">
          <JsonTree value={document} focus={focusPath} {fields} />
        </div>
      {/key}
    {/if}
  </div>
  </div>
  <footer class="bezel-foot">
    <span class="muted small">{document ? plural(Object.keys(document).length, "field") : ""}</span>
    <button class="control" onclick={copy} disabled={!document}>
      <Icon icon={copied ? Check : Copy} size={14} />
      {copied ? "Copied" : "Copy JSON"}
    </button>
  </footer>
</aside>

<style>
  .drawer {
    position: fixed;
    top: 12px;
    right: 12px;
    bottom: 12px;
    z-index: 10;
    max-width: calc(100vw - 24px);
    box-shadow: var(--shadow-overlay);
  }

  .drawer.resizing {
    user-select: none;
  }

  .handle {
    position: absolute;
    top: 0;
    bottom: 0;
    left: -6px;
    z-index: 1;
    width: 12px;
    padding: 0;
    border: 0;
    background: none;
    outline: none;
    cursor: col-resize;
    touch-action: none;
  }

  .handle::after {
    position: absolute;
    top: 50%;
    left: 5px;
    width: 2px;
    height: 32px;
    background: var(--text);
    content: "";
    opacity: 0;
    transform: translateY(-50%);
    transition: opacity var(--dur-quick) var(--ease-out);
  }

  .handle:focus-visible::after,
  .resizing .handle::after {
    opacity: 1;
    transition: none;
  }

  .handle:focus-visible::after {
    box-shadow: 0 0 0 3px var(--accent-ring);
  }

  @media (hover: hover) and (pointer: fine) {
    .handle:hover::after {
      opacity: 0.45;
    }
  }

  .body {
    transition: opacity var(--dur-quick) var(--ease-out);
  }

  .body.stale {
    opacity: 0.6;
    transition: opacity var(--dur-base) var(--ease-out) 200ms;
  }

  .inner {
    flex: 1;
  }

  .head {
    display: flex;
    align-items: center;
    gap: 4px;
    height: 52px;
    padding: 0 8px 0 16px;
    border-bottom: 1px solid var(--border);
  }

  .title {
    display: flex;
    flex: 1;
    flex-direction: column;
    min-width: 0;
    line-height: 16px;
  }

  .small {
    font-size: var(--text-xs);
  }

  .id {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: var(--text-sm);
  }

  .body {
    flex: 1;
    overflow: auto;
    padding: 12px 16px 16px 32px;
  }

  .body.editing {
    display: flex;
    flex-direction: column;
    padding: 12px 16px 16px;
  }

  .id :global(.type-tag) {
    margin-right: 6px;
  }

  .danger-text {
    color: var(--danger);
  }

  .danger-fill {
    border-color: var(--danger);
    background: var(--danger);
    color: var(--card);
    font-weight: 500;
  }

  .danger-fill:disabled {
    border-color: var(--card-border);
    background: var(--card);
    color: var(--text-faint);
  }

  .inline-confirm {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding-left: 10px;
    border-left: 2px solid var(--danger);
  }

  .inline-confirm .small {
    color: var(--danger);
    font-weight: 500;
  }

  @media (hover: hover) and (pointer: fine) {
    .danger-fill:hover:not(:disabled) {
      background: color-mix(in srgb, var(--danger) 85%, black);
    }
  }
</style>
