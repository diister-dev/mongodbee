<script>
  import { onMount, tick } from "svelte";
  import ErrorState from "./ErrorState.svelte";
  import FieldInput from "./FieldInput.svelte";
  import Icon from "./Icon.svelte";
  import Segmented from "./controls/Segmented.svelte";
  import { Plus } from "./lib/icons.js";
  import { diffDocument, editableOf, hasChanges, parseEditable, PROTECTED_KEYS } from "./lib/edit.ts";
  import { defaultFor, removeAt, setAt } from "./lib/form-model.ts";
  import { plural } from "./lib/format.js";

  let {
    mode = "edit",
    original = null,
    template = {},
    fields = {},
    validate,
    suggest,
    onsubmit,
    oncancel,
    onreload,
  } = $props();

  const VIEWS = [
    { id: "form", label: "Form" },
    { id: "json", label: "JSON" },
  ];

  let view = $state("form");
  let value = $state({});
  let text = $state("");
  let textError = $state(null);
  let issues = $state([]);
  let checking = $state(false);
  let checkedOnce = $state(false);
  let submitting = $state(false);
  let failure = $state(null);
  let body = $state();
  let request = 0;
  let timer;

  const change = $derived(mode === "edit" && original ? diffDocument(original, value) : null);
  const changed = $derived(mode === "create" || (change && hasChanges(change)));
  const ready = $derived(changed && !submitting && !textError && issues.length === 0 && !checking);

  const entries = $derived(Object.entries(fields).filter(([key]) => !PROTECTED_KEYS.includes(key)));
  const present = $derived(entries.filter(([key, node]) => !node.optional || key in value));
  const missing = $derived(entries.filter(([key, node]) => node.optional && !(key in value)));
  const extra = $derived(Object.keys(value).filter((key) => !entries.some(([entry]) => entry === key)));

  onMount(async () => {
    value = mode === "edit" && original ? editableOf(original) : { ...template };
    text = JSON.stringify(value, null, 2);
    schedule(0);
    await tick();
    body?.querySelector("input, textarea, [role=combobox]")?.focus();
  });

  function schedule(delay = 350) {
    if (!validate) return;
    clearTimeout(timer);
    const ticket = ++request;
    checking = true;
    timer = setTimeout(async () => {
      try {
        const result = await validate(value);
        if (ticket === request) issues = result;
      } catch {
        if (ticket === request) issues = [];
      } finally {
        if (ticket === request) {
          checking = false;
          checkedOnce = true;
        }
      }
    }, delay);
  }

  function update(path, next) {
    value = setAt(value, path, next);
    failure = null;
    schedule();
  }

  function remove(path) {
    value = removeAt(value, path);
    failure = null;
    schedule();
  }

  function switchView(next) {
    if (next === view) return;
    if (next === "json") {
      text = JSON.stringify(value, null, 2);
      textError = null;
      view = next;
      return;
    }
    const parsed = parseEditable(text);
    if (!parsed.ok) {
      textError = `${parsed.line ? `Line ${parsed.line}: ` : ""}${parsed.message}`;
      return;
    }
    value = parsed.value;
    textError = null;
    view = next;
    schedule(0);
  }

  function onText(event) {
    text = event.currentTarget.value;
    const parsed = parseEditable(text);
    if (!parsed.ok) {
      textError = `${parsed.line ? `Line ${parsed.line}: ` : ""}${parsed.message}`;
      return;
    }
    textError = null;
    value = parsed.value;
    schedule();
  }

  async function submit() {
    if (!ready) return;
    submitting = true;
    failure = null;
    try {
      await onsubmit(mode === "edit" ? change : value);
    } catch (error) {
      failure = error;
      if (error?.status === 422) issues = error.details?.issues ?? [];
    } finally {
      submitting = false;
    }
  }

  function onKey(event) {
    if (event.key === "Escape" && !event.defaultPrevented) {
      const inPopover = event.target instanceof Element && event.target.closest("[role=listbox], [aria-expanded=true]");
      if (inPopover) return;
      event.preventDefault();
      oncancel();
      return;
    }
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      submit();
    }
  }

  function keys(element) {
    element.addEventListener("keydown", onKey);
    return { destroy: () => element.removeEventListener("keydown", onKey) };
  }

  function jumpToIssues() {
    body?.querySelector(".has-issues")?.scrollIntoView({ block: "center", behavior: "smooth" });
  }
</script>

<div class="editor" use:keys role="group" aria-label={mode === "edit" ? "Edit document" : "New document"}>
  <div class="toolbar">
    <Segmented items={VIEWS} value={view} label="Edit as" size="sm" onselect={switchView} />
    <span class="state small" aria-live="polite">
      {#if textError}
        <span class="bad">{textError}</span>
      {:else if checking && !checkedOnce}
        <span class="faint">Checking against the schema</span>
      {:else if issues.length > 0}
        <button class="issues-link bad" type="button" onclick={jumpToIssues}>{plural(issues.length, "field")} to fix</button>
      {:else}
        <span class="good">Matches the schema</span>
      {/if}
    </span>
  </div>

  <div class="body" bind:this={body}>
    {#if view === "form"}
      {#each present as [key, node] (key)}
        <FieldInput
          name={key}
          {node}
          value={value[key]}
          path={[key]}
          {issues}
          onchange={update}
          onremove={remove}
          {suggest}
          removable={Boolean(node.optional)}
        />
      {/each}
      {#each extra as key (key)}
        <FieldInput
          name={key}
          node={{ kind: "unknown" }}
          value={value[key]}
          path={[key]}
          {issues}
          onchange={update}
          onremove={remove}
          removable
        />
      {/each}
      {#if missing.length > 0}
        <div class="add-row">
          <span class="faint small">Optional fields</span>
          {#each missing as [key, node] (key)}
            <button class="add-button press" type="button" onclick={() => update([key], defaultFor(node))}>
              <Icon icon={Plus} size={12} />
              {key}
            </button>
          {/each}
        </div>
      {/if}
    {:else}
      <textarea
        class="code mono"
        class:invalid={textError}
        value={text}
        spellcheck="false"
        autocomplete="off"
        aria-label="Document as JSON"
        oninput={onText}
      ></textarea>
      {#if issues.length > 0}
        <ul class="json-issues small">
          {#each issues as issue, index (index)}
            <li><span class="mono">{issue.path || "document"}</span> <span class="muted">{issue.message}</span></li>
          {/each}
        </ul>
      {/if}
    {/if}
  </div>

  {#if failure && failure.status !== 422}
    <ErrorState error={failure} compact title={failure.status === 409 ? "Not saved" : "The change could not be saved"}>
      {#if failure.status === 409 && onreload}
        <button class="control" type="button" onclick={onreload}>Reload the document</button>
      {/if}
    </ErrorState>
  {/if}

  <div class="footer">
    {#if mode === "edit" && change && hasChanges(change)}
      <span class="chips">
        {#each change.changed as key (key)}<span class="chip changed mono">~ {key}</span>{/each}
        {#each change.added as key (key)}<span class="chip added mono">+ {key}</span>{/each}
        {#each change.removed as key (key)}<span class="chip removed mono">− {key}</span>{/each}
      </span>
    {:else}
      <span class="hint faint small"><kbd class="kbd">ctrl</kbd><kbd class="kbd">↵</kbd> save <kbd class="kbd">esc</kbd> cancel</span>
    {/if}
    <button class="control" type="button" onclick={oncancel} disabled={submitting}>Cancel</button>
    <button class="control primary" type="button" onclick={submit} disabled={!ready}>
      {submitting ? "Saving" : mode === "edit" ? "Save changes" : "Create document"}
    </button>
  </div>
</div>

<style>
  .editor {
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 10px;
    min-height: 0;
  }

  .toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .state {
    min-width: 0;
    text-align: right;
  }

  .small {
    font-size: var(--text-xs);
  }

  .bad {
    color: var(--danger);
  }

  .good {
    color: var(--leaf);
  }

  .issues-link {
    padding: 0;
    border: 0;
    background: none;
    font: inherit;
    text-decoration: underline;
    text-underline-offset: 3px;
  }

  .body {
    display: flex;
    flex: 1;
    flex-direction: column;
    min-height: 0;
    margin: 0 -16px;
    padding: 0 16px;
    overflow: auto;
  }

  .body > :global(.field + .field) {
    border-top: 1px solid var(--hairline);
  }

  .add-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px;
    padding: 12px 0 4px;
    border-top: 1px dashed var(--hairline);
  }

  .add-button {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    height: 24px;
    padding: 0 8px;
    border: 1px dashed var(--card-border);
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: 11px;
  }

  .code {
    flex: 1;
    min-height: 260px;
    padding: 12px 14px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    outline: none;
    background: var(--frame);
    color: var(--text);
    font-size: 12px;
    line-height: 19px;
    resize: none;
    tab-size: 2;
    white-space: pre;
  }

  .code:focus {
    border-color: var(--text);
    box-shadow: 0 0 0 3px var(--accent-ring);
  }

  .code.invalid {
    border-color: var(--danger);
    border-style: dashed;
  }

  .json-issues {
    display: flex;
    flex-direction: column;
    gap: 3px;
    margin: 8px 0 0;
    padding: 0;
    color: var(--danger);
    list-style: none;
  }

  .footer {
    display: flex;
    align-items: center;
    gap: 8px;
    padding-top: 10px;
    border-top: 1px solid var(--hairline);
  }

  .chips,
  .hint {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px;
    min-width: 0;
    margin-right: auto;
  }

  .chip {
    padding: 1px 6px;
    border: 1px solid var(--card-border);
    background: var(--card);
    font-size: 11px;
  }

  .chip.changed {
    border-color: color-mix(in srgb, var(--warning) 45%, var(--card-border));
    color: var(--warning);
  }

  .chip.added {
    border-color: color-mix(in srgb, var(--leaf) 45%, var(--card-border));
    color: var(--leaf);
  }

  .chip.removed {
    border-color: color-mix(in srgb, var(--danger) 45%, var(--card-border));
    color: var(--danger);
  }

  @media (hover: hover) and (pointer: fine) {
    .add-button:hover {
      border-color: var(--text);
      color: var(--text);
    }
  }
</style>
