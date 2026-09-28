<script>
  import { api, collectionPath, send } from "./lib/api.js";
  import { formatScope, idParam, isTypedKind, plural } from "./lib/format.js";
  import {
    ArrowDown,
    ArrowUp,
    ArrowUpRight,
    Box,
    Braces,
    Calendar,
    Check,
    ChevronLeft,
    ChevronRight,
    CircleDashed,
    Copy,
    Database,
    GitBranch,
    Hash,
    List,
    Plus,
    SearchX,
    Tag,
    ToggleLeft,
    Type,
    X,
  } from "./lib/icons.js";
  import { keyToRowAction, moveSelection } from "./lib/interaction.ts";
  import { altHeld } from "./lib/modifiers.js";
  import { labels, requestLabel } from "./lib/labels.js";
  import { quiet } from "./lib/motion.ts";
  import { FIELD_FAMILY_LABEL, fieldFamily } from "./lib/values.ts";
  import { conditionParams, isComplete } from "./lib/query.ts";
  import { requiredDefaults } from "./lib/form-model.ts";
  import { useStudio } from "./lib/studio.js";
  import { nestedPaths } from "../field-paths.ts";
  import Icon from "./Icon.svelte";
  import QueryConditionToken from "./QueryConditionToken.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import Combobox from "./controls/Combobox.svelte";
  import Select from "./controls/Select.svelte";
  import Spectrum from "./controls/Spectrum.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import Value from "./values/Value.svelte";
  import JsonDrawer from "./JsonDrawer.svelte";
  import DocumentCard from "./DocumentCard.svelte";
  import EmptyState from "./EmptyState.svelte";
  import ErrorState from "./ErrorState.svelte";
  import JsonPage from "./JsonPage.svelte";
  import Segmented from "./controls/Segmented.svelte";

  let { collection, schema, type, initialScope = "", initialOpen = "" } = $props();

  const PAGE_SIZES = [25, 50, 100, 200];
  const typed = isTypedKind(collection.kind);
  const scoped = collection.kind === "scopedMultiCollection";

  let conditions = $state([]);
  let nextConditionId = 1;
  let focusCondition = $state(null);
  let sortField = $state("_id");
  let sortDir = $state("asc");
  let scope = $state(initialScope);
  let scopeDraft = $state(initialScope);
  let scopes = $state(null);
  let anchor = $state({});
  let limit = $state(50);
  let page = $state(null);
  let loading = $state(false);
  let error = $state(null);
  let openId = $state(initialOpen || null);
  let focusPath = $state([]);
  let creating = $state(null);

  const studio = useStudio();
  const canCreate = $derived(
    studio.write.enabled &&
      collection.kind !== "undeclared" &&
      collection.kind !== "internal" &&
      collection.kind !== "multiModelInstance" &&
      (!typed || Boolean(type)) &&
      (!scoped || Boolean(scope)),
  );

  function newDocument() {
    openId = null;
    creating = { type: typed ? type : "", scope: scoped ? scope : "", template: requiredDefaults(fieldMeta) };
  }

  function openRow(item, path = []) {
    openId = idParam(item._id);
    focusPath = path;
    const index = page?.items.indexOf(item) ?? -1;
    if (index >= 0) selected = index;
  }
  let requestSeq = 0;

  const schemaTypes = $derived(type ? schema.types.filter((t) => t.name === type) : schema.types);

  const fieldMeta = $derived.by(() => {
    const meta = {};
    for (const t of schemaTypes) {
      for (const [name, node] of Object.entries(t.fields)) meta[name] ??= node;
    }
    return meta;
  });

  const columns = $derived.by(() => {
    const cols = ["_id"];
    if (typed && !type) cols.push("_type");
    if (scoped && !scope) cols.push("_scope");
    for (const name of Object.keys(fieldMeta)) {
      if (!cols.includes(name)) cols.push(name);
    }
    if (schema.types.length === 0) {
      for (const item of page?.items ?? []) {
        for (const key of Object.keys(item)) {
          if (!cols.includes(key) && cols.length < 40) cols.push(key);
        }
      }
    }
    return cols;
  });

  const HEAD_COLUMNS = ["_id", "_type", "_scope"];

  const tableColumns = $derived.by(() => {
    const items = page?.items ?? [];
    if (!typed || type || items.length === 0) return columns;
    const present = new Set(items.map((item) => item._type));
    const allowed = new Set();
    for (const t of schema.types) {
      if (present.has(t.name)) for (const name of Object.keys(t.fields)) allowed.add(name);
    }
    return columns.filter(
      (column) =>
        HEAD_COLUMNS.includes(column) ||
        (allowed.has(column) && items.some((item) => item[column] !== undefined)),
    );
  });

  const hiddenColumns = $derived(columns.length - tableColumns.length);

  const MAX_TYPE_SECTIONS = 6;

  function sectionColumns(typeName, rows) {
    const own = schema.types.find((t) => t.name === typeName)?.fields ?? {};
    const head = ["_id"];
    if (scoped && !scope) head.push("_scope");
    return [
      ...head,
      ...columns.filter(
        (column) =>
          !HEAD_COLUMNS.includes(column) &&
          column in own &&
          rows.some(({ item }) => item[column] !== undefined),
      ),
    ];
  }

  const sections = $derived.by(() => {
    const items = page?.items ?? [];
    const whole = [{ key: "all", type: null, rows: items.map((item, index) => ({ item, index })), columns: tableColumns }];
    if (!typed || type || items.length === 0) return whole;
    const runs = [];
    items.forEach((item, index) => {
      const last = runs[runs.length - 1];
      if (last && last.type === item._type) last.rows.push({ item, index });
      else runs.push({ type: item._type, rows: [{ item, index }] });
    });
    if (runs.length < 2 || runs.length > MAX_TYPE_SECTIONS) return whole;
    return runs.map((run, position) => ({
      key: `${run.type}#${position}`,
      type: run.type,
      rows: run.rows,
      columns: sectionColumns(run.type, run.rows),
    }));
  });

  const VIEW_KEY = "mongodbee-studio:data-view";
  const VIEWS = [
    { id: "table", label: "Table" },
    { id: "documents", label: "Documents" },
    { id: "json", label: "JSON" },
  ];

  function readView() {
    try {
      const stored = window.localStorage.getItem(VIEW_KEY);
      return VIEWS.some((v) => v.id === stored) ? stored : "table";
    } catch {
      return "table";
    }
  }

  let view = $state(readView());
  let copiedPage = $state(false);

  function setView(next) {
    view = next;
    try {
      window.localStorage.setItem(VIEW_KEY, next);
    } catch {
      return;
    }
  }

  let undo = $state(null);
  let undoTimer;
  let undoing = $state(false);
  let undoError = $state(null);

  function showUndo(deleted) {
    clearTimeout(undoTimer);
    undoError = null;
    undo = deleted.token ? deleted : null;
    if (undo) undoTimer = setTimeout(() => (undo = null), 12000);
  }

  async function restoreDeleted() {
    if (!undo) return;
    clearTimeout(undoTimer);
    undoing = true;
    undoError = null;
    try {
      const result = await send("POST", collectionPath(collection.name, "restore"), { token: undo.token });
      undo = null;
      await fetchPage(params);
      openId = JSON.stringify(result.id);
    } catch (e) {
      undoError = e;
    } finally {
      undoing = false;
    }
  }

  function fieldsOfItem(item) {
    const own = typeof item._type === "string" ? schema.types.find((t) => t.name === item._type) : undefined;
    return own?.fields ?? fieldMeta;
  }

  const pageJson = $derived(page ? JSON.stringify(page.items, null, 2) : "");

  async function copyPage() {
    try {
      await navigator.clipboard.writeText(pageJson);
      copiedPage = true;
      setTimeout(() => (copiedPage = false), 1200);
    } catch {
      copiedPage = false;
    }
  }

  const numeric = $derived.by(() => {
    const result = {};
    for (const column of columns) {
      const kind = fieldMeta[column]?.kind;
      if (kind) {
        result[column] = kind === "number" || kind === "bigint";
        continue;
      }
      const values = (page?.items ?? []).map((item) => item[column]).filter((v) => v !== undefined && v !== null);
      result[column] = values.length > 0 && values.every((v) => typeof v === "number");
    }
    return result;
  });

  const filterable = $derived(columns.filter((c) => c !== "_id" && !(c === "_type" && type)));

  const FAMILY_ICON = {
    identity: Box,
    reference: ArrowUpRight,
    text: Type,
    number: Hash,
    date: Calendar,
    boolean: ToggleLeft,
    choice: Tag,
    list: List,
    object: Braces,
    other: CircleDashed,
  };

  const FAMILY_ORDER = Object.keys(FAMILY_ICON);

  const nested = $derived(nestedPaths(fieldMeta));

  const pathMeta = $derived({
    ...Object.fromEntries(nested.map(({ path, node }) => [path, node])),
    ...fieldMeta,
  });

  const fieldOptions = $derived([
    ...filterable
      .map((column) => {
        const node = fieldMeta[column];
        const family = fieldFamily(column, node);
        return {
          value: column,
          label: column,
          group: FIELD_FAMILY_LABEL[family],
          icon: column === "_scope" ? GitBranch : FAMILY_ICON[family],
          hint: node?.ref ? `→ ${node.ref}` : undefined,
          order: FAMILY_ORDER.indexOf(family),
        };
      })
      .sort((a, b) => a.order - b.order),
    ...nested.map(({ path, node }) => ({
      value: path,
      label: path,
      group: "Nested fields",
      icon: FAMILY_ICON[fieldFamily(path, node)],
      hint: node?.ref ? `→ ${node.ref}` : undefined,
    })),
  ]);

  const scopeOptions = $derived(
    (scopes?.top ?? []).map((item) => {
      const id = formatScope(item.scope);
      const named = $labels[id];
      return {
        value: id,
        label: named ? named.label : id,
        hint: named ? `${plural(item.count, "document")}, ${id}` : plural(item.count, "document"),
      };
    }),
  );

  $effect(() => {
    for (const item of scopes?.top ?? []) requestLabel(formatScope(item.scope));
  });

  const sortOptions = $derived([
    { value: "_id", label: "_id", group: FIELD_FAMILY_LABEL.identity, icon: Box },
    ...fieldOptions.filter((option) => option.value !== "_id"),
  ]);

  const pageSizeOptions = PAGE_SIZES.map((size) => ({ value: size, label: `${size} per page` }));

  const activeConditions = $derived(conditionParams(conditions));

  const params = $derived.by(() => ({
    type,
    scope,
    limit,
    after: anchor.after,
    before: anchor.before,
    offset: anchor.offset,
    w: activeConditions,
    sort: sortField === "_id" ? undefined : sortField,
    dir: sortDir === "desc" ? "desc" : undefined,
    count: 1,
  }));

  function addCondition() {
    const incomplete = conditions.find((condition) => !isComplete(condition));
    if (incomplete) {
      focusCondition = incomplete.id;
      return;
    }
    const id = nextConditionId++;
    conditions = [...conditions, { id, field: "", op: "eq", value: "" }];
    focusCondition = id;
  }

  function changeCondition(next) {
    conditions = conditions.map((condition) => (condition.id === next.id ? next : condition));
    anchor = {};
  }

  function removeCondition(id) {
    conditions = conditions.filter((condition) => condition.id !== id);
    anchor = {};
  }

  function clearConditions() {
    conditions = [];
    anchor = {};
  }

  function suggestValues(conditionId, field, text) {
    return api(collectionPath(collection.name, "values"), {
      type,
      scope,
      w: conditionParams(conditions.filter((condition) => condition.id !== conditionId)),
      field,
      q: text,
      limit: 30,
    });
  }

  function setSort(field, dir = sortDir) {
    sortField = field || "_id";
    sortDir = dir;
    anchor = {};
  }

  function toggleSort(column) {
    if (sortField !== column) {
      setSort(column, "asc");
      return;
    }
    if (sortDir === "asc") {
      setSort(column, "desc");
      return;
    }
    setSort("_id", "asc");
  }

  function goFirst() {
    anchor = {};
  }

  function goPrevious() {
    if (!page) return;
    if (page.sort?.paging === "offset") {
      anchor = { offset: Math.max(0, (page.offset ?? 0) - limit) };
    } else if (page.firstId) {
      anchor = { before: page.firstId };
    }
  }

  function goNext() {
    if (!page) return;
    if (page.sort?.paging === "offset") {
      anchor = { offset: (page.offset ?? 0) + limit };
    } else if (page.lastId) {
      anchor = { after: page.lastId };
    }
  }

  const resultCount = $derived.by(() => {
    const total = page?.total;
    if (!total) return null;
    if (total.timedOut) return "count timed out";
    return `${total.capped ? "at least " : ""}${plural(total.value, "document")}`;
  });

  $effect(() => {
    fetchPage(params);
  });

  $effect(() => {
    if (!scoped || !collection.exists) return;
    api(collectionPath(collection.name, "scopes"), { limit: 100 })
      .then((result) => (scopes = result))
      .catch(() => (scopes = { top: [], distinct: 0 }));
  });

  async function fetchPage(query) {
    const seq = ++requestSeq;
    loading = true;
    error = null;
    try {
      const result = await api(collectionPath(collection.name, "documents"), query);
      if (seq === requestSeq) page = result;
    } catch (e) {
      if (seq === requestSeq) {
        error = e;
        page = null;
      }
    } finally {
      if (seq === requestSeq) loading = false;
    }
  }

  function applyScope(value) {
    scope = value;
    scopeDraft = value;
    anchor = {};
  }

  let selected = $state(null);
  let slow = $state(false);
  let tableBody = $state();
  let copiedRow = $state(null);

  $effect(() => {
    if (!loading) {
      slow = false;
      return;
    }
    const timer = setTimeout(() => (slow = true), 300);
    return () => clearTimeout(timer);
  });

  $effect(() => {
    const count = page?.items.length ?? 0;
    if (selected !== null && selected >= count) selected = count > 0 ? count - 1 : null;
  });

  function select(index) {
    selected = index;
    const row = tableBody?.querySelector(`[data-row-index="${index}"]`) ?? tableBody?.children[index];
    row?.scrollIntoView({ block: "nearest" });
  }

  function typingTarget(target) {
    const tag = target?.tagName;
    return tag === "INPUT" || tag === "SELECT" || tag === "TEXTAREA" || target?.isContentEditable;
  }

  function onKey(event) {
    if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.altKey) return;
    if (document.querySelector("[data-palette]")) return;
    if (event.key === "/" && !typingTarget(event.target)) {
      event.preventDefault();
      addCondition();
      return;
    }
    if (typingTarget(event.target)) return;
    const action = keyToRowAction(event.key);
    const items = page?.items ?? [];
    if (action) {
      event.preventDefault();
      const next = moveSelection(selected, items.length, action);
      if (next !== null) {
        select(next);
        if (openId) openRow(items[next]);
      }
      return;
    }
    if (event.key === "Enter" && selected !== null && items[selected]) {
      event.preventDefault();
      quiet();
      openRow(items[selected]);
    }
  }

  async function copyId(item) {
    try {
      const id = typeof item._id === "string" ? item._id : JSON.stringify(item._id);
      await navigator.clipboard.writeText(id);
      copiedRow = idParam(item._id);
      setTimeout(() => (copiedRow = null), 1200);
    } catch {
      copiedRow = null;
    }
  }

  function headerTitle(column) {
    const node = fieldMeta[column];
    if (!node) return column;
    const parts = [node.kind.replaceAll("_", " ")];
    if (node.ref) parts.push(`reference to ${node.ref}`);
    if (node.optional) parts.push("optional");
    if (node.nullable) parts.push("nullable");
    return `${column}: ${parts.join(", ")}`;
  }
</script>

<div class="bezel frame">
<div class="bezel-card card">
<div class="query" role="search" aria-label="Query">
  <div class="sentence">
    <Tooltip text="{typed ? 'Type' : 'Collection'} {type || collection.name}">
      {#if type}
        <TypeTag name={type} />
      {:else}
        <span class="subject mono">{collection.name}</span>
      {/if}
    </Tooltip>
    {#if scoped}
      <span class="word">in</span>
      <Combobox
        bind:value={scopeDraft}
        options={scopeOptions}
        label="Scope"
        placeholder="every scope"
        free
        mono
        width="230px"
        empty="No scope recorded yet"
        onchange={(next) => applyScope(next.trim())}
      />
      {#if scope}
        <button class="clear-inline press" type="button" aria-label="Show every scope" onclick={() => applyScope("")}>
          <Icon icon={X} size={12} />
        </button>
      {/if}
    {/if}
    {#each conditions as condition, index (condition.id)}
      <span class="clause">
        <span class="word">{index === 0 ? "where" : "and"}</span>
        <QueryConditionToken
          {condition}
          {fieldOptions}
          fieldMeta={pathMeta}
          autofocus={focusCondition === condition.id}
          suggest={(field, text) => suggestValues(condition.id, field, text)}
          onchange={changeCondition}
          onremove={() => removeCondition(condition.id)}
        />
      </span>
    {/each}
    <Tooltip text="Add a condition" kbd="/">
      <button class="add press" type="button" onclick={addCondition} disabled={filterable.length === 0}>
        <Icon icon={Plus} size={13} />
        <span>{conditions.length === 0 ? "where" : "and"}</span>
      </button>
    </Tooltip>
  </div>
  <div class="order">
    <span class="word">sorted by</span>
    <Combobox
      value={sortField}
      options={sortOptions}
      label="Sort by"
      mono
      width="140px"
      empty="No field matches"
      onchange={(field) => setSort(field)}
    />
    <Tooltip text={sortDir === "asc" ? "Ascending, click for descending" : "Descending, click for ascending"}>
      <button
        class="direction press"
        type="button"
        aria-label={sortDir === "asc" ? "Ascending" : "Descending"}
        onclick={() => setSort(sortField, sortDir === "asc" ? "desc" : "asc")}
      >
        <Icon icon={sortDir === "asc" ? ArrowUp : ArrowDown} size={13} />
      </button>
    </Tooltip>
  </div>
</div>
<div class="query-foot">
  <span class="count num" class:faint={!resultCount}>
    {#if slow}
      <Spectrum size={5} gap={1} loading />
    {/if}
    {resultCount ?? " "}
  </span>
  <span class="foot-actions">
    {#if conditions.length > 0}
      <button class="text-button press" type="button" onclick={clearConditions}>Clear conditions</button>
    {/if}
    {#if view === "table" && hiddenColumns > 0}
      <Tooltip text="Fields of types that are not on this page, or empty on every row of it, are left out">
        <span class="faint small">{plural(hiddenColumns, "field")} hidden</span>
      </Tooltip>
    {/if}
    {#if view === "json" && page}
      <button class="text-button press" type="button" onclick={copyPage}>{copiedPage ? "Copied" : "Copy page"}</button>
    {/if}
    <Segmented items={VIEWS} value={view} label="Show documents as" size="sm" onselect={setView} />
    {#if canCreate}
      <button class="control new-document" type="button" onclick={newDocument}>
        <Icon icon={Plus} size={13} />
        New {type || "document"}
      </button>
    {/if}
  </span>
</div>

{#if error}
  <div class="pad">
    <ErrorState
      {error}
      compact
      title={error?.status === 400 ? "The query was refused" : "The documents could not be read"}
      onretry={() => fetchPage(params)}
    >
      {#if error?.status === 400 && conditions.length > 0}
        <button class="control" type="button" onclick={clearConditions}>Clear conditions</button>
      {/if}
    </ErrorState>
  </div>
{/if}

<div class="scroller" class:loading={slow} class:padded={view === "documents"}>
  {#if view === "documents"}
    <div class="cards" bind:this={tableBody}>
      {#each page?.items ?? [] as item, index (idParam(item._id))}
        <DocumentCard
          {item}
          fields={fieldsOfItem(item)}
          selected={selected === index}
          open={openId === idParam(item._id)}
          onopen={(path) => openRow(item, path)}
        />
      {/each}
    </div>
  {:else if view === "json"}
    <JsonPage
      items={page?.items ?? []}
      {selected}
      {openId}
      onopen={(item) => openRow(item)}
      bind:list={tableBody}
    />
  {:else}
  <div class="sections" bind:this={tableBody}>
  {#each sections as section (section.key)}
  {#if section.type}
    <div class="section-head">
      <TypeTag name={section.type} />
      <span class="faint small">{plural(section.rows.length, "document")} on this page</span>
    </div>
  {/if}
  <table class="table data" class:reveal={$altHeld}>
    <thead>
      <tr>
        {#each section.columns as column (column)}
          <th
            class:num={numeric[column]}
            aria-sort={sortField === column ? (sortDir === "asc" ? "ascending" : "descending") : undefined}
          >
            <Tooltip text={headerTitle(column)}>
              <button class="head" class:sorted={sortField === column} type="button" onclick={() => toggleSort(column)}>
                <span class="mono">{column}</span>
                {#if fieldMeta[column]?.ref}
                  <span class="ref-target mono">{fieldMeta[column].ref}</span>
                {/if}
                {#if sortField === column}
                  <span class="sort-mark"><Icon icon={sortDir === "asc" ? ArrowUp : ArrowDown} size={11} /></span>
                {/if}
              </button>
            </Tooltip>
          </th>
        {/each}
        <th class="actions-head" aria-label="Row actions"></th>
      </tr>
    </thead>
    <tbody>
      {#each section.rows as { item, index } (idParam(item._id))}
        <tr
          class="row"
          class:open={openId === idParam(item._id)}
          class:selected={selected === index}
          aria-selected={selected === index}
          data-row-index={index}
          onclick={() => openRow(item)}
        >
          {#each section.columns as column (column)}
            {@const own = fieldsOfItem(item)}
            <td class:num={numeric[column]}>
              {#if !HEAD_COLUMNS.includes(column) && own !== fieldMeta && own[column] === undefined && item[column] === undefined}
                <span class="not-in-type" aria-label="Not a field of {item._type}"></span>
              {:else}
                <Value
                  value={item[column]}
                  node={own[column] ?? fieldMeta[column]}
                  field={column}
                  reveal
                  onopen={() => openRow(item, [column])}
                />
              {/if}
            </td>
          {/each}
          <td class="actions">
            <span class="actions-inner">
              <Tooltip text={copiedRow === idParam(item._id) ? "Copied" : "Copy id"}>
                <button
                  class="row-action press"
                  aria-label="Copy id"
                  onclick={(event) => {
                    event.stopPropagation();
                    copyId(item);
                  }}
                >
                  <Icon icon={copiedRow === idParam(item._id) ? Check : Copy} size={13} />
                </button>
              </Tooltip>
              <Tooltip text="Open document" kbd="↵">
                <button
                  class="row-action press"
                  aria-label="Open document"
                  onclick={(event) => {
                    event.stopPropagation();
                    openRow(item);
                  }}
                >
                  <Icon icon={ChevronRight} size={13} />
                </button>
              </Tooltip>
            </span>
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
  {/each}
  </div>
  {/if}
  {#if page && page.items.length === 0}
    {#if activeConditions.length > 0 || scope}
      <EmptyState
        icon={SearchX}
        title="Nothing matches"
        hint={`No document matches ${[
          activeConditions.length > 0 ? `the ${plural(activeConditions.length, "condition")}` : "",
          scope ? `the scope ${$labels[scope]?.label ?? scope}` : "",
        ]
          .filter(Boolean)
          .join(" in ")}.`}
      >
        {#if activeConditions.length > 0}
          <button class="control" type="button" onclick={clearConditions}>Clear conditions</button>
        {/if}
        {#if scope}
          <button class="control" type="button" onclick={() => applyScope("")}>Show every scope</button>
        {/if}
      </EmptyState>
    {:else if !collection.exists}
      <EmptyState
        icon={Database}
        title="Not created yet"
        hint="The collection is declared in the schemas but does not exist in the database; mongodbee migrate creates it."
      />
    {:else}
      <EmptyState
        icon={Database}
        title={type ? `No ${type} yet` : "This collection is empty"}
        hint={type ? "No document of this type is stored." : "No document is stored."}
      />
    {/if}
  {/if}
</div>
</div>

<footer class="bezel-foot pager">
  <span class="left">
    <span class="muted small num">
      {#if slow}
        <span class="shimmer">Loading rows</span>
      {:else if page}
        {plural(page.items.length, "row")}
      {/if}
    </span>
    <span class="hints" aria-label="Keyboard shortcuts">
      <span class="hint" class:active={$altHeld}><kbd class="kbd">⌥</kbd> reveal</span>
      <span class="hint"><kbd class="kbd">J</kbd><kbd class="kbd">K</kbd> move</span>
      <span class="hint"><kbd class="kbd">↵</kbd> open</span>
      <span class="hint"><kbd class="kbd">/</kbd> filter</span>
      {#if conditions.length > 0}<span class="hint"><kbd class="kbd">ctrl</kbd><kbd class="kbd">space</kbd> suggest</span>{/if}
      <span class="hint"><kbd class="kbd">⌘K</kbd> jump</span>
    </span>
  </span>
  <div class="pager-actions">
    <Select bind:value={limit} options={pageSizeOptions} label="Rows per page" size="sm" onchange={() => (anchor = {})} />
    <button class="control" onclick={goFirst} disabled={!page?.hasPrevious || loading}>First</button>
    <button class="control" onclick={goPrevious} disabled={!page?.hasPrevious || loading}>
      <Icon icon={ChevronLeft} size={14} />
      Previous
    </button>
    <button class="control" onclick={goNext} disabled={!page?.hasNext || loading}>
      Next
      <Icon icon={ChevronRight} size={14} />
    </button>
  </div>
</footer>
</div>

{#if openId || creating}
  <JsonDrawer
    collection={collection.name}
    id={openId}
    {focusPath}
    fields={fieldMeta}
    writable={studio.write.enabled && collection.kind !== "multiModelInstance" && collection.kind !== "undeclared"}
    create={creating}
    fieldsFor={(name) => (name ? (schema.types.find((t) => t.name === name)?.fields ?? {}) : (schema.types[0]?.fields ?? {}))}
    onchanged={() => fetchPage(params)}
    oncreated={(id) => {
      creating = null;
      openId = id;
    }}
    ondeleted={(deleted) => showUndo(deleted)}
    onclose={() => {
      openId = null;
      creating = null;
      focusPath = [];
    }}
  />
{/if}

{#if undo}
  <div class="undo-toast" role="status">
    <span class="small">Deleted <span class="mono">{undo.label}</span></span>
    {#if undoError}<span class="small bad">{undoError.message}</span>{/if}
    <button class="control" type="button" onclick={restoreDeleted} disabled={undoing}>
      {undoing ? "Restoring" : "Undo"}
    </button>
    <button class="control quiet icon" type="button" aria-label="Dismiss" onclick={() => (undo = null)}>
      <Icon icon={X} size={13} />
    </button>
  </div>
{/if}

<svelte:window onkeydown={onKey} />

<style>
  .left {
    display: flex;
    align-items: center;
    gap: 20px;
    min-width: 0;
  }

  .hints {
    display: flex;
    align-items: center;
    gap: 14px;
    color: var(--text-faint);
    font-size: 11.5px;
    white-space: nowrap;
  }

  .hint {
    display: inline-flex;
    align-items: center;
    gap: 3px;
  }

  .hint.active {
    color: var(--text);
  }

  .hint.active .kbd {
    border-color: var(--accent);
    color: var(--accent);
  }

  .actions-head,
  .actions {
    position: sticky;
    right: 0;
    width: 1px;
    padding: 0 8px 0 0;
    border-bottom-color: var(--hairline);
  }

  .actions-head {
    z-index: 2;
  }

  .actions {
    z-index: 1;
  }

  .actions-inner {
    display: inline-flex;
    gap: 2px;
    padding-left: 18px;
    background: linear-gradient(to right, transparent, var(--bg-hover) 18px);
    opacity: 0;
  }

  .row.selected .actions-inner,
  .row:focus-within .actions-inner {
    opacity: 1;
  }

  .row.selected .actions-inner,
  .row.open .actions-inner {
    background: linear-gradient(to right, transparent, var(--select-bg) 18px);
  }

  .row-action {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    padding: 0;
    border: 1px solid transparent;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-muted);
    transition:
      background-color var(--dur-quick) var(--ease),
      border-color var(--dur-quick) var(--ease),
      transform var(--dur-press) var(--ease-out);
  }

  .row.selected td {
    background: var(--select-bg);
  }

  .row.selected td:first-child {
    box-shadow: inset 2px 0 0 var(--select-edge);
  }

  .data td:not(.actions) {
    position: relative;
  }

  .row {
    --row-bg: var(--card);
  }

  @media (hover: hover) and (pointer: fine) {
    .row:hover {
      --row-bg: var(--bg-hover);
    }

    .row:hover .actions-inner {
      opacity: 1;
    }

    .row-action:hover {
      border-color: var(--card-border);
      background: var(--card);
      color: var(--text);
    }

    .add:hover:not(:disabled) {
      border-color: var(--text-faint);
      color: var(--text);
    }

    .clear-inline:hover,
    .direction:hover {
      background: var(--bg-active);
      color: var(--text);
    }

    .head:hover {
      color: var(--text);
    }

    .text-button:hover {
      color: var(--text);
    }
  }

  .row.selected,
  .row.open {
    --row-bg: var(--select-bg);
  }

  .query {
    display: flex;
    flex: none;
    flex-wrap: wrap;
    align-items: flex-start;
    justify-content: space-between;
    gap: 8px 20px;
    padding: 10px 14px 6px;
  }

  .sentence,
  .order {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px 8px;
    min-height: 32px;
  }

  .sentence {
    flex: 1;
    min-width: 0;
  }

  .clause {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }

  .word {
    color: var(--text-faint);
    font-family: var(--font-mono);
    font-size: 11.5px;
  }

  .subject {
    display: inline-flex;
    align-items: center;
    height: 24px;
    padding: 0 8px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    background: var(--frame);
    color: var(--text);
  }

  .add {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    height: 28px;
    padding: 0 10px 0 8px;
    border: 1px dashed var(--card-border);
    border-radius: var(--radius-card);
    background: none;
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: 11.5px;
    transition:
      border-color var(--dur-quick) var(--ease),
      color var(--dur-quick) var(--ease),
      transform var(--dur-press) var(--ease-out);
  }

  .clear-inline,
  .direction {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    padding: 0;
    border: 1px solid transparent;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-faint);
    transition:
      background-color var(--dur-quick) var(--ease),
      transform var(--dur-press) var(--ease-out);
  }

  .direction {
    border-color: var(--card-border);
    background: var(--card);
    color: var(--text);
  }

  .query-foot {
    display: flex;
    flex: none;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    min-height: 26px;
    padding: 0 16px 8px;
    border-bottom: 1px solid var(--hairline);
  }

  .count {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: var(--text-muted);
    font-size: var(--text-xs);
  }

  .text-button {
    padding: 0;
    border: 0;
    background: none;
    color: var(--text-muted);
    font-size: var(--text-xs);
    text-decoration: underline;
    text-decoration-color: var(--card-border);
    text-underline-offset: 3px;
  }

  .small {
    font-size: var(--text-xs);
  }

  .scroller {
    flex: 1;
    min-height: 0;
    overflow: auto;
    transition: opacity var(--dur-base) var(--ease-enter);
  }

  .scroller.loading {
    opacity: 0.5;
  }

  .scroller.padded {
    background: var(--frame);
  }

  .foot-actions {
    display: inline-flex;
    align-items: center;
    gap: 12px;
  }

  .cards {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 8px 12px 12px;
  }

  .sections {
    min-width: 100%;
  }

  .section-head {
    position: sticky;
    left: 0;
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 14px 16px 6px;
    border-top: 1px solid var(--hairline);
    background: var(--frame);
  }

  .sections > .section-head:first-child {
    border-top: 0;
  }

  .undo-toast {
    position: fixed;
    bottom: 20px;
    left: 50%;
    z-index: 30;
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 6px 6px 14px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-card);
    background: var(--ink-panel);
    color: var(--card);
    box-shadow: var(--shadow-overlay);
    translate: -50% 0;
  }

  .undo-toast .mono {
    color: var(--card);
  }

  .undo-toast .bad {
    color: color-mix(in srgb, var(--danger) 60%, white);
  }

  .undo-toast :global(.control.quiet) {
    color: color-mix(in srgb, var(--card) 70%, transparent);
  }

  .not-in-type {
    display: block;
    width: 100%;
    min-width: 24px;
    height: 12px;
    background: repeating-linear-gradient(
      135deg,
      transparent 0 4px,
      var(--hairline) 4px 5px
    );
  }

  .data td {
    max-width: 320px;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .data th:first-child,
  .data td:first-child {
    padding-left: 16px;
  }

  .head {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 0;
    border: 0;
    background: none;
    color: inherit;
    font: inherit;
  }

  .head.sorted {
    color: var(--text);
  }

  .sort-mark {
    display: inline-flex;
    color: var(--text);
  }

  .head .mono {
    font-size: var(--text-xs);
  }

  .ref-target {
    color: var(--text-faint);
  }

  .ref-target::before {
    content: "→ ";
  }

  .row {
    cursor: pointer;
  }

  .row.open {
    background: var(--select-bg);
  }

  .pad {
    padding: 12px 16px 0;
  }

  .frame {
    flex: 1;
  }

  .card {
    flex: 1;
  }

  .pager-actions {
    display: flex;
    align-items: center;
    gap: 6px;
  }
</style>
