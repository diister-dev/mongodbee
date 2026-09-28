<script>
  import FieldInput from "./FieldInput.svelte";
  import Icon from "./Icon.svelte";
  import RefInput from "./RefInput.svelte";
  import BoolToggle from "./controls/BoolToggle.svelte";
  import Combobox from "./controls/Combobox.svelte";
  import EnumPicker from "./controls/EnumPicker.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import { Plus, X } from "./lib/icons.js";
  import { relativeTime } from "./lib/values.ts";
  import {
    checkHint,
    defaultFor,
    isDateValue,
    isObjectKind,
    issuesAt,
    issuesUnder,
    numberFromText,
    switchVariant,
    variantOption,
    variantTags,
  } from "./lib/form-model.ts";

  let {
    name,
    node,
    value,
    path,
    issues = [],
    onchange,
    onremove,
    removable = false,
    suggest,
    depth = 0,
  } = $props();

  const own = $derived(issuesAt(issues, path));
  const nested = $derived(issuesUnder(issues, path));
  const hint = $derived(checkHint(node));
  const isNull = $derived(value === null && node.nullable);
  const choices = $derived(node.values ?? (node.kind === "union" ? literalOptions(node) : undefined));
  const variant = $derived(node.kind === "variant" ? variantOption(node, value) : undefined);
  const group = $derived(!isNull && (isObjectKind(node) || node.kind === "variant" || node.kind === "array"));
  const label = $derived(typeof name === "number" ? `#${name + 1}` : String(name));
  const invalid = $derived(own.length > 0);

  let draft = $state("");
  let draftError = $state(null);
  let jsonDraft = $state("");
  let suggestions = $state([]);
  let looking = $state(false);
  let ticket = 0;

  function literalOptions(union) {
    const options = union.options ?? [];
    if (options.length > 0 && options.every((option) => option.literal !== undefined)) {
      return options.map((option) => option.literal);
    }
    return undefined;
  }

  const known = $derived(
    ["string", "number", "bigint", "boolean", "date", "picklist", "enum", "literal", "array", "variant"].includes(node.kind) ||
      isObjectKind(node) ||
      Boolean(choices),
  );

  const dateTime = $derived(isDateValue(value) ? Date.parse(value.$date) : null);

  $effect(() => {
    if (node.kind === "number" || node.kind === "bigint") draft = typeof value === "number" ? String(value) : "";
    if (node.kind === "date") draft = isDateValue(value) ? value.$date : "";
    if (!known) jsonDraft = JSON.stringify(value ?? null, null, 2);
  });

  function set(next) {
    onchange(path, next);
  }

  async function search(text) {
    if (!suggest) return;
    const mine = ++ticket;
    looking = true;
    try {
      const found = await suggest(path, text);
      if (mine === ticket) suggestions = found;
    } catch {
      if (mine === ticket) suggestions = [];
    } finally {
      if (mine === ticket) looking = false;
    }
  }

  function commitNumber() {
    if (draft.trim() === "") {
      draftError = "A number is required";
      return;
    }
    const parsed = numberFromText(draft);
    if (parsed === undefined) {
      draftError = "Not a number";
      return;
    }
    draftError = null;
    set(parsed);
  }

  function commitDate() {
    const time = Date.parse(draft);
    if (Number.isNaN(time)) {
      draftError = "Not a date: 2026-09-28 or 2026-09-28T14:30:00Z";
      return;
    }
    draftError = null;
    set({ $date: new Date(time).toISOString() });
  }

  function commitJson() {
    try {
      set(JSON.parse(jsonDraft));
      draftError = null;
    } catch (error) {
      draftError = error instanceof Error ? error.message : String(error);
    }
  }

  const entries = $derived(
    isObjectKind(node) ? Object.entries(node.entries ?? {}) : variant ? Object.entries(variant.entries ?? {}) : [],
  );
  const inValue = (key) => Boolean(value) && typeof value === "object" && key in value;
  const presentEntries = $derived(
    entries.filter(
      ([key, child]) => (!child.optional || inValue(key)) && !(node.kind === "variant" && key === node.discriminator),
    ),
  );
  const missingOptional = $derived(entries.filter(([key, child]) => child.optional && !inValue(key)));
  const extraKeys = $derived(
    (isObjectKind(node) || node.kind === "variant") && value && typeof value === "object" && !Array.isArray(value)
      ? Object.keys(value).filter((key) => !entries.some(([entry]) => entry === key))
      : [],
  );
</script>

<div class="field" class:group class:has-issues={invalid || draftError}>
  <div class="row">
    <Tooltip
      text={[hint ? `Expects ${hint}` : "", node.optional ? "optional" : "", node.nullable ? "can be null" : ""].filter(Boolean).join(", ")}
      disabled={!hint && !node.optional && !node.nullable}
    >
      <span class="name mono" class:optional={node.optional}>{label}</span>
    </Tooltip>

    <div class="cell">
      {#if isNull}
        <span class="null-value mono">null</span>
      {:else if choices}
        <EnumPicker {value} options={choices} field={String(name)} {label} {invalid} onchange={set} />
      {:else if node.kind === "literal"}
        <span class="literal mono">{JSON.stringify(node.literal)}</span>
      {:else if node.kind === "boolean"}
        <BoolToggle {value} {label} onchange={set} />
      {:else if node.kind === "string" && node.ref}
        <span class="ref-cell">
          <TypeTag name={node.ref} />
          <RefInput {value} target={node.ref} {label} {invalid} onchange={set} />
        </span>
      {:else if node.kind === "string"}
        <Combobox
          value={value ?? ""}
          options={suggestions}
          {label}
          placeholder={hint ?? ""}
          free
          width="100%"
          autoselect={false}
          openOnFocus={false}
          loading={looking}
          {invalid}
          empty="No value recorded yet"
          keyshortcuts="Control+Space"
          onsearch={search}
          ontext={set}
          onchange={set}
        />
      {:else if node.kind === "number" || node.kind === "bigint"}
        <input
          class="control text-input mono number"
          class:invalid={invalid || draftError}
          bind:value={draft}
          inputmode="decimal"
          placeholder={hint ?? "0"}
          aria-label={label}
          autocomplete="off"
          onblur={commitNumber}
          onkeydown={(event) => event.key === "Enter" && commitNumber()}
        />
      {:else if node.kind === "date"}
        <span class="inline">
          <input
            class="control text-input mono date"
            class:invalid={invalid || draftError}
            bind:value={draft}
            placeholder="2026-09-28T14:30:00.000Z"
            aria-label={label}
            spellcheck="false"
            autocomplete="off"
            onblur={commitDate}
            onkeydown={(event) => event.key === "Enter" && commitDate()}
          />
          {#if dateTime !== null && !Number.isNaN(dateTime)}
            <span class="relative faint small">{relativeTime(dateTime)}</span>
          {/if}
          <button
            class="mini-button press"
            type="button"
            onclick={() => {
              draft = new Date().toISOString();
              commitDate();
            }}
          >
            now
          </button>
        </span>
      {:else if node.kind === "variant"}
        <EnumPicker
          value={value?.[node.discriminator]}
          options={variantTags(node)}
          field="{String(name)}.{node.discriminator}"
          label="{label} {node.discriminator}"
          onchange={(tag) => set(switchVariant(node, value, tag))}
        />
      {:else if isObjectKind(node)}
        <span class="summary">{"{"} {Object.keys(value ?? {}).length} / {entries.length} {"}"}</span>
      {:else if node.kind === "array"}
        <span class="summary">[ {Array.isArray(value) ? value.length : 0} ]</span>
      {:else}
        <textarea
          class="json-input mono"
          class:invalid={invalid || draftError}
          bind:value={jsonDraft}
          rows={Math.min(6, Math.max(1, jsonDraft.split("\n").length))}
          aria-label="{label} as JSON"
          spellcheck="false"
          onblur={commitJson}
        ></textarea>
      {/if}
      {#if group && nested > 0}<span class="count-bad small">{nested} to fix</span>{/if}
    </div>

    <span class="actions">
      {#if node.nullable}
        <Tooltip text={isNull ? "Give it a value" : "Set to null"}>
          <button
            class="mini mono press"
            class:on={isNull}
            type="button"
            onclick={() => set(isNull ? defaultFor({ ...node, nullable: false }) : null)}
          >
            null
          </button>
        </Tooltip>
      {/if}
      {#if removable}
        <Tooltip text="Remove {label}">
          <button class="mini press" type="button" aria-label="Remove {label}" onclick={() => onremove(path)}>
            <Icon icon={X} size={12} />
          </button>
        </Tooltip>
      {/if}
    </span>

    {#if draftError || invalid}
      <div class="messages">
        {#if draftError}<p class="bad small">{draftError}</p>{/if}
        {#each own as message}<p class="bad small">{message}</p>{/each}
      </div>
    {/if}
  </div>

  {#if group}
    <div class="children">
      {#if node.kind === "array"}
        {#each Array.isArray(value) ? value : [] as item, index (index)}
          <FieldInput
            name={index}
            node={node.item ?? { kind: "unknown" }}
            value={item}
            path={[...path, index]}
            {issues}
            {onchange}
            {onremove}
            {suggest}
            removable
            depth={depth + 1}
          />
        {/each}
        <div class="add-row">
          <button
            class="add-button press"
            type="button"
            onclick={() => set([...(Array.isArray(value) ? value : []), defaultFor(node.item ?? { kind: "unknown" })])}
          >
            <Icon icon={Plus} size={11} /> item
          </button>
        </div>
      {:else}
        {#each presentEntries as [key, child] (key)}
          <FieldInput
            name={key}
            node={child}
            value={value?.[key]}
            path={[...path, key]}
            {issues}
            {onchange}
            {onremove}
            {suggest}
            removable={Boolean(child.optional)}
            depth={depth + 1}
          />
        {/each}
        {#each extraKeys as key (key)}
          <FieldInput
            name={key}
            node={{ kind: "unknown" }}
            value={value?.[key]}
            path={[...path, key]}
            {issues}
            {onchange}
            {onremove}
            removable
            depth={depth + 1}
          />
        {/each}
        {#if missingOptional.length > 0}
          <div class="add-row">
            {#each missingOptional as [key, child] (key)}
              <button class="add-button press" type="button" onclick={() => onchange([...path, key], defaultFor(child))}>
                <Icon icon={Plus} size={11} /> {key}
              </button>
            {/each}
          </div>
        {/if}
      {/if}
    </div>
  {/if}
</div>

<style>
  .field {
    min-width: 0;
  }

  .row {
    display: grid;
    grid-template-columns: minmax(84px, 30%) minmax(0, 1fr) auto;
    align-items: center;
    column-gap: 10px;
    min-height: 34px;
    padding: 2px 0;
  }

  .name {
    overflow: hidden;
    color: var(--text);
    font-size: 12px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .name.optional {
    color: var(--text-muted);
  }

  .has-issues > .row .name {
    color: var(--danger);
  }

  .cell {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
  }

  .inline,
  .ref-cell {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    min-width: 0;
  }

  .ref-cell :global(.ref-input) {
    flex: 1;
  }

  .text-input {
    width: 100%;
    height: 28px;
    padding: 0 9px;
    outline: none;
    font-size: 12.5px;
    text-align: left;
  }

  .text-input.number {
    max-width: 150px;
  }

  .text-input.date {
    max-width: 230px;
  }

  .cell :global(.combobox .control) {
    height: 28px;
    font-size: 12.5px;
  }

  .text-input:focus,
  .json-input:focus {
    border-color: var(--text);
    box-shadow: 0 0 0 3px var(--accent-ring);
  }

  .invalid {
    border-color: var(--danger);
  }

  .json-input {
    width: 100%;
    padding: 5px 9px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    outline: none;
    background: var(--card);
    color: var(--text);
    font-size: 12px;
    line-height: 17px;
    resize: vertical;
  }

  .relative {
    white-space: nowrap;
  }

  .mini-button {
    height: 24px;
    padding: 0 8px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    background: var(--card);
    color: var(--text-muted);
    font-size: 11.5px;
  }

  .summary,
  .null-value,
  .literal {
    color: var(--text-faint);
    font-family: var(--font-mono);
    font-size: 11.5px;
  }

  .count-bad {
    color: var(--danger);
    white-space: nowrap;
  }

  .actions {
    display: inline-flex;
    align-items: center;
    gap: 2px;
  }

  .mini {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    height: 22px;
    min-width: 22px;
    padding: 0 5px;
    border: 1px solid transparent;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-faint);
    font-size: 11px;
  }

  .mini.on {
    border-color: var(--card-border);
    background: var(--frame);
    color: var(--text);
  }

  .messages {
    grid-column: 2 / -1;
    padding-top: 3px;
  }

  .messages p {
    margin: 0;
  }

  .bad {
    color: var(--danger);
  }

  .small {
    font-size: var(--text-xs);
  }

  .children {
    margin: 0 0 4px 5px;
    padding-left: 10px;
    border-left: 1px solid var(--card-border);
  }

  .add-row {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    padding: 4px 0;
  }

  .add-button {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    height: 22px;
    padding: 0 7px;
    border: 1px dashed var(--card-border);
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: 11px;
  }

  @media (hover: hover) and (pointer: fine) {
    .add-button:hover,
    .mini:hover,
    .mini-button:hover {
      border-color: var(--text-faint);
      background: var(--bg-active);
      color: var(--text);
    }
  }
</style>
