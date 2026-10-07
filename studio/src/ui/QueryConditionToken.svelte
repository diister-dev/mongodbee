<script>
  import { tick } from "svelte";
  import Icon from "./Icon.svelte";
  import Combobox from "./controls/Combobox.svelte";
  import Select from "./controls/Select.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import { X } from "./lib/icons.js";
  import { defaultOperator, needsValue, OPERATOR_LABEL, operatorsFor } from "./lib/query.ts";
  import { fieldFamily, formatCount } from "./lib/values.ts";

  let { condition, fieldOptions, fieldMeta, onchange, onremove, autofocus = false, suggest } = $props();

  let fieldInput = $state();
  let valueInput = $state();
  let suggestions = $state([]);
  let looking = $state(false);
  let request = 0;
  let timer;

  const node = $derived(fieldMeta[condition.field]);
  const family = $derived(fieldFamily(condition.field, node));
  const operatorOptions = $derived(
    operatorsFor(family).map((op) => ({ value: op, label: OPERATOR_LABEL[op] })),
  );
  const choices = $derived.by(() => {
    const values = node?.values ?? (node?.kind === "array" ? node.item?.values : undefined);
    if (values) return values.map((value) => ({ value: String(value), label: String(value) }));
    if (node?.kind === "boolean") {
      return [
        { value: "true", label: "true" },
        { value: "false", label: "false" },
      ];
    }
    return undefined;
  });

  $effect(() => {
    condition.field;
    suggestions = [];
  });

  $effect(() => {
    if (autofocus) tick().then(() => fieldInput?.focus());
  });

  function optionValue(value) {
    if (typeof value === "string") return value;
    if (value && typeof value === "object" && "$date" in value) return String(value.$date);
    if (value && typeof value === "object") return JSON.stringify(value);
    return String(value);
  }

  function search(text) {
    if (!suggest || !condition.field) return;
    clearTimeout(timer);
    const ticket = ++request;
    looking = true;
    timer = setTimeout(
      () => {
        suggest(condition.field, text)
          .then((result) => {
            if (ticket !== request) return;
            suggestions = result.values.map(({ value, count }) => ({
              value: optionValue(value),
              label: optionValue(value),
              hint: `${formatCount(count)}${result.sampled ? "+" : ""}`,
            }));
          })
          .catch(() => {
            if (ticket === request) suggestions = [];
          })
          .finally(() => {
            if (ticket === request) looking = false;
          });
      },
      text ? 150 : 0,
    );
  }

  function update(patch) {
    onchange({ ...condition, ...patch });
  }

  function chooseField(field) {
    const nextFamily = fieldFamily(field, fieldMeta[field]);
    const op = operatorsFor(nextFamily).includes(condition.op) ? condition.op : defaultOperator(nextFamily);
    update({ field, op, value: "" });
    tick().then(() => valueInput?.focus());
  }

  function commit(value) {
    if (value !== condition.value) update({ value });
  }

  function onValueKey(event, text) {
    if (event.key === "Enter" || event.key === "Escape") {
      event.preventDefault();
      event.currentTarget.blur();
    }
    if (event.key === "Backspace" && text === "" && condition.value === "") {
      event.preventDefault();
      onremove();
    }
  }
</script>

<span class="token" class:incomplete={needsValue(condition.op) && condition.value === ""}>
  <Tooltip text="Field {condition.field || 'not chosen'}{node ? `, ${node.kind.replaceAll('_', ' ')}` : ''}">
    <Combobox
      value={condition.field}
      options={fieldOptions}
      label="Field"
      placeholder="field"
      mono
      width="150px"
      empty="No field matches"
      onchange={chooseField}
      bind:inputRef={fieldInput}
    />
  </Tooltip>
  <Select
    value={condition.op}
    options={operatorOptions}
    label="Operator"
    size="sm"
    onchange={(op) => update({ op, value: needsValue(op) ? condition.value : "" })}
  />
  {#if needsValue(condition.op)}
    {#if choices && condition.op !== "in"}
      <Select
        value={condition.value}
        options={[{ value: "", label: "choose" }, ...choices]}
        label="Value"
        size="sm"
        onchange={(value) => update({ value })}
      />
    {:else}
      <Combobox
        value={condition.value}
        options={condition.op === "in" && choices ? choices : suggestions}
        label="Value for {condition.field}"
        placeholder={condition.op === "in" ? "a, b, c" : family === "number" ? "0" : family === "date" ? "2026-01-31" : "value"}
        free
        mono
        width="170px"
        autoselect={false}
        loading={looking}
        empty="No recorded value"
        keyshortcuts="Control+Space"
        onsearch={search}
        onkeydown={onValueKey}
        onchange={commit}
        bind:inputRef={valueInput}
      />
    {/if}
  {/if}
  <button class="remove press" type="button" aria-label="Remove the condition on {condition.field}" onclick={onremove}>
    <Icon icon={X} size={12} />
  </button>
</span>

<style>
  .token {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    padding: 2px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-card);
    background: var(--frame);
  }

  .token.incomplete {
    border-style: dashed;
  }

  .token :global(.control) {
    height: 26px;
    border-color: transparent;
    background: var(--card);
  }

  .remove {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    padding: 0;
    border: 0;
    border-radius: var(--radius-control);
    background: none;
    color: var(--text-faint);
  }

  @media (hover: hover) and (pointer: fine) {
    .remove:hover {
      background: var(--bg-active);
      color: var(--text);
    }
  }
</style>
