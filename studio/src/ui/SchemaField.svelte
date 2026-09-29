<script>
  import SchemaField from "./SchemaField.svelte";
  import { ChevronRight } from "./lib/icons.js";
  import Icon from "./Icon.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import { fade, rise } from "./lib/motion.ts";
  import Enum from "./values/Enum.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import SystemTag from "./values/SystemTag.svelte";
  import { jsonSchemaChild, jsonSchemaFactDetails, jsonTypeOf, isRequired } from "./lib/json-schema.ts";
  import { formatCount } from "./lib/values.ts";

  let { name, node, depth = 0, json, parentJson, usage } = $props();

  function percent(ratio) {
    if (ratio === 0) return "0%";
    if (ratio >= 0.995) return "100%";
    const value = ratio * 100;
    return value < 1 ? "<1%" : `${Math.round(value)}%`;
  }

  const jsonType = $derived(jsonTypeOf(json));
  const jsonFacts = $derived(jsonSchemaFactDetails(json));
  const required = $derived(isRequired(parentJson, name));

  let open = $state(depth < 1);

  function typeLabel(n) {
    if (!n) return "unknown";
    if (n.ref) return `ref ${n.ref}`;
    if (n.kind === "array") return `${typeLabel(n.item)}[]`;
    if (n.kind === "literal") return JSON.stringify(n.literal);
    if (n.kind === "record") return `record<${typeLabel(n.key)}, ${typeLabel(n.value)}>`;
    if ((n.kind === "union" || n.kind === "variant") && n.options?.every((o) => !o.entries)) {
      return n.options.map(typeLabel).join(" | ");
    }
    if (n.kind === "variant") return `variant on ${n.discriminator}`;
    return n.kind.replaceAll("_", " ");
  }

  function checkLabel(check) {
    const label = check.type.replaceAll("_", " ");
    if (check.requirement === undefined) return label;
    if (check.type === "email" || check.type === "url") return label;
    const value = typeof check.requirement === "string" ? check.requirement : JSON.stringify(check.requirement);
    return `${label} ${value}`;
  }

  const element = $derived(node.kind === "array" ? node.item : node);

  const modifiers = $derived(
    [node.optional && "optional", node.nullable && "nullable", node.hasDefault && "default"].filter(Boolean),
  );

  const rules = $derived.by(() => {
    const list = [];
    for (const check of node.checks ?? []) list.push(checkLabel(check));
    if (element && element !== node) {
      for (const check of element.checks ?? []) list.push(`each ${checkLabel(check)}`);
    }
    if (node.index) {
      list.push(
        `${node.index.unique ? "unique index" : "index"}${node.index.global ? " across scopes" : ""}`,
      );
    }
    return list;
  });

  const values = $derived(node.values ?? element?.values);

  const children = $derived.by(() => {
    if (element?.entries) return Object.entries(element.entries);
    if ((element?.kind === "union" || element?.kind === "variant") && element.options?.some((o) => o.entries)) {
      return element.options.map((option, index) => {
        const tag = element.discriminator && option.entries?.[element.discriminator]?.literal;
        return [tag !== undefined ? `${element.discriminator} = ${JSON.stringify(tag)}` : `option ${index + 1}`, option];
      });
    }
    if (element?.kind === "record" && element.value?.entries) return [["[key]", element.value]];
    return [];
  });
</script>

<tr
  in:rise|global={{ from: "above", duration: depth > 0 ? 180 : 0 }}
  out:fade|global={{ duration: depth > 0 ? 100 : 0, exit: true }}
>
  <td>
    <div class="field">
    <span class="indent" style="width: {depth * 16}px"></span>
    {#if children.length > 0}
      <button class="toggle" onclick={() => (open = !open)} aria-expanded={open}>
        <span class="caret" class:open><Icon icon={ChevronRight} size={12} /></span>
        <span class="mono" class:system-name={node.system}>{name}</span>
      </button>
    {:else}
      <span class="spacer"></span>
      <span class="mono" class:system-name={node.system}>{name}</span>
    {/if}
    </div>
  </td>
  <td class="usage-cell">
    {#if usage && depth === 0}
      {@const ratio = usage.sampled > 0 ? (usage.fields[name] ?? 0) / usage.sampled : 0}
      <Tooltip
        text="{formatCount(usage.fields[name] ?? 0)} of {formatCount(usage.sampled)} {usage.sampled < usage.total ? 'sampled ' : ''}documents fill this field"
      >
        <span class="usage" class:unused={ratio === 0}>
          <span class="usage-track"><span class="usage-fill" style="--ratio: {ratio}"></span></span>
          <span class="usage-value num">{percent(ratio)}</span>
        </span>
      </Tooltip>
    {/if}
  </td>
  <td class="type-cell">
    <div class="type-line">
    {#if element?.ref}
      <span class="ref-type">
        <span class="mono type">ref</span>
        <TypeTag name={element.ref} title="References {element.ref}" />{#if element !== node}<span class="mono type"
            >[]</span
          >{/if}
      </span>
    {:else}
      <span class="mono type">{typeLabel(node)}</span>
    {/if}
    {#each modifiers as modifier}
      <span class="modifier">{modifier}</span>
    {/each}
    {#if node.system && !node.computed}
      <Tooltip text={node.system === "revision" ? "Written by mongodbee, never by the application" : "Maintained by mongodbee from the type computed declarations; the studio never edits it"}>
        <SystemTag label={node.system === "revision" ? "revision" : "computed"} />
      </Tooltip>
    {/if}
    </div>
  </td>
  <td class="rules">
    <div class="rule-list">
      {#if values}
        <span class="values">
          {#each values as option}
            <Enum value={typeof option === "string" || typeof option === "number" ? option : JSON.stringify(option)} options={values} field={name} />
          {/each}
        </span>
      {/if}
      {#if node.computed}
        <Tooltip text={node.computed} overflow><span class="rule computed-rule">{node.computed}</span></Tooltip>
      {/if}
      {#each rules as rule (rule)}
        <Tooltip text={rule} overflow><span class="rule" class:index-rule={rule.includes("index")}>{rule}</span></Tooltip>
      {/each}
      {#if node.description}
        <span class="faint">{node.description}</span>
      {/if}
    </div>
  </td>
  <td class="json">
    {#if json}
      <div class="json-line">
        {#if required !== undefined}
          <span class="req" class:on={required}>{required ? "required" : "optional"}</span>
        {/if}
        {#if jsonType}<span class="bson">{jsonType}</span>{/if}
        {#each jsonFacts as fact (fact.label)}
          <Tooltip text={fact.inert ?? fact.detail} disabled={!fact.inert && !fact.detail}>
            <span class="fact" class:inert={fact.inert}>{fact.label}</span>
          </Tooltip>
        {/each}
      </div>
    {:else}
      <span class="faint none">not in the validator</span>
    {/if}
  </td>
</tr>
{#if open}
  {#each children as [childName, child] (childName)}
    <SchemaField
      name={childName}
      node={child}
      depth={depth + 1}
      json={jsonSchemaChild(json, childName)}
      parentJson={json}
    />
  {/each}
{/if}

<style>
  .field {
    display: flex;
    align-items: center;
  }

  .indent {
    flex: none;
  }

  .toggle {
    display: inline-flex;
    align-items: center;
    padding: 0;
    border: 0;
    background: none;
    color: var(--text);
  }

  .caret,
  .spacer {
    display: inline-flex;
    flex: none;
    width: 16px;
    color: var(--text-faint);
  }

  .caret {
    transition: transform var(--dur-move) var(--ease-enter);
  }

  .caret.open {
    transform: rotate(90deg);
  }

  .usage {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    width: 100%;
  }

  .usage-track {
    flex: 1;
    height: 6px;
    min-width: 24px;
    background: color-mix(in srgb, var(--card-border) 45%, transparent);
  }

  .usage-fill {
    display: block;
    width: calc(var(--ratio) * 100%);
    height: 100%;
    background: var(--text);
  }

  .usage-value {
    min-width: 32px;
    color: var(--text-muted);
    font-size: 11px;
    text-align: right;
  }

  .usage.unused .usage-value {
    color: var(--warning);
  }

  .type {
    color: var(--text-muted);
  }

  .type-cell {
    white-space: normal;
  }

  .type-line {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 2px 8px;
    min-width: 0;
  }

  .ref-type {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
  }

  .modifier {
    color: var(--text-faint);
    font-size: var(--text-xs);
  }

  .system-name {
    color: var(--text-muted);
  }

  .computed-rule {
    border-style: dashed;
  }

  .rules,
  .json {
    padding-top: 6px;
    padding-bottom: 6px;
    white-space: normal;
  }

  .rule-list,
  .json-line {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px 6px;
  }

  .values {
    display: inline-flex;
    flex-wrap: wrap;
    gap: 4px 14px;
    margin-right: 8px;
  }

  .rule,
  .fact,
  .bson,
  .req {
    display: inline-flex;
    align-items: center;
    height: 20px;
    padding: 0 6px;
    font-family: var(--font-mono);
    font-size: 11px;
    font-variant-ligatures: none;
    white-space: nowrap;
  }

  .rule {
    display: block;
    max-width: 100%;
    overflow: hidden;
    border: 1px solid var(--card-border);
    color: var(--text-muted);
    min-width: 0;
    line-height: 18px;
    text-overflow: ellipsis;
  }

  .rule-list {
    min-width: 0;
    overflow: hidden;
  }

  .rule.index-rule {
    border-color: var(--text);
    color: var(--text);
  }

  .bson {
    padding: 0;
    color: var(--text);
  }

  .fact {
    border: 1px dashed var(--card-border);
    color: var(--text-muted);
  }

  .fact.inert {
    color: var(--text-faint);
    text-decoration: line-through;
    text-decoration-color: var(--text-faint);
  }

  .req {
    gap: 5px;
    width: 72px;
    padding: 0;
    color: var(--text-faint);
  }

  .req::before {
    flex: none;
    width: 6px;
    height: 6px;
    box-shadow: inset 0 0 0 1.5px currentColor;
    content: "";
  }

  .req.on {
    color: var(--text-muted);
  }

  .req.on::before {
    background: var(--text);
    box-shadow: none;
  }

  .none {
    font-size: var(--text-xs);
  }
</style>
