<script>
  import JsonTree from "./JsonTree.svelte";
  import Value from "./values/Value.svelte";
  import { plural } from "./lib/format.js";
  import { ChevronRight } from "./lib/icons.js";
  import Icon from "./Icon.svelte";
  import { expand } from "./lib/motion.ts";

  let { name, value, depth = 0, last = true, path = [], focus = [], node, fields, openDepth = 2 } = $props();

  function isContainer(v) {
    if (v === null || typeof v !== "object") return false;
    if (Array.isArray(v)) return true;
    const keys = Object.keys(v);
    return !(keys.length === 1 && keys[0].startsWith("$"));
  }

  function onFocusPath() {
    return focus.length >= path.length && path.every((key, index) => focus[index] === key);
  }

  const nested = $derived(isContainer(value));
  const isArray = $derived(Array.isArray(value));
  const targeted = $derived(focus.length > 0 && focus.length === path.length && onFocusPath());
  let open = $state(depth < openDepth || (focus.length > 0 && onFocusPath()));

  const children = $derived(
    nested ? (isArray ? value.map((v, i) => [String(i), v]) : Object.entries(value)) : [],
  );
  const openBracket = $derived(isArray ? "[" : "{");
  const closeBracket = $derived(isArray ? "]" : "}");

  const resolved = $derived.by(() => {
    if (node?.kind !== "variant" || !value || typeof value !== "object") return node;
    const tag = value[node.discriminator];
    return node.options?.find((option) => option.entries?.[node.discriminator]?.literal === tag) ?? node;
  });

  function childNode(key) {
    if (depth === 0 && fields) return fields[key];
    if (isArray) return resolved?.item;
    return resolved?.entries?.[key];
  }

  function reveal(element) {
    if (targeted) requestAnimationFrame(() => element.scrollIntoView({ block: "center" }));
  }
</script>

<div class="line" class:targeted use:reveal>
  {#if nested && children.length === 0}
    <span class="leaf">
      {#if name !== undefined}<span class="key">{name}</span><span class="punct">:&nbsp;</span>{/if}
      <span class="punct">{openBracket}{closeBracket}{last ? "" : ","}</span>
    </span>
  {:else if nested}
    <button class="toggle" onclick={() => (open = !open)} aria-expanded={open}>
      <span class="caret" class:open><Icon icon={ChevronRight} size={12} /></span>
      {#if name !== undefined}<span class="key">{name}</span><span class="punct">:&nbsp;</span>{/if}
      <span class="punct">{openBracket}</span>
      {#if !open}
        <span class="summary">{plural(children.length, isArray ? "item" : "key")}</span>
        <span class="punct">{closeBracket}{last ? "" : ","}</span>
      {/if}
    </button>
    {#if open}
      <div class="body" in:expand out:expand={{ exit: true }}>
        <div class="children">
          {#each children as [key, child], index (key)}
            <JsonTree
              name={isArray ? undefined : key}
              value={child}
              depth={depth + 1}
              last={index === children.length - 1}
              path={[...path, key]}
              {focus}
              {openDepth}
              node={childNode(key)}
            />
          {/each}
        </div>
        <div class="punct close">{closeBracket}{last ? "" : ","}</div>
      </div>
    {/if}
  {:else}
    <span class="leaf">
      {#if name !== undefined}<span class="key">{name}</span><span class="punct">:&nbsp;</span>{/if}
      <span class="value"><Value {value} node={node ?? (depth === 1 ? fields?.[name] : undefined)} field={name} json /></span
      ><span class="punct">{last ? "" : ","}</span>
    </span>
  {/if}
</div>

<style>
  .line {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    font-variant-ligatures: none;
    line-height: 24px;
  }

  .line.targeted {
    margin: 0 -8px;
    padding: 0 8px;
    border-radius: 0;
    background: var(--select-bg);
    box-shadow: inset 2px 0 0 var(--select-edge);
  }

  .toggle {
    display: inline-flex;
    align-items: center;
    margin-left: -16px;
    padding: 0;
    border: 0;
    background: none;
    font: inherit;
  }

  .caret {
    display: inline-flex;
    width: 16px;
    color: var(--text-faint);
    transition: transform var(--dur-move) var(--ease-enter);
  }

  .caret.open {
    transform: rotate(90deg);
  }

  .children {
    padding-left: 16px;
  }

  .leaf {
    display: flex;
    align-items: center;
    min-width: 0;
  }

  .leaf:has(> .value > :global(.quoted)) {
    display: block;
    padding-left: 2ch;
    text-indent: -2ch;
  }

  .leaf:has(> .value > :global(.quoted)) > * {
    text-indent: 0;
  }

  .value {
    display: inline-flex;
    min-width: 0;
    font-family: var(--font-sans);
    font-size: var(--text-sm);
  }

  .leaf:has(> .value > :global(.quoted)) .value {
    display: inline;
  }

  .key {
    flex: none;
    color: var(--text-muted);
  }

  .punct {
    flex: none;
    color: var(--text-faint);
  }

  .summary {
    margin: 0 4px;
    color: var(--text-faint);
    font-family: var(--font-sans);
  }
</style>
