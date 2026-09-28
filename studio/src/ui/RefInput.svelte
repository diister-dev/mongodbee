<script>
  import Combobox from "./controls/Combobox.svelte";
  import { api, collectionPath } from "./lib/api.js";
  import { labels, requestLabel } from "./lib/labels.js";
  import { useStudio } from "./lib/studio.js";

  let { value, target, label, invalid = false, onchange } = $props();

  const studio = useStudio();
  let ids = $state([]);
  let loading = $state(false);
  let loaded = false;

  const where = $derived(studio.reference?.(target));

  const options = $derived.by(() => {
    const all = typeof value === "string" && value && !ids.includes(value) ? [value, ...ids] : ids;
    return all.map((id) => {
      const named = $labels[id];
      return { value: id, label: named?.label ?? id, hint: named ? id : undefined };
    });
  });

  async function load() {
    if (loaded || !where) return;
    loaded = true;
    loading = true;
    try {
      const page = await api(collectionPath(where.collection, "documents"), { type: where.type, limit: 100 });
      ids = page.items.map((item) => item._id).filter((id) => typeof id === "string");
      for (const id of ids) requestLabel(id);
    } catch {
      ids = [];
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    if (typeof value === "string" && value) requestLabel(value);
  });
</script>

<span class="ref-input">
  <Combobox
    value={value ?? ""}
    {options}
    {label}
    placeholder="{target}:…"
    free
    width="100%"
    autoselect={false}
    openOnFocus={false}
    {loading}
    {invalid}
    keyshortcuts="Control+Space"
    empty={where ? "No document found" : `No collection holds ${target} documents`}
    onsearch={load}
    onchange={(next) => onchange(next)}
  />
</span>

<style>
  .ref-input {
    display: flex;
    min-width: 0;
  }
</style>
