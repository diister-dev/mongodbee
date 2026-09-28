<script>
  import { api, collectionPath } from "./lib/api.js";
  import { formatNumber, isTypedKind, plural } from "./lib/format.js";
  import { formatCount, formatFullDate, idTime, PALETTE, relativeTime } from "./lib/values.ts";
  import ErrorState from "./ErrorState.svelte";
  import Status from "./Status.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import BarRow from "./charts/BarRow.svelte";
  import ChartFrame from "./charts/ChartFrame.svelte";
  import MonthColumns from "./charts/MonthColumns.svelte";
  import StatStrip from "./charts/StatStrip.svelte";
  import Scope from "./values/Scope.svelte";
  import TypeTag from "./values/TypeTag.svelte";

  let { collection, navigate, schema } = $props();

  const STACKED_TYPES = 5;
  const STACK_COLORS = PALETTE.slice(0, STACKED_TYPES).map((hue) => hue.dot);
  const OTHER_COLOR = "var(--text-faint)";

  let summary = $state(null);
  let error = $state(null);
  let coverage = $state(null);
  let attempt = $state(0);

  const typed = $derived(isTypedKind(collection.kind));

  $effect(() => {
    const name = collection.name;
    attempt;
    summary = null;
    error = null;
    coverage = null;
    api(collectionPath(name, "summary"))
      .then((result) => {
        if (name === collection.name) summary = result;
      })
      .catch((e) => (error = e));
    if (!typed) {
      api(collectionPath(name, "coverage"))
        .then((result) => {
          if (name === collection.name) coverage = result;
        })
        .catch(() => (coverage = { sampled: 0, fields: {} }));
    }
  });

  const declaredFields = $derived(Object.keys(schema?.types?.[0]?.fields ?? {}));
  const fieldRows = $derived.by(() => {
    if (!coverage || coverage.sampled === 0) return [];
    const keys = new Set([...declaredFields, ...Object.keys(coverage.fields)]);
    keys.delete("_id");
    return [...keys]
      .map((key) => ({
        key,
        count: coverage.fields[key] ?? 0,
        declared: declaredFields.includes(key),
      }))
      .sort((a, b) => b.count - a.count || a.key.localeCompare(b.key));
  });
  const fieldsInUse = $derived(fieldRows.filter((row) => row.declared && row.count > 0).length);

  const created = $derived(summary?.created ?? null);
  const createdScale = $derived(created && created.sampled > 0 ? (summary?.total ?? 0) / created.sampled : 1);
  const recent = $derived.by(() => {
    if (!created || created.months.length === 0) return null;
    const last = created.months[created.months.length - 1];
    const [year, month] = last.month.split("-").map(Number);
    const name = new Date(Date.UTC(year, month - 1, 1)).toLocaleString("en", { month: "long", timeZone: "UTC" });
    return { count: Math.round(last.count * createdScale), label: `${name} ${year}` };
  });

  const scoped = $derived(Boolean(summary?.scopes));
  const dataTypes = $derived((summary?.types ?? []).filter((type) => !type.meta));
  const present = $derived(
    dataTypes.filter((type) => type.count > 0).toSorted((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
  );
  const empty = $derived(dataTypes.filter((type) => type.count === 0 && type.declared));
  const undeclared = $derived(present.filter((type) => !type.declared));
  const metaTypes = $derived((summary?.types ?? []).filter((type) => type.meta && type.count > 0));
  const documents = $derived(present.reduce((sum, type) => sum + type.count, 0));
  const FUTURE_SLACK_MS = 24 * 60 * 60 * 1000;
  const now = Date.now();
  const isFuture = (time) => time !== null && time > now + FUTURE_SLACK_MS;
  const newest = $derived(
    Math.max(
      -Infinity,
      ...present.map((type) => idTime(type.lastId)).filter((time) => time !== null && !isFuture(time)),
    ),
  );
  const futureTypes = $derived(present.filter((type) => isFuture(idTime(type.lastId))));

  const largestType = $derived(Math.max(1, ...present.map((type) => type.count)));
  const topScopes = $derived(summary?.scopes?.top ?? []);
  const largestScope = $derived(Math.max(1, ...topScopes.map((scope) => scope.count)));
  const stacked = $derived(present.slice(0, STACKED_TYPES));
  const colorOf = $derived(new Map(stacked.map((type, index) => [type.name, STACK_COLORS[index]])));
  const buckets = $derived(summary?.scopes?.sizes ?? []);
  const largestBucket = $derived(Math.max(1, ...buckets.map((bucket) => bucket.scopes)));

  function logRatio(value, largest) {
    if (!value) return 0;
    return Math.max(0.01, Math.log10(value + 1) / Math.log10(largest + 1));
  }

  function share(count) {
    if (!documents) return "";
    const percent = (count / documents) * 100;
    return percent >= 10 ? `${Math.round(percent)}%` : percent >= 0.1 ? `${percent.toFixed(1)}%` : "<0.1%";
  }

  function scopeSegments(scope) {
    const segments = [];
    let other = 0;
    for (const { type, count } of scope.types) {
      if (colorOf.has(type)) segments.push({ key: type, value: count, color: colorOf.get(type), type });
      else if (!summary.types.find((entry) => entry.name === type)?.meta) other += count;
    }
    segments.sort((a, b) => stacked.findIndex((t) => t.name === a.type) - stacked.findIndex((t) => t.name === b.type));
    if (other > 0) segments.push({ key: "other", value: other, color: OTHER_COLOR });
    return segments;
  }

  function scopeLabel(scope) {
    return typeof scope.scope === "string" ? scope.scope : JSON.stringify(scope.scope);
  }

  function compact(value) {
    return value >= 1000 ? `${Number.parseFloat((value / 1000).toFixed(1))}k` : String(value);
  }

  function bucketLabel(bucket) {
    return bucket.to === undefined ? `${compact(bucket.from)}+` : `${compact(bucket.from)}-${compact(bucket.to)}`;
  }

  function bucketSentence(bucket) {
    return bucket.to === undefined
      ? `${formatNumber(bucket.from)} documents or more`
      : `${formatNumber(bucket.from)} to ${formatNumber(bucket.to)} documents`;
  }

  function openType(type) {
    navigate({ view: "collection", collection: collection.name, tab: "data", type: type.name });
  }

  function openScope(scope) {
    navigate({ view: "collection", collection: collection.name, tab: "data", scope: scopeLabel(scope) });
  }

  const stats = $derived.by(() => {
    if (!summary) return [];
    const items = [{ label: "documents", value: formatNumber(documents) }];
    if (!typed) {
      items.push({
        label: "fields in use",
        value: coverage ? formatNumber(fieldsInUse) : "…",
        of: declaredFields.length > 0 ? `of ${formatNumber(declaredFields.length)} declared` : undefined,
        faint: !coverage,
      });
      items.push({
        label: "created in the latest month",
        value: recent === null ? "unknown" : formatNumber(recent.count),
        faint: recent === null,
        note:
          recent === null
            ? "ids carry no creation time"
            : `${recent.label}${createdScale > 1 ? ", estimated from a sample" : ""}`,
      });
    } else {
      items.push({
        label: "types in use",
        value: formatNumber(present.length),
        of: `of ${formatNumber(dataTypes.filter((type) => type.declared).length)} declared`,
      });
    }
    if (!typed) {
      const latest = created?.newest ?? (Number.isFinite(newest) ? newest : null);
      items.push({
        label: "newest document",
        value: latest === null ? "unknown" : relativeTime(latest),
        faint: latest === null,
        note: latest === null ? "ids carry no creation time" : `${formatFullDate(latest)}, from its id`,
      });
      return items;
    }
    if (scoped) {
      items.push({ label: "scopes", value: formatNumber(summary.scopes.distinct) });
    } else {
      items.push({ label: "undeclared types", value: formatNumber(undeclared.length), faint: undeclared.length === 0 });
    }
    items.push({
      label: "newest document",
      value: Number.isFinite(newest) ? relativeTime(newest) : "unknown",
      faint: !Number.isFinite(newest),
      note:
        futureTypes.length > 0
          ? `${plural(futureTypes.length, "type")} with ids dated in the future, left out`
          : "from the time in its id",
    });
    return items;
  });

  function fieldShare(count) {
    if (!coverage?.sampled) return "";
    const value = (count / coverage.sampled) * 100;
    return count === 0 ? "0%" : value >= 99.5 ? "100%" : value < 1 ? "<1%" : `${Math.round(value)}%`;
  }
</script>

{#snippet creationFoot()}
  From the creation time in the ids of {formatCount(created.sampled)}
  {created.sampled < (summary?.total ?? 0) ? "sampled" : ""} documents{#if createdScale > 1}, scaled to the collection{/if}.
  {#if created.dated < created.sampled - created.future}
    {formatCount(created.sampled - created.dated - created.future)} have ids without a time.
  {/if}
  {#if created.future > 0}
    <span class="warn">{formatCount(created.future)} have ids dated in the future, left out.</span>
  {/if}
{/snippet}

{#snippet typesFoot()}
  {#if empty.length > 0}
    {plural(empty.length, "declared type")} without documents:
    {#each empty as type, index (type.name)}<span class="mono">{type.name}</span>{index < empty.length - 1 ? ", " : ""}{/each}.
  {/if}
  {#if metaTypes.length > 0}
    Also holds {plural(metaTypes.reduce((sum, type) => sum + type.count, 0), "mongodbee marker")}, not counted.
  {/if}
{/snippet}

{#snippet scopesFoot()}
  {#if summary.scopes.distinct > topScopes.length}
    {plural(summary.scopes.distinct - topScopes.length, "more scope")}.
  {/if}
  {#if summary.scopes.unscoped > 0}
    <span class="warn">{plural(summary.scopes.unscoped, "document")} without a scope.</span>
  {/if}
{/snippet}

{#snippet stackLegend()}
  {#each stacked as type (type.name)}
    <span class="legend-item"><i style="background: {colorOf.get(type.name)}"></i>{type.name}</span>
  {/each}
  {#if present.length > stacked.length}
    <span class="legend-item"><i style="background: {OTHER_COLOR}"></i>other types</span>
  {/if}
{/snippet}

<div class="bezel page">
  <div class="bezel-card body">
    <div class="scroll">
      {#if error}
        <div class="inset">
          <ErrorState {error} title="The summary could not be computed" onretry={() => attempt++} />
        </div>
      {:else if !summary}
        <div class="empty-state"><span class="shimmer">Summarizing {collection.name}</span></div>
      {:else}
        <div class="inset">
          <StatStrip items={stats} />
        </div>

        {#if created && created.dated > 0}
          <div class="inset">
            <ChartFrame
              title="created per month"
              label="Documents created per month, from the time in their ids"
              index={4}
              foot={creationFoot}
            >
              <MonthColumns months={created.months} scale={createdScale} />
            </ChartFrame>
          </div>
        {/if}

        {#if !typed}
          <div class="inset">
            <ChartFrame title="fields in use" label="Share of documents that fill each field" index={5}>
              {#if !coverage}
                <p class="faint small"><span class="shimmer">Sampling the documents</span></p>
              {:else if fieldRows.length === 0}
                <p class="faint small">No documents yet.</p>
              {:else}
                <ol class="bars">
                  {#each fieldRows as row (row.key)}
                    <BarRow
                      ratio={row.count / coverage.sampled}
                      fill={row.declared ? "var(--text)" : "var(--warning)"}
                      label="{row.key}, {formatCount(row.count)} of {formatCount(coverage.sampled)} documents"
                      count={fieldShare(row.count)}
                      dim={row.count === 0}
                    >
                      {#snippet name()}
                        <span class="mono field-name">{row.key}</span>
                        {#if !row.declared}<span class="flag"><Status tone="warning" hollow label="not in the schema" /></span>{/if}
                      {/snippet}
                    </BarRow>
                  {/each}
                </ol>
              {/if}
            </ChartFrame>
          </div>
        {/if}

        {#if typed}
        <div class="inset">
          <ChartFrame
            title="types"
            label="Documents per type"
            index={4}
            foot={empty.length > 0 || metaTypes.length > 0 ? typesFoot : undefined}
          >
            {#if present.length === 0}
              <p class="faint small">No documents yet.</p>
            {:else}
              <ol class="bars">
                {#each present as type (type.name)}
                  {@const newestId = idTime(type.lastId)}
                  <BarRow
                    ratio={type.count / largestType}
                    fill="var(--text)"
                    label="{type.name}, {plural(type.count, 'document')}"
                    count={formatNumber(type.count)}
                    onclick={() => openType(type)}
                  >
                    {#snippet name()}
                      <TypeTag name={type.name} />
                      {#if !type.declared}<span class="flag"><Status tone="warning" hollow label="undeclared" /></span>{/if}
                    {/snippet}
                    {#snippet aside()}
                      <span class="share num">{share(type.count)}</span>
                      {#if scoped && type.scopes !== undefined}
                        <span>in {formatNumber(type.scopes)} of {formatNumber(summary.scopes.distinct)}</span>
                      {/if}
                      {#if newestId !== null && isFuture(newestId)}
                        <Tooltip text="The newest id is dated {formatFullDate(newestId)}, after today: these ids were not generated when the documents were inserted">
                          <span class="warn">future id</span>
                        </Tooltip>
                      {:else if newestId !== null}
                        <Tooltip text="Newest id dated {formatFullDate(newestId)}"><span>{relativeTime(newestId)}</span></Tooltip>
                      {/if}
                    {/snippet}
                  </BarRow>
                {/each}
              </ol>
            {/if}
          </ChartFrame>
        </div>
        {/if}

        {#if scoped && topScopes.length > 0}
          <div class="inset split">
            <ChartFrame
              title="largest scopes"
              label="Largest scopes by documents, split by type"
              index={5}
              legend={stackLegend}
              foot={summary.scopes.distinct > topScopes.length || summary.scopes.unscoped > 0 ? scopesFoot : undefined}
            >
              <ol class="bars">
                {#each topScopes as scope (scopeLabel(scope))}
                  <BarRow
                    ratio={logRatio(scope.count, largestScope)}
                    segments={scopeSegments(scope)}
                    label="{scopeLabel(scope)}, {plural(scope.count, 'document')}"
                    count={formatNumber(scope.count)}
                    onclick={() => openScope(scope)}
                  >
                    {#snippet name()}<Scope value={scope.scope} interactive={false} />{/snippet}
                  </BarRow>
                {/each}
              </ol>
            </ChartFrame>

            <ChartFrame title="scope sizes" label="Scopes grouped by how many documents they hold" index={6}>
              <ol class="histogram">
                {#each buckets as bucket (bucket.from)}
                  <li class="column">
                    <Tooltip text="{plural(bucket.scopes, 'scope')} holding {bucketSentence(bucket)}">
                      <span class="column-body">
                        <span class="column-count num" class:faint={bucket.scopes === 0}>{formatNumber(bucket.scopes)}</span>
                        <span class="column-track">
                          <span class="column-fill" style="--ratio: {bucket.scopes / largestBucket}"></span>
                        </span>
                        <span class="column-label mono">{bucketLabel(bucket)}</span>
                      </span>
                    </Tooltip>
                  </li>
                {/each}
              </ol>
            </ChartFrame>
          </div>
        {/if}
      {/if}
    </div>
  </div>
</div>

<style>
  .page {
    flex: 1;
    min-height: 0;
  }

  .body {
    flex: 1;
    min-height: 0;
  }

  .scroll {
    display: flex;
    flex-direction: column;
    gap: 20px;
    padding: 20px 0 24px;
    overflow: auto;
  }

  .inset {
    margin: 0 24px;
  }

  .split {
    display: grid;
    grid-template-columns: minmax(0, 2fr) minmax(240px, 1fr);
    gap: 16px;
    align-items: start;
  }

  .bars {
    display: flex;
    flex-direction: column;
    gap: 2px;
    margin: 0;
    padding: 0;
  }

  .flag {
    display: inline-flex;
    margin-left: 8px;
    font-size: var(--text-xs);
  }

  .field-name {
    overflow: hidden;
    font-size: 12px;
    text-overflow: ellipsis;
  }

  .share {
    min-width: 40px;
    color: var(--text-muted);
    text-align: right;
  }

  .warn {
    color: var(--warning);
  }

  .small {
    margin: 0;
    font-size: var(--text-xs);
  }

  .histogram {
    display: grid;
    max-width: 480px;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 8px;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .column :global(.tip-trigger) {
    display: flex;
    width: 100%;
  }

  .column-body {
    display: flex;
    flex-direction: column;
    align-items: stretch;
    gap: 6px;
    width: 100%;
  }

  .column-count {
    font-size: var(--text-sm);
    text-align: center;
  }

  .column-track {
    display: flex;
    align-items: flex-end;
    height: 96px;
    background: color-mix(in srgb, var(--card-border) 45%, transparent);
  }

  .column-fill {
    width: 100%;
    height: calc(var(--ratio) * 100%);
    min-height: 0;
    background: var(--text);
  }

  .column-label {
    overflow: hidden;
    color: var(--text-faint);
    font-size: 10.5px;
    text-align: center;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  @media (max-width: 1100px) {
    .split {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
