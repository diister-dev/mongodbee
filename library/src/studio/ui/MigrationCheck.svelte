<script>
  import { onMount } from "svelte";
  import Icon from "./Icon.svelte";
  import Status from "./Status.svelte";
  import CommandChip from "./CommandChip.svelte";
  import Combobox from "./controls/Combobox.svelte";
  import Select from "./controls/Select.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import Duration from "./values/Duration.svelte";
  import MigrationId from "./values/MigrationId.svelte";
  import { checkRequest, checkState, startCheck, stopCheck } from "./lib/check-store.js";
  import { splitWarnings } from "./lib/check-warnings.ts";
  import { plural } from "./lib/format.js";
  import { ChevronRight } from "./lib/icons.js";
  import { expand } from "./lib/motion.ts";

  let { report } = $props();

  let mode = $state($checkState.mode ?? "normal");
  let last = $state($checkState.last ?? 0);
  let docs = $state($checkState.docs ?? "");
  let retention = $state($checkState.retention ?? "");
  let open = $state({});
  let sharedOpen = $state(false);

  const split = $derived(splitWarnings($checkState.rows ?? [], $checkState.status !== "running"));

  const migrations = $derived(
    (report?.migrations ?? [])
      .filter((m) => m.position !== null)
      .map((m) => ({ id: m.id, name: m.name, fileName: m.fileName, status: m.status })),
  );

  const settings = $derived(
    report?.simulation ?? { presets: { quick: 10, normal: 100, hard: 500 }, maxDocs: 5000, defaultRetention: 0.5 },
  );

  const MODES = $derived([
    { value: "quick", label: "quick", hint: `${settings.presets.quick} documents` },
    { value: "normal", label: "normal", hint: `${settings.presets.normal} documents` },
    { value: "hard", label: "hard", hint: `${settings.presets.hard} documents` },
  ]);

  const STARTS = $derived([
    { value: 0, label: "every migration", hint: plural(migrations.length, "migration") },
    ...migrations.slice(1).map((migration, index) => ({
      value: migrations.length - index - 1,
      label: migration.name,
      hint: `${migration.status === "applied" ? "" : `${migration.status}, `}last ${migrations.length - index - 1}`,
    })),
  ]);

  const docsError = $derived.by(() => {
    if (docs.trim() === "") return null;
    const value = Number(docs);
    return Number.isInteger(value) && value >= 1 && value <= settings.maxDocs
      ? null
      : `A whole number from 1 to ${settings.maxDocs}`;
  });

  const retentionError = $derived.by(() => {
    if (retention.trim() === "") return null;
    const value = Number(retention);
    return Number.isFinite(value) && value >= 0 && value <= 100 ? null : "A percentage from 0 to 100";
  });

  const tuned = $derived(docs.trim() !== "" || retention.trim() !== "");

  const command = $derived(
    [
      "mongodbee check",
      mode !== "normal" ? `--mode ${mode}` : "",
      last > 0 ? `--last ${last}` : "",
      docs.trim() !== "" && !docsError ? `--docs ${Number(docs)}` : "",
      retention.trim() !== "" && !retentionError ? `--retention ${Number(retention) / 100}` : "",
    ]
      .filter(Boolean)
      .join(" "),
  );

  function resetTuning() {
    docs = "";
    retention = "";
  }

  const deleteWarnings = $derived(
    ($checkState.rows ?? []).flatMap((row) =>
      (split.specific[row.id] ?? [])
        .filter((warning) => /^Operation \d+ \((delete|dedupe)_/.test(warning))
        .map((warning) => ({ row, warning })),
    ),
  );

  const counts = $derived.by(() => {
    const rows = $checkState.rows ?? [];
    return {
      valid: rows.filter((r) => r.status === "valid").length,
      failed: rows.filter((r) => r.status === "failed").length,
      total: rows.length,
    };
  });

  function run() {
    if (docsError || retentionError) return;
    open = {};
    startCheck({ mode, last, docs: docs.trim(), retention: retention.trim(), migrations });
  }

  function justSettled(row) {
    return row.settledAt !== undefined && Date.now() - row.settledAt < 1000;
  }

  function toggle(id) {
    open = { ...open, [id]: !open[id] };
  }

  onMount(() =>
    checkRequest.subscribe((n) => {
      if (n > 0 && $checkState.status !== "running" && migrations.length > 0) {
        checkRequest.set(0);
        run();
      }
    }),
  );
</script>

<div class="page">
  <section class="bezel controls">
    <div class="bezel-card controls-card">
      <div class="sentence">
        <span class="word">simulate from</span>
        <Combobox
          value={last}
          options={STARTS}
          label="First migration to check"
          width="260px"
          mono
          empty="No migration matches"
          onchange={(value) => (last = value)}
        />
        <span class="word">in</span>
        <Select bind:value={mode} options={MODES} label="Simulation mode" size="sm" />
        <span class="word">mode, with</span>
        <Tooltip text={docsError ?? `Mock documents generated per collection; the ${mode} mode uses ${settings.presets[mode]}`}>
          <input
            class="tune mono"
            class:invalid={docsError}
            bind:value={docs}
            inputmode="numeric"
            placeholder={String(settings.presets[mode])}
            aria-label="Mock documents per collection"
            aria-invalid={docsError ? "true" : undefined}
            spellcheck="false"
            autocomplete="off"
          />
        </Tooltip>
        <span class="word">documents per collection, keeping</span>
        <Tooltip text={retentionError ?? "Share of each migration's documents carried into the next one; the rest is generated fresh"}>
          <span class="percent">
            <input
              class="tune mono"
              class:invalid={retentionError}
              bind:value={retention}
              inputmode="decimal"
              placeholder={String(Math.round(settings.defaultRetention * 100))}
              aria-label="Percentage of documents kept between migrations"
              aria-invalid={retentionError ? "true" : undefined}
              spellcheck="false"
              autocomplete="off"
            />
            <span class="unit">%</span>
          </span>
        </Tooltip>
        <span class="word">between migrations</span>
        {#if tuned}
          <button class="control quiet" type="button" onclick={resetTuning}>Reset</button>
        {/if}
      </div>
      <div class="controls-row">
        {#if $checkState.status === "running"}
          <button class="control" onclick={stopCheck}>Stop check</button>
        {:else}
          <button
            class="control primary"
            onclick={run}
            disabled={migrations.length === 0 || Boolean(docsError || retentionError)}
          >
            Run check
          </button>
        {/if}
        <CommandChip {command} />
        <Tooltip text="The simulation is purely in memory; it never connects to the database">
          <span class="dry muted">Dry run</span>
        </Tooltip>
      </div>
    </div>
  </section>

  {#if $checkState.status !== "idle"}
    <section class="bezel">
      <div class="bezel-card">
        <div class="row schema-row">
          <span class="cell-status">
            {#if $checkState.schema}
              <Status tone={$checkState.schema.valid ? "success" : "danger"} label={$checkState.schema.valid ? "consistent" : "inconsistent"} />
            {:else if $checkState.status === "running"}
              <span class="shimmer small">Comparing</span>
            {:else}
              <Status tone="neutral" label="not run" />
            {/if}
          </span>
          <span class="name">Schema consistency</span>
          <span class="muted small">schemas.ts against the last migration</span>
        </div>
        {#if $checkState.schema && !$checkState.schema.valid}
          <div class="errors" in:expand>
            {#each $checkState.schema.errors as error}<pre class="mono">{error}</pre>{/each}
          </div>
        {/if}

        {#if split.shared.length > 0}
          <div class="migration shared">
            <button class="row clickable" onclick={() => (sharedOpen = !sharedOpen)} aria-expanded={sharedOpen}>
              <span class="cell-status"><Status tone="warning" label="every migration" /></span>
              <span class="name shared-name">
                Shared warnings
                <span class="count-badge small-badge">{split.shared.length}</span>
              </span>
              <span class="muted small">Raised by each checked migration, shown once</span>
              <span class="facts small muted">
                <span>{plural(split.shared.reduce((sum, item) => sum + item.occurrences, 0), "occurrence")}</span>
                <span class="caret" class:open={sharedOpen}><Icon icon={ChevronRight} size={12} /></span>
              </span>
            </button>
            {#if sharedOpen}
              <div class="detail" in:expand out:expand={{ exit: true }}>
                <div class="detail-inner">
                  {#each split.shared as item (item.text)}<p class="warning small">{item.text}</p>{/each}
                </div>
              </div>
            {/if}
          </div>
        {/if}

        {#each $checkState.rows as row (row.id)}
          {@const specific = split.specific[row.id] ?? []}
          {@const expandable = (row.errors?.length ?? 0) + specific.length > 0}
          {@const isOpen = open[row.id] ?? row.status === "failed"}
          <div class="migration">
            <svelte:element
              this={expandable ? "button" : "div"}
              class="row"
              class:clickable={expandable}
              onclick={expandable ? () => toggle(row.id) : undefined}
              aria-expanded={expandable ? isOpen : undefined}
              role={expandable ? undefined : "group"}
            >
              <span class="cell-status">
                {#if row.status === "running"}
                  <span class="shimmer small">Simulating</span>
                {:else if row.status === "valid"}
                  <Status tone="success" label="valid" stamped={justSettled(row)} />
                {:else if row.status === "failed"}
                  <Status tone="danger" label="failed" stamped={justSettled(row)} />
                {:else if row.status === "skipped"}
                  <Status tone="neutral" label="not run" />
                {:else}
                  <Status tone="neutral" hollow label="waiting" />
                {/if}
              </span>
              <span class="name"><Tooltip text={row.name} overflow><span class="ellipsis">{row.name}</span></Tooltip></span>
              <span class="file"><MigrationId value={row.fileName ?? row.id} /></span>
              <span class="facts small muted">
                {#if row.operationCount !== undefined}
                  <span>{plural(row.operationCount, "operation")}</span>
                  <span>{row.reversible ? "reversible" : "irreversible"}</span>
                {/if}
                {#if specific.length}<span>{plural(specific.length, "warning")}</span>{/if}
                {#if row.durationMs !== undefined}<Duration ms={row.durationMs} />{/if}
                {#if expandable}<span class="caret" class:open={isOpen}><Icon icon={ChevronRight} size={12} /></span>{/if}
              </span>
            </svelte:element>
            {#if isOpen && expandable}
              <div class="detail" in:expand out:expand={{ exit: true }}>
                <div class="detail-inner">
                  {#each row.errors ?? [] as error}<pre class="mono error">{error}</pre>{/each}
                  {#each specific as warning (warning)}<p class="warning small">{warning}</p>{/each}
                </div>
              </div>
            {/if}
          </div>
        {/each}
      </div>
      <footer class="bezel-foot">
        <span class="small muted">
          {#if $checkState.status === "running"}
            <span class="shimmer">
              Checking {plural(counts.total, "migration")} in {$checkState.mode} mode{#if $checkState.docs}, {$checkState.docs} documents per collection{/if}{#if $checkState.retention}, {$checkState.retention}% kept{/if}
            </span>
          {:else if $checkState.status === "error"}
            <span class="error-text">{$checkState.error}</span>
          {:else if $checkState.status === "stopped"}
            Stopped after {counts.valid + counts.failed} of {plural(counts.total, "migration")}{#if counts.failed}, {counts.failed} failed{/if}
          {:else if $checkState.report}
            {#if $checkState.report.valid}
              All {plural(counts.valid, "migration")} valid
            {:else}
              {counts.failed} of {plural(counts.total, "migration")} failed
            {/if}
            {#if $checkState.report.notValidated?.length}, {$checkState.report.notValidated.length} earlier not validated{/if}
          {/if}
        </span>
        {#if $checkState.durationMs !== null}<span class="small"><Duration ms={$checkState.durationMs} /></span>{/if}
      </footer>
    </section>
  {/if}

  {#if deleteWarnings.length > 0}
    <section class="bezel">
      <div class="bezel-card list-card">
        <h3 class="section-title">Delete warnings</h3>
        {#each deleteWarnings as item}
          <p class="warning-line"><Status tone="warning" /> <span class="mono small">{item.row.name}</span> <span>{item.warning}</span></p>
        {/each}
      </div>
    </section>
  {/if}

</div>

<style>
  .page {
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 16px;
    min-height: 0;
    margin-right: -16px;
    padding: 4px 4px 24px 8px;
    scrollbar-gutter: stable;
    overflow: auto;
  }

  .page > :global(.bezel) {
    flex: none;
  }

  .controls-card {
    gap: 10px;
    padding: 14px 16px;
  }

  .controls-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 10px;
  }

  .sentence {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px 8px;
  }

  .word {
    color: var(--text-muted);
    font-size: var(--text-sm);
    white-space: nowrap;
  }

  .tune {
    width: 64px;
    height: 28px;
    padding: 0 8px;
    border: 1px solid var(--card-border);
    border-radius: var(--radius-control);
    outline: none;
    background: var(--card);
    color: var(--text);
    font-size: 12.5px;
    text-align: right;
  }

  .tune:focus {
    border-color: var(--text);
    box-shadow: 0 0 0 3px var(--accent-ring);
  }

  .tune::placeholder {
    color: var(--text-faint);
  }

  .tune.invalid {
    border-color: var(--danger);
    border-style: dashed;
  }

  .percent {
    position: relative;
    display: inline-flex;
    align-items: center;
  }

  .percent .tune {
    width: 56px;
    padding-right: 20px;
  }

  .unit {
    position: absolute;
    right: 8px;
    color: var(--text-faint);
    font-size: var(--text-xs);
    pointer-events: none;
  }

  .dry {
    font-size: var(--text-xs);
  }

  .small {
    font-size: var(--text-xs);
  }

  .row {
    display: grid;
    grid-template-columns: 120px minmax(160px, 240px) minmax(0, 1fr) auto;
    align-items: center;
    gap: 14px;
    width: 100%;
    min-height: 40px;
    padding: 0 16px;
    border: 0;
    border-bottom: 1px solid var(--hairline);
    background: none;
    color: inherit;
    font: inherit;
    text-align: left;
  }

  .schema-row {
    grid-template-columns: 120px minmax(160px, 240px) minmax(0, 1fr);
  }

  .clickable {
    cursor: pointer;
  }

  .migration:last-child .row {
    border-bottom: 0;
  }

  .cell-status :global(.status) {
    font-size: var(--text-xs);
  }

  .shared {
    background: var(--frame);
  }

  .shared-name {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }

  .small-badge {
    min-width: 18px;
    height: 18px;
    padding: 0 5px;
    font-size: 11px;
    line-height: 18px;
  }

  .name {
    min-width: 0;
    font-weight: 500;
  }

  .ellipsis {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .file {
    min-width: 0;
    overflow: hidden;
  }

  .facts {
    display: flex;
    align-items: center;
    gap: 12px;
    white-space: nowrap;
  }

  .caret {
    display: inline-flex;
    color: var(--text-faint);
    transition: transform var(--dur-move) var(--ease-enter);
  }

  .caret.open {
    transform: rotate(90deg);
  }

  .detail {
    border-bottom: 1px solid var(--hairline);
  }

  .detail-inner,
  .errors {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 8px 16px 12px 150px;
  }

  pre {
    margin: 0;
    white-space: pre-wrap;
  }

  .error {
    color: var(--danger);
  }

  .warning {
    margin: 0;
    color: var(--text-muted);
  }

  .error-text {
    color: var(--danger);
  }

  .list-card {
    gap: 6px;
    padding: 14px 16px;
  }

  .warning-line {
    display: flex;
    align-items: baseline;
    gap: 8px;
    margin: 0;
    font-size: var(--text-xs);
  }
</style>
