<script>
  import { api, collectionPath } from "./lib/api.js";
  import { formatNumber, plural } from "./lib/format.js";
  import { durationParts, formatCount } from "./lib/values.ts";
  import ErrorState from "./ErrorState.svelte";
  import Status from "./Status.svelte";
  import Segmented from "./controls/Segmented.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import BarRow from "./charts/BarRow.svelte";
  import ChartFrame from "./charts/ChartFrame.svelte";
  import TypeTag from "./values/TypeTag.svelte";
  import Value from "./values/Value.svelte";

  let { collection, report, typed = false, navigate, index = 6 } = $props();

  const SIZES = [
    { id: "200", label: "200" },
    { id: "1000", label: "1,000" },
    { id: "2000", label: "2,000" },
  ];
  const MAX_REVISION_ROWS = 8;

  let size = $state("200");
  let checks = $state({});

  const subjects = $derived(report.subjects);
  const bySubject = $derived(
    subjects.map((subject) => ({ subject, fields: report.fields.filter((field) => field.subject === subject) })),
  );

  const revisionRows = $derived.by(() => {
    const rows = (report.stats?.revisions ?? []).filter((row) => row.count > 0);
    if (rows.length <= MAX_REVISION_ROWS) return rows;
    const head = rows.slice(0, MAX_REVISION_ROWS - 1);
    const rest = rows.slice(MAX_REVISION_ROWS - 1);
    return [...head, { revision: rest[0].revision, upper: true, count: rest.reduce((sum, row) => sum + row.count, 0) }];
  });
  const neverFilled = $derived(
    (report.stats?.sampled ?? 0) > 0 && report.fields.every((field) => (field.filled ?? 0) === 0),
  );
  const largestRevision = $derived(Math.max(1, ...revisionRows.map((row) => row.count)));

  function share(filled, sampled) {
    if (!sampled) return "";
    const value = (filled / sampled) * 100;
    return filled === 0 ? "0%" : value >= 99.5 ? "100%" : value < 1 ? "<1%" : `${Math.round(value)}%`;
  }

  function age(ms) {
    return durationParts(ms)
      .map(([value, unit]) => `${value} ${unit}`)
      .join(" ");
  }

  function revisionLabel(row) {
    if (row.revision === null) return "never recomputed";
    return row.upper ? `revision ${row.revision} and more` : `revision ${row.revision}`;
  }

  async function runCheck(subject) {
    checks = { ...checks, [subject]: { running: true } };
    try {
      const result = await api(collectionPath(collection.name, "computed-check"), {
        type: typed ? subject : undefined,
        limit: size,
      });
      checks = { ...checks, [subject]: { result } };
    } catch (error) {
      checks = { ...checks, [subject]: { error } };
    }
  }

  function openExample(example) {
    navigate({
      view: "collection",
      collection: collection.name,
      tab: "data",
      type: typed ? example.subject : "",
      open: example.open,
    });
  }

  function pendingOf(subject, name) {
    return report.fields.find((field) => field.subject === subject && field.name === name)?.pending ?? 0;
  }

  function driftTotal(result) {
    return Object.values(result.drifted).reduce((sum, count) => sum + count, 0);
  }
</script>

{#snippet pendingFoot()}
  {#if neverFilled}
    <span class="warn">No sampled document holds a value yet: the migration that applies these fields has not run on this database.</span>
  {/if}
  {#if report.pending && report.pending.count > 0}
    <span class="warn">
      {plural(report.pending.count, "recomputation")} waiting for the drainer{#if report.pending.oldestAgeMs !== null}, the oldest for {age(report.pending.oldestAgeMs)}{/if}.
    </span>
  {:else}
    No recomputation waiting for the drainer.
  {/if}
  Filled means the document holds a value for the field, measured on a sample of {formatCount(report.stats?.sampled ?? 0)} documents.
{/snippet}

<ChartFrame title="computed fields" label="Computed fields: how many documents hold a value" {index} foot={pendingFoot}>
  {#each bySubject as group (group.subject)}
    {#if typed && subjects.length > 1}
      <div class="subject"><TypeTag name={group.subject} /></div>
    {/if}
    <ol class="bars">
      {#each group.fields as field (field.name)}
        <BarRow
          ratio={field.sampled ? (field.filled ?? 0) / field.sampled : 0}
          fill="var(--text)"
          label="{field.name}, filled on {formatCount(field.filled ?? 0)} of {formatCount(field.sampled ?? 0)} documents"
          count={share(field.filled ?? 0, field.sampled ?? 0)}
          dim={(field.filled ?? 0) === 0}
        >
          {#snippet name()}
            <Tooltip text={field.description} overflow>
              <span class="mono field-name">{field.name}</span>
            </Tooltip>
          {/snippet}
          {#snippet aside()}
            {#if field.pending > 0}
              <Status tone="warning" hollow label="{formatNumber(field.pending)} pending" />
            {/if}
          {/snippet}
        </BarRow>
      {/each}
    </ol>
  {/each}
</ChartFrame>

{#if revisionRows.length > 0}
  <ChartFrame title="recomputations" label="Documents by the number of times mongodbee recomputed them" index={index + 1}>
    <ol class="bars">
      {#each revisionRows as row (row.revision ?? "none")}
        <BarRow
          ratio={row.count / largestRevision}
          fill={row.revision === null ? "var(--warning)" : "var(--text)"}
          label="{revisionLabel(row)}, {plural(row.count, 'document')}"
          count={formatCount(row.count)}
        >
          {#snippet name()}
            <span class="mono field-name">{revisionLabel(row)}</span>
          {/snippet}
        </BarRow>
      {/each}
    </ol>
  </ChartFrame>
{/if}

<ChartFrame title="stored values against a full recompute" label="Compare stored computed values with a recompute from their sources" index={index + 2}>
  <div class="check-bar">
    <span class="small muted">Recompute the first</span>
    <Segmented items={SIZES} value={size} label="Documents to check" size="sm" onselect={(id) => (size = id)} />
    <span class="small muted">documents of each type and compare. It only reads.</span>
  </div>
  {#each subjects as subject (subject)}
    {@const state = checks[subject]}
    <div class="check">
      <div class="check-head">
        {#if typed}<TypeTag name={subject} />{:else}<span class="mono">{subject}</span>{/if}
        <span class="spacer"></span>
        {#if state?.result}
          {@const total = driftTotal(state.result)}
          <span class="small muted">{plural(state.result.checked, "document")} checked{state.result.complete ? "" : ", more remain"}</span>
          <Status
            tone={total === 0 ? "success" : "danger"}
            label={total === 0 ? "in step" : `${formatNumber(total)} drifted`}
          />
        {/if}
        <button class="control" type="button" onclick={() => runCheck(subject)} disabled={state?.running}>
          {state?.running ? "Checking" : state?.result ? "Check again" : "Check"}
        </button>
      </div>
      {#if state?.error}
        <ErrorState error={state.error} compact title="The check did not run" />
      {:else if state?.result}
        <ul class="fields">
          {#each Object.entries(state.result.drifted) as [name, count] (name)}
            <li class="field">
              <span class="mono field-name">{name}</span>
              {#if count === 0}
                <Status tone="success" hollow label="in step" />
              {:else}
                <Status tone="danger" label="{formatNumber(count)} drifted" />
                {#if pendingOf(subject, name) > 0}
                  <span class="small faint">{formatNumber(pendingOf(subject, name))} still waiting for the drainer</span>
                {/if}
              {/if}
            </li>
          {/each}
        </ul>
        {#if state.result.examples.length > 0}
          <table class="table examples">
            <thead>
              <tr><th>document</th><th>field</th><th>stored</th><th>recomputed</th></tr>
            </thead>
            <tbody>
              {#each state.result.examples as example (`${example.id}|${example.field}`)}
                <tr class="example">
                  <td>
                    <button class="open-link" type="button" onclick={() => openExample({ ...example, subject })}>
                      <Value value={example.id} field="_id" />
                    </button>
                  </td>
                  <td class="mono small">{example.field}</td>
                  <td>
                    {#if example.missing}<span class="faint small">missing</span>{:else}<Value value={example.stored} compact />{/if}
                  </td>
                  <td><Value value={example.truth} compact /></td>
                </tr>
              {/each}
            </tbody>
          </table>
        {/if}
      {/if}
    </div>
  {/each}
</ChartFrame>

<style>
  .bars {
    display: flex;
    flex-direction: column;
    gap: 2px;
    margin: 0;
    padding: 0;
  }

  .subject {
    margin: 8px 0 4px 6px;
  }

  .field-name {
    overflow: hidden;
    font-size: var(--text-xs);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .small {
    font-size: var(--text-xs);
  }

  .warn {
    color: var(--warning);
  }

  .check-bar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
  }

  .check {
    padding: 8px 0;
    border-top: 1px solid var(--hairline);
  }

  .check-head {
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .spacer {
    flex: 1;
  }

  .fields {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 18px;
    margin: 8px 0 0;
    padding: 0;
    list-style: none;
  }

  .field {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }

  .examples {
    width: 100%;
    margin-top: 10px;
  }

  .open-link {
    display: inline-flex;
    max-width: 100%;
    padding: 0;
    border: 0;
    background: none;
    cursor: pointer;
  }

  @media (hover: hover) and (pointer: fine) {
    .open-link:hover {
      text-decoration: underline;
      text-decoration-color: var(--card-border);
      text-underline-offset: 3px;
    }
  }
</style>
