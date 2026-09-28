<script>
  import Status from "./Status.svelte";
  import Segmented from "./controls/Segmented.svelte";
  import MigrationTimeline from "./MigrationTimeline.svelte";
  import MigrationPlan from "./MigrationPlan.svelte";
  import MigrationCheck from "./MigrationCheck.svelte";
  import MigrationDrift from "./MigrationDrift.svelte";
  import MigrationHistory from "./MigrationHistory.svelte";

  let { report, section = "timeline", navigate } = $props();

  const SECTIONS = ["timeline", "plan", "check", "drift", "history"];

  const items = $derived([
    { id: "timeline", label: "Timeline" },
    { id: "plan", label: "Plan", badge: report?.pending > 0 ? report.pending : undefined },
    { id: "check", label: "Check" },
    { id: "drift", label: "Drift" },
    { id: "history", label: "History" },
  ]);

  const active = $derived(SECTIONS.includes(section) ? section : "timeline");
</script>

<div class="shell">
  <header class="head">
    <div class="heading">
      <span class="eyebrow">migration chain</span>
      <div class="title">
        <h1 class="page-title">Migrations</h1>
        {#if report}
          <div class="summary">
            <Status tone="success" label="{report.applied} applied" />
            {#if report.pending > 0}
              <Status tone="warning" hollow label="{report.pending} pending" />
            {/if}
          </div>
        {/if}
      </div>
    </div>
    <Segmented
      {items}
      value={active}
      label="Migration sections"
      onselect={(id) => navigate({ view: "migrations", section: id })}
    />
  </header>

  <div class="body">
    {#if active === "plan"}
      <MigrationPlan {navigate} />
    {:else if active === "check"}
      <MigrationCheck {report} />
    {:else if active === "drift"}
      <MigrationDrift {navigate} />
    {:else if active === "history"}
      <MigrationHistory />
    {:else}
      <MigrationTimeline {report} />
    {/if}
  </div>
</div>

<style>
  .shell {
    display: flex;
    flex: 1;
    flex-direction: column;
    min-height: 0;
  }

  .head {
    display: flex;
    flex: none;
    align-items: flex-end;
    justify-content: space-between;
    gap: 16px;
    padding: 6px 0 16px 8px;
  }

  .heading {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .title {
    display: flex;
    align-items: baseline;
    gap: 24px;
  }

  .summary {
    display: flex;
    align-items: center;
    gap: 20px;
  }

  .summary :global(.status) {
    font-size: var(--text-xs);
  }

  .body {
    display: flex;
    flex: 1;
    flex-direction: column;
    min-height: 0;
  }
</style>
