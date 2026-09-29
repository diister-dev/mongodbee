<script>
  import { onMount, tick } from "svelte";
  import { formatDateTime, plural } from "./lib/format.js";
  import { ChevronRight } from "./lib/icons.js";
  import { expand } from "./lib/motion.ts";
  import Icon from "./Icon.svelte";
  import OperationStack from "./OperationStack.svelte";
  import Status from "./Status.svelte";
  import Tooltip from "./controls/Tooltip.svelte";
  import Duration from "./values/Duration.svelte";
  import MigrationId from "./values/MigrationId.svelte";

  let { report } = $props();

  let expanded = $state({});
  let list = $state();

  const TONE = { applied: "success", pending: "warning", failed: "danger", reverted: "neutral" };
  const LABEL = { applied: "applied", pending: "pending", failed: "failed", reverted: "reverted" };

  const entries = $derived(report?.migrations ?? []);

  function runKey(migration) {
    if (migration.missingFile) return "missing";
    if (migration.status !== "applied") return `status:${migration.status === "reverted" ? "reverted" : "pending"}`;
    const raw = migration.appliedAt?.$date;
    return raw ? `run:${String(raw).slice(0, 16)}` : "run:unknown";
  }

  function runTitle(key, first) {
    if (key === "missing") return "Recorded in the database, file missing";
    if (key === "status:pending") return "Not applied yet";
    if (key === "status:reverted") return "Reverted";
    if (key === "run:unknown") return "Applied";
    return `Applied ${formatDateTime(first.appliedAt.$date).replace(/:\d\d UTC$/, " UTC")}`;
  }

  const groups = $derived.by(() => {
    const result = [];
    for (const migration of entries) {
      const key = runKey(migration);
      const last = result[result.length - 1];
      if (last && last.key === key) last.items.push(migration);
      else result.push({ key, items: [migration] });
    }
    return result.map((group, index) => ({
      ...group,
      id: `${group.key}#${index}`,
      title: runTitle(group.key, group.items[0]),
      raised: !group.key.startsWith("run:"),
    }));
  });

  function toggle(id) {
    expanded = { ...expanded, [id]: !expanded[id] };
  }

  onMount(async () => {
    await tick();
    const first = list?.querySelector("[data-raised]");
    first?.scrollIntoView({ block: "center" });
  });
</script>

<div class="page">
  {#if entries.length === 0}
    <div class="bezel"><div class="bezel-card"><div class="empty-state">No migrations</div></div></div>
  {:else}
    <section class="bezel">
      <div class="bezel-card" bind:this={list}>
        {#each groups as group (group.id)}
          <div class="group">
          <div class="group-head" class:raised={group.raised} data-raised={group.raised ? "" : undefined}>
            <span class="group-title">{group.title}</span>
            <span class="group-count">{plural(group.items.length, "migration")}</span>
          </div>
          {#each group.items as migration (migration.id)}
            {@const open = !!expanded[migration.id]}
            {@const position = migration.position === null ? null : migration.position + 1}
            <div class="entry" class:open>
              <button class="row" type="button" onclick={() => toggle(migration.id)} aria-expanded={open}>
                <span class="index num">{position ?? ""}</span>
                <span class="status">
                  <Status
                    tone={TONE[migration.status] ?? "neutral"}
                    hollow={migration.status === "pending"}
                    label={LABEL[migration.status] ?? migration.status}
                  />
                </span>
                <span class="name">
                  <Tooltip text={migration.name} overflow><span class="ellipsis">{migration.name}</span></Tooltip>
                  {#each migration.properties as property}
                    <span class="tag" class:danger={property === "irreversible"} class:warning={property === "lossy"}>{property}</span>
                  {/each}
                  {#if migration.compileError}<span class="tag danger">does not compile</span>{/if}
                  {#if migration.error}<span class="tag danger">error</span>{/if}
                </span>
                <span class="file"><MigrationId value={migration.fileName ?? migration.id} /></span>
                <span class="facts">
                  {#if !migration.missingFile}<span>{plural(migration.operations.length, "operation")}</span>{/if}
                  {#if migration.duration}<Duration ms={migration.duration} />{/if}
                </span>
                <span class="caret" class:open><Icon icon={ChevronRight} size={12} /></span>
              </button>
              {#if open}
                <div class="details" in:expand out:expand={{ exit: true }}>
                  <div class="details-inner">
                    {#if migration.appliedAt?.$date}
                      <p class="small muted">Applied {formatDateTime(migration.appliedAt.$date)}</p>
                    {/if}
                    {#if migration.revertedAt?.$date}
                      <p class="small muted">Reverted {formatDateTime(migration.revertedAt.$date)}</p>
                    {/if}
                    {#if migration.error}<div class="notice danger">{migration.error}</div>{/if}
                    {#if migration.compileError}<div class="notice danger">{migration.compileError}</div>{/if}
                    {#if migration.operations.length > 0}
                      <OperationStack operations={migration.operations} />
                    {:else if !migration.missingFile}
                      <p class="small faint">No operations.</p>
                    {/if}
                  </div>
                </div>
              {/if}
            </div>
          {/each}
          </div>
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
    min-height: 0;
    padding-bottom: 24px;
    overflow: auto;
  }

  .page > :global(.bezel) {
    flex: none;
  }

  .group-head {
    position: sticky;
    top: 0;
    z-index: 1;
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
    padding: 10px 16px 6px;
    border-bottom: 1px solid var(--hairline);
    background: var(--frame);
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: 11px;
  }

  .group-head.raised {
    color: var(--warning);
  }

  .group-count {
    color: var(--text-faint);
  }

  .row {
    display: grid;
    grid-template-columns: 28px 100px minmax(180px, 1.2fr) minmax(0, 1.4fr) auto 14px;
    align-items: center;
    gap: 12px;
    width: 100%;
    min-height: 36px;
    padding: 0 16px 0 8px;
    border: 0;
    border-bottom: 1px solid var(--hairline);
    background: none;
    color: inherit;
    font: inherit;
    text-align: left;
    cursor: pointer;
  }

  .entry.open .row {
    background: var(--select-bg);
    box-shadow: inset 2px 0 0 var(--select-edge);
  }

  .index {
    color: var(--text-faint);
    font-size: 11px;
    text-align: right;
  }

  .status :global(.status) {
    font-size: var(--text-xs);
  }

  .name {
    display: flex;
    align-items: center;
    gap: 8px;
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
    gap: 12px;
    color: var(--text-muted);
    font-size: var(--text-xs);
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

  .details {
    border-bottom: 1px solid var(--hairline);
    background: var(--frame);
  }

  .details-inner {
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 10px 16px 14px 48px;
  }

  .details-inner p {
    margin: 0;
  }

  .small {
    font-size: var(--text-xs);
  }

  @media (hover: hover) and (pointer: fine) {
    .row:hover {
      background: var(--bg-active);
    }
  }

  @media (max-width: 900px) {
    .row {
      grid-template-columns: 28px 100px minmax(0, 1fr) auto 14px;
    }

    .file {
      display: none;
    }
  }
</style>
