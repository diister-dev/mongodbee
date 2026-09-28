<script>
  import Icon from "./Icon.svelte";
  import { RefreshCw, SearchX, ServerOff, TriangleAlert, Unplug } from "./lib/icons.js";

  let { error, title, onretry, compact = false, children } = $props();

  const facts = $derived.by(() => {
    if (!error) return { message: "" };
    if (typeof error === "string") return { message: error };
    return {
      message: error.message ?? String(error),
      status: error.status ?? null,
      path: error.path ?? "",
      transport: Boolean(error.transport),
      method: error.method ?? "GET",
    };
  });

  const kind = $derived.by(() => {
    if (facts.transport) {
      return { icon: Unplug, title: "The studio server did not answer", hint: "Check that mongodbee studio is still running, then retry." };
    }
    if (facts.status === 404) return { icon: SearchX, title: "Not found" };
    if (facts.status === 409) return { icon: TriangleAlert, title: "Busy" };
    if (facts.status !== null && facts.status >= 500) return { icon: ServerOff, title: "The server failed" };
    if (facts.status !== null && facts.status >= 400) return { icon: TriangleAlert, title: "The request was refused" };
    return { icon: TriangleAlert, title: "Something went wrong" };
  });

  const code = $derived(
    [facts.transport ? "no response" : facts.status !== null ? `HTTP ${facts.status}` : "", facts.path ? `${facts.method} ${facts.path}` : ""]
      .filter(Boolean)
      .join(" · "),
  );
</script>

<div class="error-state" class:compact role="alert">
  <span class="glyph" aria-hidden="true"><Icon icon={kind.icon} size={compact ? 14 : 18} /></span>
  <div class="text">
    <p class="title">{title ?? kind.title}</p>
    {#if code}<p class="code mono">{code}</p>{/if}
    {#if facts.message && facts.message !== code}<p class="message">{facts.message}</p>{/if}
    {#if kind.hint}<p class="hint">{kind.hint}</p>{/if}
    {#if onretry || children}
      <div class="actions">
        {#if onretry}
          <button class="control" type="button" onclick={onretry}>
            <Icon icon={RefreshCw} size={13} />
            Retry
          </button>
        {/if}
        {@render children?.()}
      </div>
    {/if}
  </div>
</div>

<style>
  .error-state {
    display: flex;
    align-items: flex-start;
    gap: 14px;
    max-width: 560px;
    margin: 48px auto;
    padding: 18px 20px;
    border: 1px solid color-mix(in srgb, var(--danger) 30%, var(--card-border));
    border-radius: var(--radius-card);
    background: var(--card);
  }

  .error-state.compact {
    max-width: none;
    margin: 0;
    padding: 12px 14px;
    gap: 10px;
  }

  .glyph {
    display: inline-flex;
    flex: none;
    align-items: center;
    justify-content: center;
    width: 34px;
    height: 34px;
    background: color-mix(in srgb, var(--danger) 10%, var(--card));
    color: var(--danger);
  }

  .compact .glyph {
    width: 26px;
    height: 26px;
  }

  .text {
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
  }

  p {
    margin: 0;
  }

  .title {
    color: var(--text);
    font-weight: 600;
  }

  .code {
    color: var(--text-faint);
    font-size: 11px;
    overflow-wrap: anywhere;
  }

  .message {
    color: var(--text-muted);
    font-size: var(--text-sm);
    overflow-wrap: anywhere;
  }

  .hint {
    color: var(--text-faint);
    font-size: var(--text-xs);
  }

  .actions {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 8px;
  }
</style>
