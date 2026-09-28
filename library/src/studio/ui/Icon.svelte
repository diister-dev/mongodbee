<script>
  let { icon, size = 16, stroke = 1.5, label } = $props();

  function attributes(attrs) {
    return Object.entries(attrs)
      .map(([key, value]) => `${key}="${String(value).replaceAll('"', "&quot;")}"`)
      .join(" ");
  }

  const markup = $derived(icon.map(([tag, attrs]) => `<${tag} ${attributes(attrs)}></${tag}>`).join(""));
</script>

<svg
  class="icon"
  width={size}
  height={size}
  viewBox="0 0 24 24"
  fill="none"
  stroke="currentColor"
  stroke-width={stroke}
  stroke-linecap="round"
  stroke-linejoin="round"
  aria-hidden={label ? undefined : "true"}
  aria-label={label}
  role={label ? "img" : undefined}
>
  {@html markup}
</svg>

<style>
  .icon {
    flex: none;
    display: block;
  }
</style>
