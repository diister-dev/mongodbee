<script lang="ts">
  import Tooltip from "../controls/Tooltip.svelte";

  interface Props {
    field?: string;
  }

  let { field }: Props = $props();
</script>

<Tooltip text={field ? `${field} is absent from this document` : "Absent from this document"} block
  ><span class="absent" aria-label="absent"></span></Tooltip
>

<style>
  .absent {
    display: inline-block;
    width: 100%;
    min-width: 12px;
    height: 16px;
    vertical-align: middle;
  }
</style>
