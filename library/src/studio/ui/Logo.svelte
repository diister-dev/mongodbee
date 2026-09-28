<script>
  import logo from "./assets/logo.svg";

  let { size = 20 } = $props();
</script>

<img class="logo" src={logo} width={size} height={size} alt="" draggable="false" />

<style>
  .logo {
    display: block;
    flex: none;
  }
</style>
