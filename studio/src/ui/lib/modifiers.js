import { writable } from "svelte/store";

export const altHeld = writable(false);

export function trackModifiers() {
  const update = (event) => altHeld.set(event.altKey);
  const reset = () => altHeld.set(false);
  const onKey = (event) => {
    if (event.key === "Alt") event.preventDefault();
    update(event);
  };
  window.addEventListener("keydown", onKey);
  window.addEventListener("keyup", onKey);
  window.addEventListener("blur", reset);
  document.addEventListener("visibilitychange", reset);
  return () => {
    window.removeEventListener("keydown", onKey);
    window.removeEventListener("keyup", onKey);
    window.removeEventListener("blur", reset);
    document.removeEventListener("visibilitychange", reset);
  };
}
