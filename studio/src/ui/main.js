import "./styles.css";
import { mount } from "svelte";
import App from "./App.svelte";
import { loadFonts } from "./lib/fonts.js";

loadFonts();
mount(App, { target: document.getElementById("app") });
