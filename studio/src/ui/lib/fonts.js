import interLatin from "@fontsource-variable/inter/files/inter-latin-opsz-normal.woff2";
import interLatinExt from "@fontsource-variable/inter/files/inter-latin-ext-opsz-normal.woff2";
import monoLatin from "@fontsource-variable/jetbrains-mono/files/jetbrains-mono-latin-wght-normal.woff2";
import monoLatinExt from "@fontsource-variable/jetbrains-mono/files/jetbrains-mono-latin-ext-wght-normal.woff2";

const LATIN =
  "U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD";
const LATIN_EXT =
  "U+0100-02BA,U+02BD-02C5,U+02C7-02CC,U+02CE-02D7,U+02DD-02FF,U+1D00-1DBF,U+1E00-1E9F,U+1EF2-1EFF,U+2020,U+20A0-20AB,U+20AD-20C0,U+2113,U+2C60-2C7F,U+A720-A7FF";

const FACES = [
  ["Inter Variable", interLatin, LATIN],
  ["Inter Variable", interLatinExt, LATIN_EXT],
  ["JetBrains Mono Variable", monoLatin, LATIN],
  ["JetBrains Mono Variable", monoLatinExt, LATIN_EXT],
];

export function loadFonts() {
  if (typeof FontFace === "undefined") return;
  for (const [family, url, unicodeRange] of FACES) {
    const face = new FontFace(family, `url(${url}) format("woff2")`, {
      weight: "100 900",
      style: "normal",
      display: "swap",
      unicodeRange,
    });
    document.fonts.add(face);
  }
}
