# MongoDBee Studio: art direction

The studio gets an identity of its own instead of a generic dashboard look. The
colour and geometry come from the work of Ayush Soni (hex.inc); the calm comes
from the other references: nanda's layered frames and tone steps, nan.fyi's
editorial restraint, usrnk1's reveal-on-hover interactions and Emil Kowalski's
motion rules. Everything was studied as inspiration only; no asset or code is
reused.

## What defines his style

Twelve pieces were studied frame by frame: the Grafana AI prototypes, the
Tensorlake site and agent diagrams, Verita, Quotient, the Unsigned stamps, the
swipe file, the pixel iconography, the identity and motion tests.

- **Flat colour, square geometry.** Saturated fields (yellow, magenta, lime)
  behind hard-edged black or white panels. Almost no radius, no soft shadow.
- **Technical diagram language.** Hairline grids, dashed and dotted outlines,
  small square nodes where lines meet, connectors with square midpoints,
  labels in mono wrapped in brackets such as `[analyzing performance]`.
- **Pixel motifs.** Diamonds built from rotated squares, a vertical stack of
  five coloured squares used as a legend mark, pixelated icons.
- **Type in three voices.** A tight grotesk for statements, a mono for labels,
  codes and numbers, and sometimes a serif for editorial text.
- **Density through structure.** Many small labels, all aligned to a grid, so
  a busy composition still reads calmly.
- **Motion.** Stepped, stamp-like reveals, lines that draw, pixels that fill in
  order, nothing floaty.

## What translates to a dense data tool

Kept:

- square geometry (2 px on controls, 4 px on cards, square status markers);
- warm paper tones stepped from sidebar to canvas to sheet, crisp hairlines
  instead of shadows, and layered frames (a sheet inside a slightly darker
  ring) for the one object that heads a page;
- mono bracket labels as page eyebrows;
- the five-colour spectrum as the categorical palette and the loading
  indicator;
- colour only where it carries data: type tags, picklist values, the kind of a
  collection in the overview bars;
- stepped, short motion for state changes.

Left out:

- full-bleed or block colour: a table read for an hour needs a quiet ground,
  so even the overview numbers sit on paper, separated by hairlines;
- offset drop shadows (honey or tinted): they read as a gimmick next to data;
  depth comes from tone steps, hairlines and the ink fill instead;
- the blueprint grid as a page background: like nan.fyi's crosses, it appears
  once, behind the overview's collection bars, where it reads as a chart grid;
- dark panels as the main surface: the studio stays light;
- all-caps text: labels stay in lowercase mono;
- decorative marks that carry no data.

## The mongodbee identity: "hive blueprint"

### The mark

The leaf bee is rebuilt on a 24-unit grid to belong to the language: the leaf
becomes a faceted diamond body in forest green, crossed by two honey bands, with
four square wings in the two logo greens. Seven flat shapes, no curves, no
gradients, a clear silhouette at 16 px. The wordmark sets "mongodbee" in Inter
600, outlined to paths.

### Colour and depth

The blue accent is retired. Interactive colour is the logo's forest green,
honey stays in the mark and in data, and the warm spectrum replaces the old
type hues. Selection and depth use tone steps, not colour: the selected row and
the active item take a slightly darker paper tone with a 2 px ink edge, the
active tab is filled with ink, and the focus ring is a translucent forest.

Signature details:

1. **Square status markers**: a filled square for a state, a hollow square for
   pending, no halo and no offset.
2. **Bracket eyebrows**: `[ migrations ]` above page titles, mono and
   lowercase.
3. **Tone steps**: sidebar on the frame tone, canvas one step lighter, sheets
   lighter again; the collection header and the brand tile sit in a layered
   ring.
4. **The query sentence**: the data table's filters, scope and sort read as a
   sentence of tokens (`artwork in exposition:alpha01 where year at least 2000,
   sorted by year`), each one a control with a key and value tooltip.
5. **Spectrum loader**: five squares stepping in sequence while something
   loads, and nowhere else as decoration.
6. **Collection bars**: the overview ranks the largest collections as labelled
   bars coloured by kind, on the single blueprint grid of the product.

## Tokens

### Palette

| Token                | Value     | Use                                         |
| -------------------- | --------- | ------------------------------------------- |
| `--canvas`           | `#F4F1E8` | main area ground                            |
| `--grid-line`        | `#E9E4D6` | the overview chart grid                     |
| `--frame`            | `#EEEADD` | frames around sheets, rails, footers        |
| `--card`             | `#FFFDF8` | sheets: tables, cards, popovers             |
| `--hairline`         | `#ECE7DA` | row separators                              |
| `--card-border`      | `#DDD7C7` | sheet and control borders                   |
| `--text`             | `#141A15` | ink                                         |
| `--text-muted`       | `#5C645B` | secondary text                              |
| `--text-faint`       | `#98A093` | tertiary text, placeholders                 |
| `--accent`           | `#0B4A31` | forest: primary actions, active icons, links |
| `--accent-strong`    | `#083826` | forest, pressed                             |
| `--honey`            | `#F2C230` | the mark's bands, never a shadow            |
| `--select-bg`        | `#EDE8DA` | selected row and item, one tone step down   |
| `--select-edge`      | `#141A15` | 2 px edge on the selected row and item      |
| `--accent-ring`      | forest at 45% | focus ring                              |
| `--leaf`             | `#1F8A4C` | success                                     |
| `--sprout`           | `#35A862` | brand light green                           |
| `--warning`          | `#C77800` | warning                                     |
| `--danger`           | `#D93A22` | failure                                     |
| `--ink-panel`        | `#141A15` | tooltips, the active tab                    |
| `--spectrum-1..5`    | `#F2B81F` `#7FD13B` `#E24BD2` `#2FB5E6` `#FF5A1F` | categorical hues, spectrum mark |

### Type

| Role    | Family                  | Sizes                             |
| ------- | ----------------------- | --------------------------------- |
| Display | Inter, optical size on  | 28 px page titles, 32 px numbers, tracking -0.03em |
| UI      | Inter                   | 12, 13, 14, 16 px                 |
| Mono    | JetBrains Mono          | 11 px labels and eyebrows, 12 px data |

Inter ships as the optical-size variable cut, so large sizes take the display
drawing automatically.

### Radii, borders, elevation

- Radii: 2 px controls, 4 px cards and popovers, 0 px frames.
- Borders: 1 px hairlines everywhere, dashed 1 px for "not yet" states
  (pending migrations, missing indexes, an incomplete condition).
- Elevation: no ambient and no offset shadows. A layered ring (`0 0 0 4px`
  frame, `0 0 0 5px` frame border) lifts the one object that heads a page.
  Popovers, tooltips and the palette get one soft shadow,
  `0 12px 32px -16px rgb(20 26 21 / 0.35)`, plus their border.
- Frame corner nodes (small squares at the corners of a frame) exist as the
  opt-in `.bezel.marked` and are used sparingly.

### Motion

Emil Kowalski's rules still apply:

- curves: `--ease-out cubic-bezier(0.23, 1, 0.32, 1)`, the in-out curve for
  movement and the drawer curve;
- nothing animates on keyboard or frequent actions, and a warm tooltip opens
  instantly;
- popovers scale from their trigger;
- pressed controls scale to 0.97;
- hover styles only apply to fine pointers.

The identity adds:

- **Stamp**: a status square that changes state scales from 0.6 with a short
  ease-out (120 ms), only when the change comes from the system, such as the
  check stream.
- **Pixel loader**: five squares light up in sequence with `steps()`, the one
  constant motion in the product. It is replaced by a static row under reduced
  motion.
