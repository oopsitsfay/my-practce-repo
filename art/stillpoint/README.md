# Stillpoint

A generative art system: **ash settling on the silent lines of a struck plate.**

- **[PHILOSOPHY.md](PHILOSOPHY.md)** — the algorithmic manifesto
- **[LORE.md](LORE.md)** — the Order of the Ninth Confirmation, and why Kell
  settles its arguments in bronze instead of words
- **[stillpoint.html](stillpoint.html)** — the interactive engine

## Running it

Open `stillpoint.html` in any browser. Nothing to install and no server needed;
p5.js loads from a CDN, so the first load wants a network connection.

A plate takes roughly ten seconds to settle: nine successive strikes, each gentler
than the one before, with the rim ticks filling in as each confirmation lands.

## What the algorithm actually does

The plate is a superposition of standing-wave modes — classical square-plate
Chladni harmonics `cos(nπu)cos(mπv) − cos(mπu)cos(nπv)` interleaved with radial
rosette modes — summed with decaying amplitudes, all registered to one shared
centre and axis, then bent by a low-frequency noise warp so the casting has flaws.

Nothing draws the figure. Sixty thousand grains of ash each read the local
amplitude `|f|` and obey two lines of physics:

- **jitter ∝ |f|^0.75** — thrown hard where the plate is loud, not at all where it is still
- **a damped Newton step `−f·∇f/|∇f|²`** — carrying them down onto the zero-set

The black curves are simply where the ash stopped. A separate airborne fraction is
driven *uphill*, into the antinodes, and never settles — that is the copper haze,
and it is a record of where the plate refused to hold anything.

Between strikes the temperature anneals exponentially and an exponential
forgetting runs over the accumulation buffers, so earlier settlements survive as
faint, slightly misregistered ghosts beneath the final one.

**The stillpoint.** Every square mode vanishes identically at the plate's centre,
so the centre is a node of every plate ever struck — a degenerate crossing where
many nodal curves converge into a small dense knot. It appears on nearly every
seed. It is not an artefact; it is the feature the system is named for.

## Parameters

| Control | What it changes |
|---|---|
| Striking Pattern (seed) | the whole mode signature — which harmonics speak, at what weight and angle |
| Harmonics | how many modes interfere |
| Mode Order | highest harmonic the plate will hold — raise it for finer, busier figures |
| Rosette Bias | radial modes vs. square-plate modes |
| Alignment | how strictly the modes share one axis; drop it and the figure loses its symmetry |
| Ash Grain | number of particles |
| Airborne Ash | fraction that never settles (the haze) |
| Agitation | strike force |
| Settling | strength of the pull onto the nodal lines |
| Plate Warp | flaws in the casting |
| Confirmations | how many successive settlements before the record is closed |
| Ash Weight | exposure of the deposit |

Four palettes ship with it — Ash & Ember, Salt Bell, Verdigris, Obsidian — and all
three colours are individually adjustable.

## Reproducibility

A seed fixes the plate completely. The same seed re-struck at any time produces a
byte-identical image; this is verified, not assumed. Neighbouring seeds are not
neighbours — harmonic space is discrete, so seed *n* and seed *n+1* are unrelated
figures.
