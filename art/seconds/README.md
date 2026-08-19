# The Book of Seconds

A generative character collection: **four halls, four kinds of champion, one
registry.** Nobody in Vaurn fights their own duel — you send a second, and the
Registry recognises exactly four kinds.

- **[LORE.md](LORE.md)** — the reform, the four Halls, and the Lantern Street affair
- **[seconds.html](seconds.html)** — the cabinet itself

## Running it

Open `seconds.html` in any browser. No build, no server, no dependencies — all
the drawing is hand-written Canvas 2D. The only network request is Google Fonts,
and the page degrades cleanly without it.

## The four collections

| Hall | Second | What it is |
|---|---|---|
| **Grudges** | Bred | A fighting beetle — horn, shell, morph, stance, temper, and a notch for every time it has been carried back |
| **Faces** | Worn | A helm and a coat of arms — helm form, crest, visor, field division, charge, tincture |
| **Hours** | Wound | A clockwork automaton — escapement, casing, winder, gear train, complication, wear |
| **Distances** | Named | A figure of stars — asterism, star count, primary magnitude, field, doubles, ecliptic crossing |

Every specimen is drawn procedurally in a 1000×1000 space, so the same routine
produces both the lit plate and the thumbnails in the casting strip.

## Serials and reproducibility

A serial *is* the specimen. `buildSpec(hall, serial)` is pure and deterministic:
traits, name, flavour, marks and drawing all derive from it, so the same serial
yields the same second forever. There is nothing to persist — a collection is a
list of numbers, and any second is recoverable by typing its serial back in.

## How rarity works, and why it isn't the obvious thing

Each trait is drawn from a weighted table, and the share printed beside a trait
is computed from those weights — not chosen by hand.

The tier is deliberately **not** the joint probability. With six traits, every
specimen has a joint probability in the range of one in tens of thousands, so
grading on it would mark essentially the whole roll as priceless. Instead each
hall is calibrated on load: 16,000 serials are drawn, each scored by
`−Σ log p`, and the scores sorted. A specimen's tier is then its **rank against
its own hall** — the share of seconds at least as unusual as it is:

| Tier | Share of the hall | Observed over 12,000 specimens |
|---|---|---|
| Common | top 100–25% | 74.7% |
| Uncommon | top 25–6% | 19.3% |
| Rare | top 6–1.2% | 4.7% |
| Superior | top 1.2–0.2% | 1.0% |
| Singular | top 0.2% | 0.23% |

The same calibration pass double-checks the printed trait shares against what
the generator actually produces, and the page reports the largest gap it found
in its own colophon — typically under one percentage point, which is ordinary
sampling error at 16,000 draws.
