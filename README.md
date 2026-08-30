# indigenous-feed

Invasion of native peoples, worldwide, in 25 languages: territory, rights, settler-state policy, and
what is done to homelands.

`harvest_indigenous.py` runs every two hours in GitHub Actions, reads 166 wires, keeps what concerns
a people and a matter of ground, marks whether ground was taken or held, grades the evidence, tags
by subject and region, and writes `wire_indigenous.json`. `index.html` loads that file and renders
it.

Nothing here rewrites a headline. Titles and snippets are the publishers' own, truncated but never
reworded, and every row keeps its original link. No model in the pipeline, no API key, no paid
service, no dependencies beyond the Python standard library.

## Both directions

**Ground taken** — concessions granted over territory, roads and dams and mines, eviction and
displacement, contamination of rivers and land, leaders killed, protest criminalised, consent
overridden.

**Ground held or returned** — land titled and demarcated, treaties settled, courts ruling for
communities, co-management and self-government agreed, ancestors and heritage repatriated,
moratoria won.

Every row is marked, and the *Direction* filter separates them. A story can be both.

## Standing

| Standing | What it covers |
|---|---|
| Bodies & courts | UN mechanisms, OHCHR, regional human-rights courts, legal news services |
| Indigenous media & orgs | APTN, ICT News, National Indigenous Times, Cultural Survival, IWGIA, Amazon Watch |
| Research & rights groups | Human Rights Watch, Amnesty, Global Witness |
| Field press | Mongabay in three languages, Dialogue Earth |
| Press | General news across 25 languages |

Indigenous-run outlets are labelled so they can be read on their own — that is the point of the
label.

## What gets in

A story has to name **a people** and **a matter of ground**. Either alone is not enough: a language
festival with no territorial dimension does not qualify, and a mining permit with no community
attached belongs to a different feed. Environmental harm counts when it lands on a homeland.

Refused: *native* in its software, advertising, linguistic and botanical senses; *tribal* as a
design or reality-television term; and the sports franchises carrying these words, which otherwise
flood every query.

## Evidence

| Signal | Worth |
|---|---|
| A documented act: ruling, title granted, eviction, killing, concession | 2 |
| Institutional material: UN mechanism, court, HRW, Global Witness, IWGIA, satellite monitoring, peer review | 2 |
| A measured extent: hectares, families, per cent | 1 |
| A pending decision | 1 |
| A named place | 1 |
| Primary source | 1 |

At **3** or more the row is marked well documented, and the *Evidence* filter narrows to those.

## Region, three levels deep

Placement runs region → subregion → place: 10 regions, 26 subregions, 134 places. Pick a region and
a second row opens with its subregions; pick one and a third lists the places inside it. Latin
America → Amazon Basin → Brazilian Amazon. Oceania → Australia → Northern Australia. Europe →
Nordic & Arctic Europe → Sweden. A story naming a place files under everything above it, and each
row is labelled with the most specific placement known.

Every chip carries its own count, so an empty subregion shows as empty rather than vanishing.

## Ten subjects

Territory & titling, Extraction & infrastructure, Courts & treaties & policy, Defenders &
criminalisation, Peoples in isolation, Homelands & environment, Eviction & displacement, Culture &
heritage & language, Health & wellbeing, Recognition & self-rule.

## Files

| File | Path in repo | What it is |
|---|---|---|
| `index.html` | `/index.html` | The feed page. Pages serves the repo root, so it must carry this name. |
| `harvest_indigenous.py` | `/harvest_indigenous.py` | The harvester. Self-contained. |
| `sources_indigenous.json` | `/sources_indigenous.json` | The wire list, with each wire's standing. |
| `wire_indigenous.json` | `/wire_indigenous.json` | The output the page reads. Empty placeholder until the first run. Never hand-edit. |
| `indigenous-feed-weebly-embed.html` | `/indigenous-feed-weebly-embed.html` | The page wrapped for a Weebly Embed Code element. |
| `verify_sources.py` | `/verify_sources.py` | Reports which wires answer and which are dead. Run it from the Actions tab. |
| `README.md` | `/README.md` | This file. |
| `harvest.yml` | `/.github/workflows/harvest.yml` | Runs every two hours at :13 and commits the wire. |
| `verify.yml` | `/.github/workflows/verify.yml` | The manual wire check. |

## Setup

1. Push these files to the repository root.
2. Settings → Actions → General → Workflow permissions → **Read and write permissions**, save.
3. Actions tab → **Harvest the Indigenous peoples wire** → *Run workflow*.
4. Settings → Pages → **Deploy from a branch**, branch `main`, folder `/ (root)`.
5. Confirm
   `https://raw.githubusercontent.com/WelcomeToYourGalaxy/indigenous-feed/main/wire_indigenous.json`
   loads in a browser.

If the repository is named something else, change `REPO` near the top of the feed script in
`index.html` and regenerate the embed.

## Limits worth knowing

The gate is mechanical: it reads words, not meaning. The term list cannot hold every one of the
world's several thousand Indigenous nations — peoples whose names it does not carry are found only
when a story also uses a general term. Add the ones you follow to `PEOPLE` in
`harvest_indigenous.py`; that list is meant to grow.

Standing is assigned per wire rather than per article. Google News caps a query at roughly 100
results over about 45 days. Coverage is uneven by language and the counts show it rather than
hiding it.

## Running it locally

```bash
python3 harvest_indigenous.py              # full run
python3 harvest_indigenous.py --dry-run    # harvest and report, write nothing
python3 verify_sources.py                  # which wires are dead
```

Python 3.9 or later.
