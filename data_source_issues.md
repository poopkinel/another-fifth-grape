# Data-source issues — CPFTA-mandated chain feeds

Tracker for data-quality problems originating in the supermarket chains' published price/store XML feeds (the feeds mandated by `cpfta_prices_regulations`, pulled via `il-supermarket-scraper` in `app/scraper/runner.py`).

**Purpose.** Evidence base for a possible complaint to the Consumer Protection and Fair Trade Authority (הרשות להגנת הצרכן ולסחר הוגן). Each issue separates *what the chains publish* from *what we do about it downstream*, so the complaint can stay strictly about source-side problems.

**Scope.** Only issues caused by data published by the chains themselves. Bugs in our scraper, parser, geocoder, or schema are out of scope and excluded — even when they share a symptom. Where causation is mixed, we say so.

**Reproducing the queries.** All counts below were taken on 2026-04-26 against `data/fifth_grape.db`. Re-run with `sqlite3 data/fifth_grape.db "<query>"` to refresh before submitting any complaint.

**Changelog.**
- 2026-04-26 — Initial tracker. Issues #5 and #6 retracted same day on re-verification: #5 had no actual sentinel `branch_name` rows; #6 was our Pandas parser, not source.
- 2026-04-26 — Issue #1 strengthened with library-source evidence (see §1).

**Verification methods available without re-scraping.**

1. **DB queries**: re-run the SQL in each section against the current SQLite at `data/fifth_grape.db`. Cheap; reflects last scrape (2026-04-10 as of this writing).
2. **Direct chain portal fetch (clean snapshot)**: chain feeds are hosted on per-chain portals, *not* on gov.il. The `il-supermarket-scraper` package at `.venv/lib/python3.12/site-packages/il_supermarket_scarper/` is effectively the public registry — each `scrappers/<name>.py` hardcodes the chain's `chain_id` and portal infix. Confirmed reachable from this VPS: `https://prices.shufersal.co.il/` (HTTP 200 on 2026-04-26, lists today's gz files at `pricesprodpublic.blob.core.windows.net/price/Price7290027600007-…`). Some chain portals (`prices.carrefour.co.il/` tested) timed out from this host — the library's README warns "some scrapers' sites are blocked from being accessed from outside of Israel."
3. **gov.il regulation page**: returns 403 to non-browser fetchers; not useful as a programmatic source. It's regulation, not a data registry.

---

## Summary table

| # | Issue | Severity | Source-side confidence | Stores / records affected |
|---|-------|----------|------------------------|---------------------------|
| 1 | Carrefour stores published under the Yeinot Bitan feed | High | **Confirmed** | Whole sub-chain |
| 2 | Empty `city` field in store records | High | **Confirmed** | 739 originally; 214 still empty |
| 3 | Empty `address` field in store records | High | **Confirmed** | 157 stores |
| 4 | Suspicious "ghost" stores at empty/placeholder addresses across many chains | High | **Confirmed** for empty-address clusters; *suspected* placeholder pattern at real addresses | 531 (address, city) tuples shared by ≥2 chains; multiple addresses shared by 8–9 chains |
| 5 | ~~Test/sentinel values in `branch_name`~~ | — | **Withdrawn** — see §5 | 0 |
| 6 | ~~Barcoded products with no product name in feed~~ | — | **Withdrawn** — see §6 | 0 source-side; 1,126 our parser |
| 7 | High Places "not-at-address" failure rate, smaller chains | Medium-Low | *Suspected* — needs disambiguation from Places coverage gaps | See per-chain table in §7 |

---

## 1. Carrefour stores published under the Yeinot Bitan feed

**What's wrong (source side).** Carrefour-branded stores are published inside the `yeinot_bitan` chain feed instead of in their own `chain_id`. The feed's `chainid` field for these records is Yeinot Bitan's, even though the physical stores operate under the Carrefour brand and are signed as such.

**Evidence.**
- `app/scraper/chains.py:13` — registry comment: `"yeinot_bitan": (ScraperFactory.YAYNO_BITAN_AND_CARREFOUR.name, "יינות ביתן")` — i.e. one feed for both chains.
- `README.md:40` — documents this co-publication in plain text.
- `app/main.py:51–59` — `_display_chain_name()` overrides `chain_id == "yeinot_bitan"` to display "קרפור" when Places confirms the on-the-ground chain is Carrefour. Pure workaround for source mislabeling.
- Commit `ba771a9` (2026-04-21) added a verification retry that successfully re-resolves Carrefour locations like store id 2518.
- **Library-source corroboration (2026-04-26):** `il_supermarket_scarper/scrappers/bitan.py:5–15` defines `class YaynotBitanAndCarrefour(PublishPrice)` with hardcoded `chain_id="7290055700007"` (Yeinot Bitan's GS1 company prefix) and `site_infix="carrefour"` (i.e. fetches from `https://prices.carrefour.co.il/`). The library uses `chain_id` to *filter* what the source publishes; for that filter to find Carrefour stores in the Carrefour portal's XML, the source must be tagging Carrefour stores with `<ChainId>7290055700007</ChainId>` (Yeinot Bitan's). This means the mislabel is **in the chain's published XML**, not introduced by the library or by us.

**Why it matters for the complaint.** The CPFTA transparency regime presumes the consumer can identify which retailer's price they're looking at. Co-publication forces consumer-facing apps to invent ad-hoc heuristics (here, a Google Places lookup) just to recover the brand identity. A clean fix is one of: a separate `carrefour` `chainid`, or a documented `subchainid` that distinguishes the brand. Neither is in place today.

---

## 2. Empty `city` field in store records

**What's wrong (source side).** Many `StoresFull` records have `city` as an empty string. Without a city, an address like "המכבי 81" is ambiguous — the same street name exists in dozens of Israeli cities — and any consumer-facing geocoding will misplace the store, sometimes by tens of kilometres.

**Evidence.**
- `README.md:131–132` — known limitation: "stores whose `city` column is empty get geocoded to whichever city's street-name match the geocoder prefers first — frequently Tel Aviv". Current count: `SELECT COUNT(*) FROM stores WHERE city = ''` → **214** as of 2026-04-26.
- Commit `08f7c44` (2026-04-21) — backfill effort: started with 739 empty-city stores, recovered 525 via cross-chain address inference, leaves 214 unrecoverable from feed contents alone.
- Concrete real-user impact: `SESSION_HANDOFF.md:52–59` documents a Carrefour Petah Tikva branch (`yeinot_bitan_2750`) that geocoded to Tel Aviv (~15 km off) and showed up as 2.4 km from a Tel Aviv user.

**Distinguishing source vs. ours.** The empty `city` arrives that way in the chain XML. Our backfill is a workaround.

---

## 3. Empty `address` field in store records

**What's wrong (source side).** 157 store records have `address = ''`. With no street address, the store cannot be located on a map and cannot be verified against a registry like Google Places.

**Evidence.**
- `SELECT COUNT(*) FROM stores WHERE address = ''` → **157** on 2026-04-26.
- Co-occurs heavily with empty `city` and with the multi-chain "ghost address" pattern in §4.

---

## 4. Multi-chain "ghost stores" at empty / placeholder addresses

**What's wrong (source side).** 531 distinct `(address, city)` tuples are claimed by stores from two or more chains. Most extreme cases have the same `(address, city)` tuple appearing across 8–9 chains:

| Address | City | # chains claiming a store there |
|---------|------|---------------------------------|
| `''` | `''` | 9 |
| `''` | `באר שבע` | 9 |
| `''` | `נתיבות` | 9 |
| `''` | `אופקים` | 8 |
| `''` | `אפרת` | 8 |

The empty-address rows are clearly garbage records — there is no real address. The pattern of *every* major chain happening to publish a store at the empty-address-in-Be'er-Sheva placeholder is hard to read as anything other than systematic placeholder/test entries leaking into the public feed.

**At a real address — *suspected*, not confirmed.** Address `יהודה מכבי 81, ת"א` (a Shufersal Express that all eight other chains' feeds *also* claim under `store_id = 305`) might be the same pattern at a real location, or it might be coincidence. A full audit of which other addresses are shared *and* have suspiciously round/recurring `store_id` values would strengthen the complaint here. Not done yet.

**Workaround on our side.** `verify_stores.py` (clustering at lines 180–420) runs Places lookups on every multi-chain address and tags rows that don't match (`verified_by_places = 'not_at_address'`); the API filters them out. It's a Google Maps API bill we wouldn't be paying if the feeds were clean.

---

## 5. ~~Test / sentinel values in `branch_name`~~ — Withdrawn 2026-04-26

**Status.** Retracted. Direct query `SELECT COUNT(*) FROM stores WHERE branch_name IN ('','0','TEST','בדיקה')` returned **0** on 2026-04-26. The original "331 rows" figure was a count of stores with empty `address` OR empty `city` (issues #2, #3) — the explore agent that drafted this issue listed the sentinel `branch_name` values its query was *checking for* and treated them as values it had *found*. They are not present.

Keeping this section as a visible retraction so anyone re-reading the doc sees the correction; do not cite the original claim in any complaint.

---

## 6. ~~Barcoded products with no product name in feed~~ — Withdrawn 2026-04-26

**Status.** Retracted as a source-side issue. The 1,126 `name = 'nan'` rows in our DB are what Pandas writes when it reads a missing/empty cell with `dtype=object` and we then `str()` the value. That's *our parser*, not the chains. To prove this is source-side we would need to inspect raw `PriceFull` XML and confirm `<ItemName>` elements are actually empty for those barcodes — that work has not been done. Until it is, this claim doesn't belong in a complaint.

**Action item for our side.** Fix the parser to skip or null missing names instead of writing `'nan'`. Tracked separately from this complaint doc.

---

## 7. High Places "not-at-address" rates — smaller chains

**What's wrong (suspected source side).** When we cluster stores by `(address, city)` and ask Google Places "what supermarkets are at this address?", the smaller chains fail to match Places far more often than Shufersal:

```
SELECT chain_id, COUNT(*) FROM stores
WHERE verified_by_places = 'not_at_address'
GROUP BY chain_id ORDER BY 2 DESC;

hazi_hinam      463
yeinot_bitan    387
osher_ad        386
yohananof       384
victory         382
tiv_taam        379
rami_levy       360
shufersal        93
mahsani_hashuk   32
```

**Why this is *suspected* not confirmed.** Two competing explanations:

1. The chains publish stores that don't actually exist or are mislocated, and Places (which is a fairly accurate registry) correctly fails to find them.
2. Places has worse coverage of small Israeli supermarket chains than of Shufersal, and is generating false negatives.

To turn this from suspicion into evidence, we'd need to manually walk a sample of, say, 20 `not_at_address` rows per chain and check on the ground (or via independent registries). Not done. Until then this is a soft signal, not complaint-grade.

---

## How to add new issues

Append a new numbered section. Keep the per-issue structure: *what's wrong (source side)*, *evidence* (queries + file refs + commits with dates), *honest separation of source vs. our pipeline*, *why it matters for the complaint*. Update the summary table.

When citing query counts, include the date the query was run — these counts move as the chains republish and as we backfill.

## How to verify against the raw source

Use `scripts/verify_source.py` for any complaint claim. It bypasses the DB and the parsing library entirely — fetches a chain's `.gz` files, gunzips them, parses XML, runs per-issue checks, and writes `verification.md` + the raw bytes to an output directory.

```bash
python scripts/verify_source.py STORES_URL PRICE_URL --expected-chain-id ID --out /tmp/evidence
```

Most chain portals are geo-blocked from our VPS (Hetzner DE) — only Shufersal is reachable. Run from a local Israeli IP for everything else. URL discovery isn't automated because every portal has its own UI; paste the URL by hand from the chain's portal page.

Reports generated by the script are intended to be attached as evidence when this tracker is escalated to a CPFTA complaint.
