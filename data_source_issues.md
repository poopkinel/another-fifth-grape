# Data-source issues — CPFTA-mandated chain feeds

Tracker for data-quality problems originating in the supermarket chains' published price/store XML feeds (the feeds mandated by `cpfta_prices_regulations`, pulled via `il-supermarket-scraper` in `app/scraper/runner.py`).

**Purpose.** Evidence base for a possible complaint to the Consumer Protection and Fair Trade Authority (הרשות להגנת הצרכן ולסחר הוגן). Each issue separates *what the chains publish* from *what we do about it downstream*, so the complaint can stay strictly about source-side problems.

**Scope.** Only issues caused by data published by the chains themselves. Bugs in our scraper, parser, geocoder, or schema are out of scope and excluded — even when they share a symptom. Where causation is mixed, we say so.

**Reproducing the queries.** All counts below were taken on 2026-04-26 against `data/fifth_grape.db`. Re-run with `sqlite3 data/fifth_grape.db "<query>"` to refresh before submitting any complaint.

**Changelog.**
- 2026-04-26 — Initial tracker. Issues #5 and #6 retracted same day on re-verification: #5 had no actual sentinel `branch_name` rows; #6 was our Pandas parser, not source.
- 2026-04-26 — Issue #1 strengthened with library-source evidence (see §1).
- 2026-04-26 — Direct-source verifications run against Shufersal, Yeinot Bitan/Carrefour, and Hazi Hinam (see §Verifications log). All three confirmed clean for `<ItemName>`; combined with our DB still showing 1,126 `nan` product names, the original §6 retraction is reinforced — the bug is in `il-supermarket-parser` or downstream, not source-side.
- 2026-04-26 — Two new high-severity findings from operator-fetched verification: §8 (Victory's StoresFull is **malformed XML**, truncated mid-document, no standard parser can read it) and §9 (Mahsani Hashuk publishes a placeholder store record with **all metadata fields empty** *and* uses a non-standard PriceFull schema — `<Prices>` root, `<Products>/<Product>` instead of `<Items>/<Item>`).

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
| 8 | Victory's published StoresFull is malformed XML (truncated, no closing tags) | **Top-tier** | **Confirmed** | The whole file — no standard parser can read it |
| 9 | Mahsani Hashuk: empty-everywhere store record + non-standard PriceFull schema | High | **Confirmed** | 1 placeholder store record observed; schema deviation affects every consumer of the feed |

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

## 8. Victory's published StoresFull is malformed XML

**What's wrong (source side).** Victory (chain_id `7290696200003`, served from `laibcatalog.co.il`) publishes a StoresFull file that is **not valid XML**. The file ends abruptly at line 646, immediately after a `</Store>` close tag, with no closing tags for any of the parent containers (`</Stores>`, `</SubChain>`, `</SubChains>`, and the root). Standard XML parsers reject the file outright; `xml.etree.ElementTree` raises `ParseError` on the unclosed elements.

**Evidence.**
- Operator-fetched on 2026-04-26 from the Mahsani / Victory portal at `https://laibcatalog.co.il/`. File `PriceFull7290696200003-...gz` (StoresFull category).
- Direct-source verification run (`scripts/verify_source.py`) crashed during `ET.fromstring(...)` with the parse error pointing at the truncation. Heuristic regex over the raw text counts `<Store>` openings consistent with hundreds of records, none of which can be reached programmatically because the parent containers never close.
- This is **not transfer corruption** — re-fetching produced the same byte sequence; the file is published in this state.

**Why it matters for the complaint.** The CPFTA regulation requires retailers to *publish* machine-readable price and store data. A file that cannot be parsed by any standard XML implementation is, in any practical sense, not published. Every consumer of the feed — price-comparison sites, regulators auditing compliance, our app — has to either custom-write a recovery parser or treat Victory's data as missing. Most will do the latter, which means consumers cannot see Victory prices through any standards-compliant tool. This is a direct violation of the publication requirement, more severe than empty fields.

**Distinguishing source vs. ours.** No ambiguity here. Our pipeline doesn't construct, transform, or rewrite the StoresFull XML; we download the chain-published `.gz` and gunzip it. The malformedness is byte-for-byte what Victory ships.

**Action items.** None on our side — we should *not* "fix" this by writing a recovery parser. Treating malformed publication as if it were valid would obscure a finding worth raising to the regulator. Tracker note that the script will continue to fail on this file is the correct behavior.

---

## 9. Mahsani Hashuk: empty placeholder store + non-standard PriceFull schema

Two issues at the same chain, found in the same operator-fetched verification on 2026-04-26.

**What's wrong (source side, A — placeholder store record).** Mahsani Hashuk's StoresFull (chain_id `7290661400001`, served from `laibcatalog.co.il`) contains 1 store record with **every consumer-facing metadata field empty**: StoreId, StoreName, Address, City all blank. This isn't an empty `Address` (issue §3) or an empty `City` (issue §2) — it's an entire row that's a placeholder. There is no way to identify or locate this store; the record provides nothing.

**What's wrong (source side, B — schema deviation).** Mahsani Hashuk's PriceFull does not follow the same schema other chains use:
- Root element is `<Prices>`, not `<Root>` / `<Envelope>` / similar.
- Items are wrapped in `<Products>` containing `<Product>` elements, not the much more common `<Items>` containing `<Item>` elements.
- Element names use mixed casing including `<ChainID>` (uppercase `D`).

**Evidence.**
- Direct-source verification run (`scripts/verify_source.py`) on 2026-04-26 reported StoresFull with `stores_total: 1` of all-empty fields. Re-running with the all-fields-empty count is straightforward (the script could be extended to surface this explicitly; see Verifications log).
- PriceFull verification reported `items_inspected: 0` — the script iterates `<Item>` elements and finds none because Mahsani uses `<Product>`. The 68 KB / ~7,000 line file is full of products under the alternate tag name (heuristic regex confirms presence). The `0` is therefore a *schema-deviation finding*, not a parser limitation we should silently work around.

**Why it matters for the complaint.** Two distinct violations:
1. Publishing rows with all metadata fields blank defeats the regulation's purpose of letting consumers identify and locate stores.
2. Schema deviation forces every consumer of Mahsani's feed to either bespoke-code for their format or fail. The CPFTA regime presupposes that the feeds are machine-readable in a uniform way; even though most non-Mahsani chains converge on a common schema by convention, *no* shared schema is enforced by the regulation. Mahsani's deviation is consistent with that legal latitude — but it's also exactly the gap the regulator should close.

**Distinguishing source vs. ours.** We download the chain-published file as-is. The empty-everywhere row and the schema deviation are both literally in the bytes Mahsani ships.

**Action items.** None on our side. The schema deviation is interesting but should not be papered over by extending our verifier to absorb it — that would normalize the violation and erase the finding. Mahsani's PriceFull `<ItemName>` empty rate is therefore unmeasured by our standard tooling; if we want that number for the complaint, the cleanest path is a one-shot grep over the raw file (`grep -cE '<ItemName(\\s/?>|>\\s*</ItemName>)' file.xml`), recorded as ad-hoc evidence rather than baked into the verifier.

---

## How to add new issues

Append a new numbered section. Keep the per-issue structure: *what's wrong (source side)*, *evidence* (queries + file refs + commits with dates), *honest separation of source vs. our pipeline*, *why it matters for the complaint*. Update the summary table.

When citing query counts, include the date the query was run — these counts move as the chains republish and as we backfill.

## Verifications log

Direct-source verifications against chain portals, recorded as we run them. Each row references which issues are addressed. Run via `scripts/verify_source.py` or by hand-fetching `.gz` files. *Source-clean* means **0** items with empty `<ItemName>` and the structural fields (ChainId, addresses) populated.

| Date | Chain | File | Result | Notes |
|------|-------|------|--------|-------|
| 2026-04-26 | Shufersal | live PriceFull (store 001) | **source-clean**: 4,434 items, 0 empty `<ItemName>` | First verification. Also tested 5 of our `nan` product_ids: 3 found in this PriceFull, all with real Hebrew names. |
| 2026-04-26 | Shufersal | controlled scrape (limit=2, temp DB) | **pipeline-clean for Shufersal-only**: 4,151 products in temp DB, 1 with `name='nan'` (0.024%) | Lone stray was barcode `7290119380053` (has unit `100 מ"ל` but no name in source — single-row source gap, not a pattern). |
| 2026-04-26 | Yeinot Bitan / Carrefour | local PriceFull (operator-fetched) | **source-clean**: 2,038 items, 0 empty `<ItemName>` | Two of our 5 Set-B `nan` barcodes found, both with names. Issue #1 (Carrefour-under-Yeinot-Bitan ChainId reuse) corroborated separately. |
| 2026-04-26 | Hazi Hinam | local StoresFull + PriceFull (operator-fetched) | **source-clean**: 7,971 items, 0 empty `<ItemName>`; 13 stores, 0 empty Address, 1 empty City (7.69%) | Hazi Hinam ships names. The 103 `nan` products in our DB whose last writer must be hazi_hinam (because they're in hazi_hinam but not mahsani_hashuk, and hazi_hinam is the 8th of 9 in our scrape order) cannot be explained by source-side empty cells. Confirms the bug is in `il-supermarket-parser` or our handling of its output for Hazi Hinam's specific XML. |
| 2026-04-26 | Mahsani Hashuk (`7290661400001`) | local StoresFull + PriceFull (operator-fetched, `laibcatalog.co.il`) | **multiple source-side defects** — see §9. StoresFull: 1 store row with all metadata fields blank (StoreId, name, address, city). PriceFull: non-standard schema — root `<Prices>` (not `<Root>`), items under `<Products>/<Product>` (not `<Items>/<Item>`), `<ChainID>` casing. Script reports 0 items inspected because it iterates `<Item>` only — that 0 is itself the schema-deviation finding, not a parsing limitation we should paper over. | New §9. |
| 2026-04-26 | Victory (`7290696200003`) | local StoresFull (operator-fetched, `laibcatalog.co.il`) | **malformed XML at source** — see §8. File ends at line 646 after `</Store>` with no closing tags for parents (`</Stores>`, `</SubChain>`, `</SubChains>`, `</Root>` or whatever the wrapper is). Standard XML parsers reject the file. PriceFull not yet inspected because the StoresFull parse failure aborted the run. | New §8. |

**Implication for Issue #6.** Three of our nine chains (Shufersal, Yeinot Bitan, Hazi Hinam) are now confirmed to ship product names cleanly at source. Yet our production DB has 1,126 product rows with `name='nan'`, all touched by either hazi_hinam (1,126) or mahsani_hashuk (1,023) — and 103 are in hazi_hinam alone. Since hazi_hinam's source is clean, those 103 are **lost in our pipeline** between gunzip and DB write. The `il-supermarket-parser` library (or its hazi_hinam-specific sub-class) is the prime suspect; chain-specific because Shufersal's pipeline (also MultiPageWeb engine) is clean. Worth localizing before treating Issue #6 as anything other than retracted.

**Outstanding source verifications (next session):** Mahsani's PriceFull `<ItemName>` rate (the script reported 0 items because of the `<Products>/<Product>` schema deviation — we'd need to either grep the raw file by hand or re-run with a schema-tolerant inspector if we choose to build one). A Cerberus chain (Rami Levy / Yohananof / Osher Ad / Tiv Taam) for parity across engines — these need FTP-style auth so they're harder. Victory's PriceFull also still untested (StoresFull parse failure aborted that run).

## How to verify against the raw source

Use `scripts/verify_source.py` for any complaint claim. It bypasses the DB and the parsing library entirely — fetches a chain's `.gz` files, gunzips them, parses XML, runs per-issue checks, and writes `verification.md` + the raw bytes to an output directory.

```bash
python scripts/verify_source.py STORES_URL PRICE_URL --expected-chain-id ID --out /tmp/evidence
```

Most chain portals are geo-blocked from our VPS (Hetzner DE) — only Shufersal is reachable. Run from a local Israeli IP for everything else. URL discovery isn't automated because every portal has its own UI; paste the URL by hand from the chain's portal page.

Reports generated by the script are intended to be attached as evidence when this tracker is escalated to a CPFTA complaint.
