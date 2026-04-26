# Data-source issues — CPFTA-mandated chain feeds

Tracker for data-quality problems originating in the supermarket chains' published price/store XML feeds (the feeds mandated by `cpfta_prices_regulations`, pulled via `il-supermarket-scraper` in `app/scraper/runner.py`).

**Purpose.** Evidence base for a possible complaint to the Consumer Protection and Fair Trade Authority (הרשות להגנת הצרכן ולסחר הוגן).

**Strict scope: direct-source evidence only.** A claim only graduates into a numbered Confirmed issue (§1, §2, …) when we have eyes on the source XML bytes and the defect is visible in those bytes. Numbers from our own SQLite DB are not regulator-grade — they could be partially or wholly caused by our pipeline (parser, scraper, schema cleaning) — and live in a separate "DB signals worth investigating at source" research log below. The complaint cites only sections marked Confirmed.

**Changelog.**
- 2026-04-26 — Initial tracker.
- 2026-04-26 — §5 (sentinel `branch_name`) and §6 (nameless products) retracted on re-verification: §5 had no actual sentinel rows; §6 was our Pandas parser, not source.
- 2026-04-26 — §1 strengthened with library-source evidence.
- 2026-04-26 — §8 (Victory malformed XML) and §9 (Mahsani placeholder + schema deviation) added from operator-fetched verifications.
- 2026-04-26 — **Restructure to direct-source-only.** §3 (empty `<Address>`, was 157 in our DB), §4 (multi-chain ghost stores, was 531 tuples in our DB) and §7 (Places mismatch rate) moved out of Confirmed because they were derived from DB queries, not direct-source observation. §2 (empty `<City>`) narrowed from "214 in our DB" to "Hazi Hinam StoresFull, 1 of 13 stores directly observed". The DB-derived patterns continue as candidates in the research log; each can graduate back to Confirmed with a per-chain source verification.

**Verification methods available without re-scraping.**

1. **Direct chain portal fetch (clean snapshot)**: chain feeds are hosted on per-chain portals, *not* on gov.il. The `il-supermarket-scraper` package at `.venv/lib/python3.12/site-packages/il_supermarket_scarper/` is effectively the public registry — each `scrappers/<name>.py` hardcodes the chain's `chain_id` and portal infix. Most chain portals are geo-blocked from our VPS (Hetzner DE); operator runs them locally.
2. **DB queries** against `data/fifth_grape.db`: useful for *finding candidates* worth source-verifying. Not direct evidence by themselves.
3. **gov.il regulation page**: the regulation, not a data registry. Returns 403 to non-browser fetchers.

---

## Summary table

| # | Issue | Direct-source confidence | Per-chain evidence |
|---|-------|--------------------------|--------------------|
| 1 | Yeinot Bitan publishes Carrefour-branded stores under its own ChainId | **Confirmed** | Library source (`bitan.py:5–15`); Carrefour PriceFull root `<ChainId>=7290055700007` (operator-fetched 2026-04-26) |
| 2 | Empty `<City>` field shipped in StoresFull | **Confirmed for Hazi Hinam (1 of 13 stores)** | Hazi Hinam StoresFull, operator-fetched 2026-04-26 from `shop.hazi-hinam.co.il/Prices` |
| 3 | (former DB-only finding — moved to research log) | — | — |
| 4 | (former DB-only finding — moved to research log) | — | — |
| 5 | ~~Sentinel branch_name values~~ | **Withdrawn** 2026-04-26 | — |
| 6 | ~~Nameless barcoded products~~ | **Withdrawn** 2026-04-26 | — |
| 7 | (former suspected pattern — moved to research log) | — | — |
| 8 | Victory's StoresFull is malformed XML (truncated, no closing tags) | **Confirmed (top-tier severity)** | Victory StoresFull, operator-fetched 2026-04-26 from `laibcatalog.co.il`; lxml strict parser rejects |
| 9 | Mahsani Hashuk: placeholder store record + non-standard PriceFull schema | **Confirmed** | Mahsani StoresFull + PriceFull, operator-fetched 2026-04-26 from `laibcatalog.co.il` |

The numbering gaps (3, 4, 7) are deliberate — those positions previously held DB-only claims that have been demoted to the research log; numbers are kept stable so prior commit / handoff references resolve to the right thing.

---

## 1. Carrefour stores published under the Yeinot Bitan ChainId

**What's wrong (source side).** Carrefour-branded stores are published in the Carrefour transparency portal but tagged with Yeinot Bitan's GS1 company prefix as `<ChainId>`. Consumer-facing brand identity is not preserved at the ChainId level. SubChain-level fields could carry the brand distinction but are not consistently populated.

**Direct-source evidence.**
- `il_supermarket_scarper/scrappers/bitan.py:5–15` defines `class YaynotBitanAndCarrefour(PublishPrice)` with hardcoded `chain_id="7290055700007"` (Yeinot Bitan Ltd.'s GS1 company prefix) and `site_infix="carrefour"` (i.e. the library fetches from `https://prices.carrefour.co.il/`). The `chain_id` is used to *match* file names at the portal — for the match to succeed, the portal's published file names contain `7290055700007`.
- Operator-fetched Carrefour PriceFull on 2026-04-26 confirmed root `<ChainId>=7290055700007` and store records whose `<StoreName>` identifies them as Carrefour branches.

**Why it matters for the complaint.** The CPFTA transparency regime presumes the consumer can identify which retailer's price they're looking at. Co-publication forces consumer-facing apps to invent ad-hoc heuristics (e.g. our app does a Google Places lookup) just to recover the brand identity. A clean fix is one of: a separate Carrefour `<ChainId>`, or a documented `<SubChainId>` / `<SubChainName>` requirement that distinguishes the brand. Neither is in place today.

---

## 2. Empty `<City>` field — Hazi Hinam StoresFull (1 of 13 stores)

**What's wrong (source side).** Hazi Hinam's published StoresFull contains at least one store record with `<City></City>` — the city element is empty. Without a city, an address like "המכבי 81" is ambiguous: the same street name exists in dozens of Israeli cities, and any consumer-facing geocoding will misplace the store.

**Direct-source evidence.** Operator-fetched Hazi Hinam StoresFull on 2026-04-26 from `https://shop.hazi-hinam.co.il/Prices`. `scripts/verify_source.py` reported `stores_total: 13`, `stores_empty_city: 1`. The verification's evidence Markdown (when run with `scripts/build_evidence.py`) prints the affected store's StoreId, StoreName, Address, and source line number.

**Why it matters for the complaint.** The regulation requires publication of store metadata; an empty `<City>` defeats the purpose of the field for the affected record. Even one such row demonstrates a compliance gap.

**Distinguishing source vs. ours.** Our pipeline writes empty cities to the DB only when the source field is empty / NaN / digit-only via `_clean_city`. The Hazi Hinam observation is direct in the source XML, so this is unambiguously source-side for that record.

**Scope note.** This entry is intentionally narrow to what's been directly verified. Our DB shows 214 stores with empty `city` post-pipeline; the broader pattern is a candidate (see research log) until source verification confirms it for additional chains.

---

## 3. ~~Empty `<Address>` field in store records~~ — Moved to research log 2026-04-26

The original §3 cited 157 stores with `address = ''` in our DB. That's a DB-derived count, not a direct-source observation. Moved to *DB signals worth investigating at source* below; can graduate back to Confirmed with a per-chain source verification.

---

## 4. ~~Multi-chain ghost stores at empty / placeholder addresses~~ — Moved to research log 2026-04-26

The original §4 cited 531 cross-chain `(address, city)` tuples in our DB and the 8-9-chains-at-empty-address pattern. Cross-chain DB groupbys are not direct-source evidence. Moved to *DB signals worth investigating at source* below.

---

## 5. ~~Test / sentinel values in `branch_name`~~ — Withdrawn 2026-04-26

**Status.** Retracted. Direct query `SELECT COUNT(*) FROM stores WHERE branch_name IN ('','0','TEST','בדיקה')` returned **0** on 2026-04-26. The original "331 rows" figure was a count of stores with empty `address` OR empty `city` — the explore agent that drafted this issue listed the sentinel `branch_name` values its query was *checking for* and treated them as values it had *found*. They are not present.

Kept as a visible retraction so anyone re-reading the doc sees the correction; do not cite the original claim in any complaint.

---

## 6. ~~Barcoded products with no product name in feed~~ — Withdrawn 2026-04-26

**Status.** Retracted as a source-side issue. Three direct-source verifications (Shufersal, Yeinot Bitan / Carrefour, Hazi Hinam) all showed 0 empty `<ItemName>` cells in their PriceFull files. Our DB still has 1,126 products with `name='nan'`, but those are produced by our pipeline — `il-supermarket-parser` (or our handling of its CSV output) drops names for some chains' XMLs even though the source has them. Tracked separately as a pipeline bug.

---

## 7. ~~High Places "not-at-address" rate — smaller chains~~ — Moved to research log 2026-04-26

The original §7 cited a per-chain skew in `verified_by_places = 'not_at_address'` rates between Shufersal (93) and the smaller chains (~380 each). That's a Google-Places-vs-our-DB observation, not direct-source evidence, and was already labeled "suspected." Moved to *DB signals worth investigating at source* below.

---

## 8. Victory's published StoresFull is malformed XML

**What's wrong (source side).** Victory (chain_id `7290696200003`, served from `laibcatalog.co.il`) publishes a StoresFull file that is **not valid XML**. The file ends abruptly at line 646, immediately after a `</Store>` close tag, with no closing tags for any of the parent containers (`</Stores>`, `</SubChain>`, `</SubChains>`, root). Standard XML parsers reject the file outright.

**Direct-source evidence.**
- Operator-fetched on 2026-04-26 from `https://laibcatalog.co.il/`.
- `scripts/verify_source.py` (and `scripts/build_evidence.py`) both encounter `lxml.etree.XMLSyntaxError` / `xml.etree.ElementTree.ParseError` at the truncation point. The evidence bundle quotes the last 600 bytes of the raw file, showing the missing close tags directly.
- Re-fetching produced the same byte sequence; this is publication state, not transfer corruption.

**Why it matters for the complaint.** The CPFTA regulation requires retailers to *publish* machine-readable price and store data. A file that cannot be parsed by any standard XML implementation is, in any practical sense, not published. Every consumer of the feed — price-comparison sites, regulators auditing compliance, our app — has to either custom-write a recovery parser or treat Victory's data as missing. This is a direct violation of the publication requirement, more severe than empty fields.

**Distinguishing source vs. ours.** No ambiguity. Our pipeline doesn't construct, transform, or rewrite the StoresFull XML; we download the chain-published `.gz` and gunzip it. The malformedness is byte-for-byte what Victory ships.

**Action items.** None on our side — we should *not* "fix" this by writing a recovery parser. Treating malformed publication as if it were valid would obscure a finding worth raising to the regulator.

---

## 9. Mahsani Hashuk: empty placeholder store + non-standard PriceFull schema

Two issues at the same chain, both directly observed in the same operator-fetched verification on 2026-04-26.

**§9A — Placeholder store record (all metadata fields empty).** Mahsani Hashuk's StoresFull (chain_id `7290661400001`, served from `laibcatalog.co.il`) contains 1 store record with **every consumer-facing metadata field empty**: `<StoreId>`, `<StoreName>`, `<Address>`, `<City>` all blank. There is no way to identify or locate this store; the record provides nothing.

**§9B — Non-standard PriceFull schema.** Mahsani Hashuk's PriceFull does not follow the same schema other chains use:
- Root element is `<Prices>`, not `<Root>` / `<Envelope>` / similar.
- Items are wrapped in `<Products>` containing `<Product>` elements, not the much more common `<Items>` containing `<Item>` elements.
- Element names use mixed casing including `<ChainID>` (uppercase `D`).

**Direct-source evidence.**
- Operator-fetched StoresFull and PriceFull on 2026-04-26 from `laibcatalog.co.il`.
- `scripts/verify_source.py` reported `stores_total: 1` of all-empty fields for §9A. The bundled evidence quotes the verbatim `<Store/>` placeholder block.
- Same script reported `items_inspected: 0` for the PriceFull because it iterates `<Item>` only — that 0 is itself the schema-deviation finding for §9B; the file is full of products under the alternate `<Product>` tag (heuristic regex confirms presence).

**Why it matters for the complaint.** Two distinct violations:
1. Publishing rows with all metadata fields blank defeats the regulation's purpose of letting consumers identify and locate stores.
2. Schema deviation forces every consumer of Mahsani's feed to either bespoke-code for their format or fail. The CPFTA regime presupposes that the feeds are machine-readable in a uniform way; the regulation should close this gap.

**Distinguishing source vs. ours.** We download the chain-published file as-is. Both defects are literally in the bytes Mahsani ships.

**Action items.** None on our side. The schema deviation should *not* be papered over by extending our verifier to absorb it — that would normalize the violation and erase the finding. Mahsani's PriceFull `<ItemName>` empty rate is therefore unmeasured by our standard tooling; if we want that number for the complaint, the cleanest path is a one-shot `grep -cE '<ItemName(\s/?>|>\s*</ItemName>)' file.xml` over the raw file, recorded as ad-hoc evidence rather than baked into the verifier.

---

## DB signals worth investigating at source — research log (NOT regulator-grade)

These are patterns observed in our own SQLite database. They are useful for finding *candidates* worth source-verifying, but the numbers themselves don't appear in the complaint until each candidate has been confirmed in the source XML for specific chains. Each entry below names the verification step that would graduate it back into the Confirmed list.

### Candidate A — Empty `<City>` across non-Hazi-Hinam chains

**DB signal.** `SELECT COUNT(*) FROM stores WHERE city = ''` → 214 on 2026-04-26. Direct-source confirmed for 1 row in Hazi Hinam (graduated to §2). The remaining 213 are from other chains and not directly verified. Some unknown fraction may be `_clean_city` converting digit-only source values (e.g. `<City>0</City>`) to empty — that's a pipeline artifact, not a source defect.

**To graduate.** Run `scripts/build_evidence.py` against StoresFull files from one or more additional chains. Each chain whose `<City></City>` rows are visible in the source bytes adds a per-chain entry to §2.

### Candidate B — Empty `<Address>` field

**DB signal.** `SELECT COUNT(*) FROM stores WHERE address = ''` → 157 on 2026-04-26. Co-occurs heavily with empty `city`. No direct-source observation yet.

**To graduate.** Same path as Candidate A — per-chain StoresFull verification. Once any chain ships visible `<Address></Address>` in the bytes, this opens as a §3 Confirmed entry with the verified chain's name attached.

### Candidate C — Multi-chain "ghost stores" at empty / placeholder addresses

**DB signal.** 531 distinct `(address, city)` tuples are claimed by stores from two or more chains in our DB. Most extreme cases have the same empty-address tuple appearing across 8–9 chains:

| Address | City | # chains in our DB |
|---------|------|--------------------|
| `''` | `''` | 9 |
| `''` | `באר שבע` | 9 |
| `''` | `נתיבות` | 9 |
| `''` | `אופקים` | 8 |
| `''` | `אפרת` | 8 |

That pattern is consistent with multiple chains independently shipping placeholder records. Whether each individual chain's StoresFull XML actually contains an empty-address row at that locale is **not directly verified**.

**At a real address.** `יהודה מכבי 81, ת"א` shows up under all eight non-Shufersal chains with `store_id = 305`. This may be a placeholder pattern or coincidence; either way, requires per-chain source verification to distinguish.

**To graduate.** Per-chain StoresFull verification, ideally against ≥4 chains. If multiple chains independently ship empty-address records that cluster around the same dummy locale, the cross-chain pattern becomes a real §4 Confirmed finding.

**Workaround on our side (not relevant to source claim).** `verify_stores.py` (clustering at lines 180–420) uses Google Places to detect and filter these. The Google Maps API bill is a sunk cost; mentioning here only because the workaround obscures the source-side count we'd need for direct evidence.

### Candidate D — Per-chain Places "not-at-address" rate skew

**DB signal.**

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

**Two competing explanations,** neither directly verifiable:
1. Smaller chains publish stores that don't actually exist or are mislocated, and Places correctly fails to find them.
2. Places has worse coverage of small Israeli supermarket chains than of Shufersal, and is generating false negatives.

This was previously labeled "suspected" — a classification it never earned with direct evidence. Stays in the research log as a soft signal; not complaint-grade.

**To graduate.** Manually walk a sample of 20 `not_at_address` rows per chain and verify on the ground (or against an independent registry). This is field work, not XML work.

---

## Verifications log

Direct-source verifications against chain portals, recorded as we run them. Run via `scripts/verify_source.py` (spot-check) or `scripts/build_evidence.py` (complaint bundle).

| Date | Chain | File | Result | Notes |
|------|-------|------|--------|-------|
| 2026-04-26 | Shufersal | live PriceFull (store 001) | **source-clean**: 4,434 items, 0 empty `<ItemName>` | First verification. Three of our `nan` product_ids found in this PriceFull, all with real Hebrew names — supports §6 retraction. |
| 2026-04-26 | Shufersal | controlled scrape (limit=2, temp DB) | **pipeline-clean for Shufersal-only**: 4,151 products in temp DB, 1 with `name='nan'` (0.024%) | Lone stray was barcode `7290119380053` (had unit `100 מ"ל` but no source name — single-row source gap, not a pattern). |
| 2026-04-26 | Yeinot Bitan / Carrefour | local PriceFull (operator-fetched) | **source-clean for `<ItemName>`**: 2,038 items, 0 empty | §1 (Carrefour-under-Yeinot-Bitan ChainId) directly corroborated: root `<ChainId>=7290055700007`, store names visibly Carrefour-branded. |
| 2026-04-26 | Hazi Hinam | local StoresFull + PriceFull (operator-fetched) | **§2 confirmed (1 of 13 empty `<City>`)**, otherwise source-clean: 7,971 items, 0 empty `<ItemName>`, 0 empty `<Address>` | Reinforces §6 retraction (clean PriceFull names) and feeds §2 (direct empty-City row). |
| 2026-04-26 | Mahsani Hashuk (`7290661400001`) | local StoresFull + PriceFull (operator-fetched, `laibcatalog.co.il`) | **§9A and §9B confirmed**. StoresFull: 1 placeholder row (all fields empty). PriceFull: non-standard schema — `<Prices>` root, `<Products>/<Product>`, `<ChainID>`. | `verify_source.py` reported `items_inspected: 0` due to schema deviation; that 0 is itself the §9B finding. |
| 2026-04-26 | Victory (`7290696200003`) | local StoresFull (operator-fetched, `laibcatalog.co.il`) | **§8 confirmed**: malformed XML, no closing tags, lxml strict-mode rejects. PriceFull untested (parse failure aborted run). | Top-tier severity. |

**Outstanding source verifications (next session).** Mahsani's PriceFull `<ItemName>` rate (would need ad-hoc grep over the raw file because of the schema deviation). Victory's PriceFull (its StoresFull parse failure aborted that run). At least one Cerberus-engine chain (Rami Levy / Yohananof / Osher Ad / Tiv Taam) for parity across engines — these need FTP-style auth so they're harder. Each successful run upgrades the relevant research-log candidates into Confirmed entries.

---

## How to verify against the raw source

Two tools, used together.

**`scripts/verify_source.py`** — quick spot-check. Takes a single StoresFull URL + PriceFull URL, downloads, gunzips, runs structural checks, writes `verification.md` + the raw bytes to an output directory. Strict on schema by design — its failure on Mahsani's `<Products>/<Product>` schema or Victory's malformed XML is *the finding*, not a bug.

```bash
python scripts/verify_source.py STORES_URL PRICE_URL --expected-chain-id ID --out /tmp/evidence
```

**`scripts/build_evidence.py`** — complaint-grade bundle builder. Takes one or many local `.gz` or `.xml` files plus optional fetch metadata, produces a self-contained directory with: byte-identical copies of every source file, gunzipped XMLs, SHA-256 hashes for both, per-file evidence Markdown that calls out each tracker issue (§1 / §2 / §3 / §8 / §9A / §9B) with line numbers and verbatim XML quotes, and a `manifest.json` tying it together.

```bash
python scripts/build_evidence.py file1.gz file2.gz ... \
    --out /tmp/cpfta-evidence --metadata fetch_meta.json
```

The metadata JSON maps each input absolute path to `{chain, url, fetched_at, http_headers, expected_chain_id, kind, wayback_url}` — the fetch context. The bundle is independently verifiable: the regulator can re-hash any file against `manifest.json`, re-fetch any source URL, and (if recorded) consult the corresponding Wayback Machine archive URL as third-party witness.

Most chain portals are geo-blocked from our VPS (Hetzner DE) — only Shufersal is reachable. Run from a local Israeli IP for everything else. URL discovery isn't automated because every portal has its own UI; paste the URL by hand.

### Authenticity chain — what the bundle proves

- **SHA-256 in `manifest.json`** — the file in the bundle is byte-for-byte the file we downloaded; no tampering after fetch.
- **`fetched_at` + `url` + `http_headers`** — when and from where we fetched, including the chain's own server identification (`Server`, `Last-Modified`).
- **Wayback Machine submission** — independent third-party archive at fetch time. Submit each source URL to `https://web.archive.org/save/<url>` and record the archive URL in the metadata JSON's `wayback_url` field. Strongest corroboration available without per-feed cryptographic signatures (which the chains do not provide).
- **What it does NOT prove** — that the chain published exactly those bytes, since chain feeds aren't signed. Closest available: regulator re-fetches during the complaint window and either matches the hash (corroborates) or finds different bytes (informative — chain has republished, possibly fixed).

Reports generated by either script are intended to be attached as evidence when this tracker is escalated to a CPFTA complaint.

---

## How to add new issues

**Strict source-verification requirement.** A new numbered entry (§N) only joins the Confirmed list when we have direct source-XML evidence: a downloaded `.gz` from the chain's own portal, with the defect visible in the raw bytes and quoted (with line numbers and SHA-256) in `build_evidence.py` output. DB queries can suggest candidates, but those go in the *DB signals worth investigating at source* research log, not the Confirmed list.

Per-issue structure for Confirmed entries:
- *What's wrong (source side)* — the defect described against the regulation.
- *Direct-source evidence* — file URL, fetch date, line numbers / lxml sourceline, SHA-256 if applicable.
- *Why it matters for the complaint* — the regulatory hook.
- *Distinguishing source vs. ours* — explicit statement that our pipeline doesn't introduce or amplify the defect.
- *Action items (if any)* — including the explicit non-action of "don't paper over" when the script's failure is itself the evidence.

Update the summary table; append a row to the Verifications log with the chain, file, fetch date, and result.
