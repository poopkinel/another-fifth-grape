# Data-source issues — CPFTA-mandated chain feeds

Tracker for data-quality problems originating in the supermarket chains' published price/store XML feeds (the feeds mandated by `cpfta_prices_regulations`, pulled via `il-supermarket-scraper` in `app/scraper/runner.py`).

**Status (2026-04-27).** Complaint not pursued — see changelog. Doc retained as a research log of observed feed quirks. No entry currently meets the bar for a CPFTA complaint.

**Original purpose (preserved for context).** Evidence base for a possible complaint to the Consumer Protection and Fair Trade Authority (הרשות להגנת הצרכן ולסחר הוגן).

**Strict scope: direct-source evidence only.** A claim only graduates into a numbered Confirmed issue (§1, §2, …) when we have eyes on the source XML bytes and the defect is visible in those bytes. Numbers from our own SQLite DB are not regulator-grade — they could be partially or wholly caused by our pipeline (parser, scraper, schema cleaning) — and live in a separate "DB signals worth investigating at source" research log below. The complaint cites only sections marked Confirmed.

**Changelog.**
- 2026-04-26 — Initial tracker.
- 2026-04-26 — §5 (sentinel `branch_name`) and §6 (nameless products) retracted on re-verification: §5 had no actual sentinel rows; §6 was our Pandas parser, not source.
- 2026-04-26 — §1 strengthened with library-source evidence.
- 2026-04-26 — §8 (Victory malformed XML) and §9 (Mahsani placeholder + schema deviation) added from operator-fetched verifications.
- 2026-04-26 — **Restructure to direct-source-only.** §3 (empty `<Address>`, was 157 in our DB), §4 (multi-chain ghost stores, was 531 tuples in our DB) and §7 (Places mismatch rate) moved out of Confirmed because they were derived from DB queries, not direct-source observation. §2 (empty `<City>`) narrowed from "214 in our DB" to "Hazi Hinam StoresFull, 1 of 13 stores directly observed". The DB-derived patterns continue as candidates in the research log; each can graduate back to Confirmed with a per-chain source verification.
- 2026-04-26 — §10 added (YB / Carrefour Stores file format and naming violations: filename, no gzip, UTF-16 LE without XML declaration, `<ChainID>` casing). §1 strengthened with publisher self-attestation: the same file declares `<ChainName>קרפור</ChainName>` while `<ChainID>` is YB's GS1 prefix.
- 2026-04-27 — **§1 retracted.** Inspection of the YB Stores file shows ≥5 distinct consumer-facing brands (Carrefour, Yeinot Bitan, Be'er, Shuk Mehadrin, Quik) — all owned and operated by Yeinot Bitan Ltd. — sharing the company's single GS1 prefix `7290055700007` as `<ChainId>`. Multiple brands operated by one legal entity sharing that entity's `<ChainId>` is normal corporate-parent behavior, not a regulatory violation. Earlier §1-strengthening evidence (publisher self-attestation `<ChainName>קרפור</ChainName>`, library-source corroboration) is retracted with the issue.
- 2026-04-27 — **§2 retracted.** The single empty-`<City>` row in Hazi Hinam's StoresFull is the chain's online delivery service ("חצי חינם משלוחים"), which has no physical location; an empty `<City>` for a non-physical entity is reasonable, not a regulatory defect. With this row removed, no direct-source observation of a regulatorily-improper empty `<City>` remains; §2 has no Confirmed cases.
- 2026-04-27 — **Complaint dropped in full. §8, §9A, §9B, §10A–D all retracted.** Reasoning per item:
  - §8 (Victory malformed XML) — checked across other publication dates; the truncation was not repeating. Looks like a one-day publication glitch, not a persistent defect, so it doesn't sustain a complaint.
  - §9B (Mahsani schema deviation, `<Products>/<Product>` vs `<Items>/<Item>`) — the CPFTA regulation does not mandate any specific schema field or element name; the deviations from de-facto convention (different root, different item wrappers, casing) make consumer code harder but are not regulatory violations.
  - §9A (Mahsani all-empty placeholder store record) — by extension: one record with empty consumer-identifying fields is at most a data-quality slip, not a violation of a specific publication mandate.
  - §10A (filename `Stores` not `StoresFull`), §10B (uncompressed `.xml` not `.xml.gz`), §10D (`<ChainID>` casing) — same reasoning as §9B; convention deviations, not regulatory violations.
  - §10C (UTF-16 LE without XML declaration) — XML 1.0 §4.3.3 explicitly permits UTF-16 with BOM and no declaration; technically standards-compliant.

  Net result: no entry in this tracker meets the bar for a CPFTA complaint. Doc retained as a research log of observed feed quirks for any future re-evaluation; not currently the basis for any regulatory action.

**Verification methods available without re-scraping.**

1. **Direct chain portal fetch (clean snapshot)**: chain feeds are hosted on per-chain portals, *not* on gov.il. The `il-supermarket-scraper` package at `.venv/lib/python3.12/site-packages/il_supermarket_scarper/` is effectively the public registry — each `scrappers/<name>.py` hardcodes the chain's `chain_id` and portal infix. Most chain portals are geo-blocked from our VPS (Hetzner DE); operator runs them locally.
2. **DB queries** against `data/fifth_grape.db`: useful for *finding candidates* worth source-verifying. Not direct evidence by themselves.
3. **gov.il regulation page**: the regulation, not a data registry. Returns 403 to non-browser fetchers.

---

## Summary table

| # | Issue | Direct-source confidence | Per-chain evidence |
|---|-------|--------------------------|--------------------|
| 1 | ~~Yeinot Bitan publishes Carrefour-branded stores under its own ChainId~~ | **Withdrawn** 2026-04-27 | — |
| 2 | ~~Empty `<City>` field shipped in StoresFull~~ | **Withdrawn** 2026-04-27 | — |
| 3 | (former DB-only finding — moved to research log) | — | — |
| 4 | (former DB-only finding — moved to research log) | — | — |
| 5 | ~~Sentinel branch_name values~~ | **Withdrawn** 2026-04-26 | — |
| 6 | ~~Nameless barcoded products~~ | **Withdrawn** 2026-04-26 | — |
| 7 | (former suspected pattern — moved to research log) | — | — |
| 8 | ~~Victory's StoresFull is malformed XML (truncated, no closing tags)~~ | **Withdrawn** 2026-04-27 | — |
| 9 | ~~Mahsani Hashuk: placeholder store record + non-standard PriceFull schema~~ | **Withdrawn** 2026-04-27 | — |
| 10 | ~~Yeinot Bitan / Carrefour Stores file format & naming violations~~ | **Withdrawn** 2026-04-27 | — |

All numbered entries are now either retracted (§1, §2, §5, §6, §8, §9, §10) or demoted to the research log (§3, §4, §7). Numbers are kept stable so prior commit / handoff references resolve to the right thing.

---

## 1. ~~Carrefour stores published under the Yeinot Bitan ChainId~~ — Withdrawn 2026-04-27

**Status.** Retracted. Direct inspection of the YB Stores file (operator-fetched 2026-04-26 from `https://prices.carrefour.co.il/20260426/Stores7290055700007-000-20260426-000100.xml`) shows 154 stores carrying ≥5 distinct consumer-facing brand names: **Carrefour** (≥143), **Be'er** (בעיר, 6), **Yeinot Bitan** (1 genuine + 1 placeholder URL-as-address record), **Shuk Mehadrin** (1), **Quik** (1, also placeholder URL-as-address). All five are brands owned and operated by Yeinot Bitan Ltd., the legal entity whose GS1 company prefix is `7290055700007`.

The CPFTA `<ChainId>` is the chain operator's identifier — it identifies the legal entity required to publish the data, not the consumer-facing brand. A holding company operating multiple consumer brands under its single ChainId is normal and consistent with how the regulation is structured. No regulation appears to require a per-brand split into separate `<ChainId>` namespaces.

The earlier framing — "Carrefour mislabeled under YB" — was the wrong abstraction. The right framing would have been about the absence of any machine-readable brand discriminator (per-store `<SubChainId>` is empty for all 154 stores in the file), but even that is a usability gripe rather than a clear regulatory violation absent a specific rule mandating brand-level disclosure. Pulled from the complaint.

Earlier "strengthening" evidence — library-source `chain_id` hardcoding, publisher self-attestation `<ChainName>קרפור</ChainName>` — described real facts about the file but does not establish a regulatory violation, so it is retracted with the issue.

Kept as a visible retraction so anyone re-reading the doc sees the correction; do not cite the original claim in any complaint.

---

## 2. ~~Empty `<City>` field — Hazi Hinam StoresFull~~ — Withdrawn 2026-04-27

**Status.** Retracted. The single empty-`<City>` row originally cited is `StoreName="חצי חינם משלוחים"` (Hazi Hinam Deliveries), the chain's online delivery service. Its `<Address>` is `https://shop.hazi-hinam.co.il/` — a website URL, not a physical address — confirming the record represents a non-physical entity. An empty `<City>` for a delivery service is a reasonable representation of the absence of a single physical location, not a defect in store-metadata publication.

This was the only direct-source empty-`<City>` row we had verified, so §2 has no remaining Confirmed cases. The broader DB-derived empty-City pattern continues as Candidate A in the research log, but per-chain re-verification needs to distinguish "missing data for a real store" from "intentionally non-physical entity (delivery service / web store / etc.)" before any row graduates back into a Confirmed entry.

Kept as a visible retraction so anyone re-reading the doc sees the correction; do not cite the original claim in any complaint.

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

## 8. ~~Victory's published StoresFull is malformed XML~~ — Withdrawn 2026-04-27

**Status.** Retracted. Operator checked Victory's StoresFull on later publication dates; the truncation observed on 2026-04-26 was not repeating. The malformedness appears to be a one-day publishing glitch rather than a persistent defect, so it does not sustain a complaint about Victory's compliance with the publication requirement.

Original technical observations preserved below for future reference; do not cite as evidence in any complaint.

---

**Original observation (for reference, not a complaint claim).** Victory (chain_id `7290696200003`, served from `laibcatalog.co.il`) published on 2026-04-26 a StoresFull file that ended abruptly at line 646 immediately after a `</Store>` close tag, with no closing tags for any of the parent containers (`</Stores>`, `</SubChain>`, `</SubChains>`, root). On that date, both `scripts/verify_source.py` and `scripts/build_evidence.py` failed to parse it under `lxml` strict mode; re-fetching that day produced the same byte sequence. Subsequent dates showed well-formed files.

---

## 9. ~~Mahsani Hashuk: empty placeholder store + non-standard PriceFull schema~~ — Withdrawn 2026-04-27

**Status.** Both sub-items retracted.

- **§9B (non-standard PriceFull schema)** — the CPFTA regulation does not mandate any specific schema field or element name. Deviations from the de-facto convention used by other chains (`<Prices>` root, `<Products>/<Product>` items, `<ChainID>` casing) make consumer code harder to write but are not regulatory violations.
- **§9A (placeholder all-empty store record)** — by extension: a single record with empty consumer-identifying fields is a data-quality slip rather than a violation of a specific publication mandate.

Original technical observations preserved below for future reference; do not cite as complaint evidence.

---

**Original observations (for reference, not complaint claims).** On 2026-04-26, Mahsani Hashuk's StoresFull (chain_id `7290661400001`, served from `laibcatalog.co.il`) contained 1 store record with `<StoreId>`, `<StoreName>`, `<Address>`, and `<City>` all blank. The PriceFull on the same date used a different schema from other chains: root element `<Prices>` (not `<Root>`/`<Envelope>`), items wrapped in `<Products>`/`<Product>` (not `<Items>`/`<Item>`), and `<ChainID>` casing (uppercase D). `scripts/verify_source.py` reported `stores_total: 1` (the all-empty record) and `items_inspected: 0` (the script iterates `<Item>` only, which doesn't match `<Product>`).

---

## 10. ~~Yeinot Bitan / Carrefour Stores file — format and naming violations~~ — Withdrawn 2026-04-27

**Status.** All four sub-items retracted. The CPFTA regulation does not mandate specific filename patterns, gzip compression, encoding choice, or element-name casing.

- **§10A** (filename `Stores` not `StoresFull`) — naming convention, not in the regulation.
- **§10B** (uncompressed `.xml` not `.xml.gz`) — compression convention, not in the regulation.
- **§10C** (UTF-16 LE without XML declaration) — XML 1.0 §4.3.3 explicitly permits UTF-16 with BOM and no declaration; standards-compliant.
- **§10D** (`<ChainID>` casing) — naming convention, same reasoning as §9B.

Original technical observations preserved below for future reference; do not cite as complaint evidence.

---

**Original observations (for reference, not complaint claims).** On 2026-04-26, Yeinot Bitan / Carrefour published its only Stores file at `https://prices.carrefour.co.il/20260426/Stores7290055700007-000-20260426-000100.xml` (HTTP 200, `Server: Apache/2.4.52 (Ubuntu)`, `Content-Type: application/xml`, `Last-Modified: Sat, 25 Apr 2026 21:01:01 GMT`, `Content-Length: 64290`; SHA-256 in the evidence bundle's `manifest.json`). It was the only Stores-type file in the portal listing of 3,286 files. It deviated from the format used by Hazi Hinam / Mahsani / Victory / Shufersal in four ways: filename used `Stores` rather than `StoresFull`; file was raw `.xml` rather than `.xml.gz` (~10× bigger; ~64 KB vs ~6 KB compressed); first 32 bytes were `ff fe 3c 00 52 00 6f 00 6f 00 74 00 3e 00 3c 00 43 00 68 00 61 00 69 00 6e 00 49 00 44 00 3e 00` — UTF-16 LE BOM followed by `<Root><ChainID>` in UTF-16 LE, with no `<?xml ... ?>` declaration; element names used `<ChainID>` (uppercase D) rather than `<ChainId>`. Decoded prefix: `﻿<Root><ChainID>7290055700007</ChainID><ChainName>קרפור</ChainName><LastUpdateDate>2026-04-26</LastUpdateDate>...`.

---

## DB signals worth investigating at source — research log (NOT regulator-grade)

These are patterns observed in our own SQLite database. They are useful for finding *candidates* worth source-verifying, but the numbers themselves don't appear in the complaint until each candidate has been confirmed in the source XML for specific chains. Each entry below names the verification step that would graduate it back into the Confirmed list.

### Candidate A — Empty `<City>` across non-Hazi-Hinam chains

**DB signal.** `SELECT COUNT(*) FROM stores WHERE city = ''` → 214 on 2026-04-26. The single Hazi Hinam direct-source row that was originally graduated to §2 turned out to be the chain's delivery service (non-physical entity, legitimate empty `<City>`); §2 retracted 2026-04-27. The remaining 213 rows are from other chains and not directly verified. Some unknown fraction may be `_clean_city` converting digit-only source values (e.g. `<City>0</City>`) to empty — that's a pipeline artifact, not a source defect.

**To graduate.** Run `scripts/build_evidence.py` against StoresFull files from one or more additional chains. Each empty-`<City>` row found needs to be checked for whether it represents a real-but-mislabeled physical store vs. an intentionally non-physical entity (delivery service, web store, etc.) before it can graduate. Multiple unambiguous mislabeled-physical-store rows would re-open §2 under a tighter framing.

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
| 2026-04-26 | Yeinot Bitan / Carrefour | local PriceFull (operator-fetched) | **source-clean for `<ItemName>`**: 2,038 items, 0 empty | Originally credited as §1 corroboration; §1 retracted 2026-04-27 — see changelog. |
| 2026-04-26 | Hazi Hinam | local StoresFull + PriceFull (operator-fetched) | **source-clean** (after §2 retraction): 7,971 items, 0 empty `<ItemName>`, 0 empty `<Address>`. The single empty-`<City>` row is the chain's delivery service, not a physical store. | Reinforces §6 retraction (clean PriceFull names). The empty-`<City>` finding (1/13) was originally credited to §2; §2 retracted 2026-04-27 — see changelog. |
| 2026-04-26 | Mahsani Hashuk (`7290661400001`) | local StoresFull + PriceFull (operator-fetched, `laibcatalog.co.il`) | **§9A and §9B confirmed**. StoresFull: 1 placeholder row (all fields empty). PriceFull: non-standard schema — `<Prices>` root, `<Products>/<Product>`, `<ChainID>`. | `verify_source.py` reported `items_inspected: 0` due to schema deviation; that 0 is itself the §9B finding. |
| 2026-04-26 | Victory (`7290696200003`) | local StoresFull (operator-fetched, `laibcatalog.co.il`) | **§8 confirmed**: malformed XML, no closing tags, lxml strict-mode rejects. PriceFull untested (parse failure aborted run). | Top-tier severity. |
| 2026-04-26 | Yeinot Bitan / Carrefour (`7290055700007`) | live Stores file (operator-fetched, `prices.carrefour.co.il`) | **§10A–D confirmed**: filename `Stores...xml` (no `Full`, uncompressed); UTF-16 LE BOM, no XML declaration; `<ChainID>` casing. | First Stores file inspected for YB; only Stores-type file in the entire portal listing of 3,286 entries. Brand-mix of stores (≥5 YB-owned brands all under one ChainId) noted but is not a regulatory finding — §1 retracted 2026-04-27. |

**Outstanding source verifications (next session).** Mahsani's PriceFull `<ItemName>` rate (would need ad-hoc grep over the raw file because of the schema deviation). Victory's PriceFull (its StoresFull parse failure aborted that run). At least one Cerberus-engine chain (Rami Levy / Yohananof / Osher Ad / Tiv Taam) for parity across engines — these need FTP-style auth so they're harder. Each successful run upgrades the relevant research-log candidates into Confirmed entries.

---

## How to verify against the raw source

Two tools, used together.

**`scripts/verify_source.py`** — quick spot-check. Takes a single StoresFull URL + PriceFull URL, downloads, gunzips, runs structural checks, writes `verification.md` + the raw bytes to an output directory. Strict on schema by design — its failure on Mahsani's `<Products>/<Product>` schema or Victory's malformed XML is *the finding*, not a bug.

```bash
python scripts/verify_source.py STORES_URL PRICE_URL --expected-chain-id ID --out /tmp/evidence
```

**`scripts/build_evidence.py`** — complaint-grade bundle builder. Takes one or many local `.gz` or `.xml` files plus optional fetch metadata, produces a self-contained directory with: byte-identical copies of every source file, gunzipped XMLs, SHA-256 hashes for both, per-file evidence Markdown that calls out each tracker issue (§3 / §8 / §9A / §9B / §10A–D) with line numbers and verbatim XML quotes, and a `manifest.json` tying it together. The script also still performs §1 and §2 checks for now, but those issues are retracted; treat any "**CONFIRMED**" output for §1 or §2 in the per-file evidence as legacy until the script is updated.

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
