# Session handoff — 2026-04-26

## What landed today

Three threads, framing a possible CPFTA complaint plus filling concrete gaps in the ingest pipeline. Everything is on disk; **nothing has been committed, pushed, or installed on the VPS** — the working tree is dirty, and `sudo systemctl daemon-reload` / `enable --now` have not been run.

### 1. CPFTA data-source assessment

Confirmed that what we currently scrape via `il-supermarket-scraper` *is* the gov.il-mandated chain transparency feed — the gov.il page is the regulation, not a separate source. Library is a thin per-chain wrapper (one ~16-line scraper class per chain at `.venv/lib/python3.12/site-packages/il_supermarket_scarper/scrappers/*.py`); it does not introduce data quality issues by itself. **No parallel pipeline.**

Key finding for the Carrefour-under-Yeinot-Bitan question: the library's `YaynotBitanAndCarrefour` class hardcodes `chain_id="7290055700007"` (Yeinot Bitan's GS1 company prefix) and fetches from `prices.carrefour.co.il`. The chain_id is a filter against XML content, so the source's own `<ChainId>` element really does carry Yeinot Bitan's ID for Carrefour-branded stores. That's an artifact of corporate consolidation (Yeinot Bitan owns Carrefour Israel) plus the regulation only requiring one ChainId per legal entity — making it the strongest source-side complaint angle: the regulation should require a per-brand identifier.

### 2. New tracker doc + memory pointer for the complaint

Created `backend/data_source_issues.md` — a complaint-grade running tracker of source-side issues, with a summary table, per-issue evidence (queries, file refs, commits, dates), and a strict separation between source-side issues and our-pipeline bugs. It distinguishes **Confirmed** vs **Suspected** confidence per issue.

Initial 7 entries; **two retracted same day on re-verification** —
- §5 (test sentinel `branch_name` values) — retracted: query `WHERE branch_name IN ('','0','TEST','בדיקה')` returns 0; the original 331-row figure was empty-`address` OR empty-`city` rows (i.e. issues #2/#3 already covered).
- §6 ('nan' product names) — retracted as source-side: the `'nan'` strings in `products.name` are produced by our own parser at `app/scraper/runner.py:222`, where `str(row.get("itemname", "")).strip()` doesn't go through the existing `_nullable()` helper that the brand/unit fields use. Fix is one line; not yet applied. Direct-source check on a live Shufersal PriceFull (43 items inspected) showed 0 empty `<ItemName>` cells, supporting the retraction.

Memory entry at `~/.claude/projects/-home-fifth-grape/memory/project_cpfta_complaint_tracker.md` points at the tracker and codifies the rule: append findings there with complaint-grade rigor; don't pad with our-side bugs.

### 3. Three concrete pipeline changes

**(a) Daily scheduled scrape — `deploy/fifth-grape-scrape.{service,timer}`.** Mirror of the existing prune-events timer. Runs `scrape.py` daily at 03:00 (with up to 30 min jitter) under the `fifth-grape` user. `Type=oneshot`, `TimeoutStartSec=4h`. `systemd-analyze verify` passed clean. **Not yet installed** — install command in updated `README.md`. Closes the staleness gap (current data is from 2026-04-10).

**(b) PromoFull ingestion — `promotions` + `promotion_items` tables.** Library and parser library both already supported `PROMO_FULL_FILE`; we just hadn't wired it. Changes:
- `app/db.py` — two new tables (promotions PK on `promo_id` = `chain_id|store_id|promotion_id`; promotion_items as a join table with FK + ON DELETE CASCADE). Promoted typed columns: `description`, `start_at`, `end_at` (ISO strings, combining date+hour from feed), `reward_type`, `discounted_price`, `min_qty`, `min_purchase_amt`, `update_date`. Fields not promoted to columns are JSON-stuffed into `raw_json` so nothing's lost. New helpers: `upsert_promotion()` and `replace_promotion_items()`.
- `app/scraper/runner.py` — added `PROMO_FULL_FILE` to `SCRAPE_FILE_TYPES`; new `_load_promotions(chain_id, parsed_dir)` that reads `promo_full_file_*.csv`, groups by `(storeid, promotionid)`, upserts the promo and replaces its items in one go. Helper `_combine_date_hour()` produces ISO strings. Synthetic-fixture test passed (two promos, three items, gift flag preserved, raw_json populated correctly). **Schema is idempotent — applies on next `init_db()` run.**

**(c) Direct-source verification script — `scripts/verify_source.py`.** Bypasses both our DB and the library's parser layer. Takes direct StoresFull and PriceFull URLs (or local paths), downloads, gunzips, runs XML checks aligned to each tracker issue, writes `verification.md` plus the raw XMLs to an out dir. Stdlib only. Smoke-tested against a live Shufersal PriceFull URL — fetch + gzip + parse all worked.

URL discovery deliberately *not* in the script: every chain portal has its own UI (Shufersal uses category dropdowns, publishprice portals embed file lists in `<script>` blocks, Cerberus-engine portals like Rami Levy/Yohananof use FTP with auth). Operator pastes a URL; script does the parsing. Run it locally for the Carrefour-under-Yeinot-Bitan verification — `prices.carrefour.co.il` is geo-blocked from our VPS (DE). Verified: VPS is on Hetzner Falkenstein, only `prices.shufersal.co.il` was reachable from the chain portals tried.

---

## Files touched (uncommitted)

```
backend/data_source_issues.md           NEW — complaint-grade tracker
backend/deploy/fifth-grape-scrape.service  NEW — daily scrape unit
backend/deploy/fifth-grape-scrape.timer    NEW — 03:00 jitter timer
backend/scripts/verify_source.py        NEW — direct-source verification CLI
backend/app/db.py                       MODIFIED — promotions + promotion_items tables, upserts
backend/app/scraper/runner.py           MODIFIED — _load_promotions, PROMO_FULL_FILE in SCRAPE_FILE_TYPES
backend/README.md                       MODIFIED — deploy/ listing, scheduled-scrape section
frontend/BACKLOG.md                     MODIFIED — map-pin overlap ticket; CPFTA bullet rewritten with tracker pointer
```

The frontend also got a separate set of edits earlier in the day (i18n hydration gate, settings.tsx walk-chip key fix) — those are documented in commit-log style if pushed; not part of this CPFTA thread.

---

## Outstanding for next session

### Prio 1 — Complaint follow-through

- **Run `verify_source.py` against `prices.carrefour.co.il`** from a machine with an Israeli IP. Expected: root `<ChainId>` = `7290055700007` (Yeinot Bitan), `<SubChainName>` includes `קרפור` — that's the smoking-gun evidence for tracker §1 of the CPFTA complaint.
- For each remaining "Confirmed" tracker issue (§2, §3, §4), run `verify_source.py` against at least one chain to attach a "verified at source on YYYY-MM-DD" stamp to the tracker.

### Prio 2 — Our-side cleanup before complaint goes out

- **Fix the `nan` parser bug.** `app/scraper/runner.py:222` — wrap `itemname` and `raw_name` reads in `_nullable()`, mirror what brand/unit do. Then a one-shot UPDATE to set `name = ?` for the 1,126 existing `nan` rows (re-pull from `raw_name` if non-nan, else delete the row + cascade prices). Knocks out a false complaint vector and cleans 81,887 stale price rows.

### Prio 3 — Coverage expansion (task #4, deferred)

- `app/scraper/chains.py` registers 9 chains; `il_supermarket_scarper.ScraperFactory` exposes 35+. Audit which to enable — criteria: actually open to consumers, not flagged unstable in `ScraperStability`, supplies feeds in our region. Candidates: Cofix, Dor Alon, Good Pharm, Fresh Market & Super Dosh, Yellow, Stop Market, Keshet, Zol VeBegadol, City Market variants. Skip: Quik (flagged flaky in library), Wolt, Bareket (chicken-only), most others not selling general groceries.

### Prio 4 — VPS install

- `sudo cp deploy/fifth-grape-scrape.{service,timer} /etc/systemd/system/`
- `sudo systemctl daemon-reload && sudo systemctl enable --now fifth-grape-scrape.timer`
- (Same pattern for `fifth-grape-prune-events.timer` if not already enabled — README listed it for the first time today.)

---

## Reusable facts learned

- **VPS is on Hetzner Falkenstein, Germany.** `prices.shufersal.co.il` is reachable; `prices.carrefour.co.il`, `url.publishedprice.co.il`, `prices.yohananof.co.il`, `matrixcatalog.co.il` are not (timeout / DNS). Library README warns about this.
- **ChainId is a 13-digit GS1 company prefix.** Shufersal=`7290027600007`, Yeinot Bitan=`7290055700007`. Mapped legally to a company, not a brand. SubChainId/SubChainName carry the brand distinction *if* the chain populates them.
- **CSV filename convention from `il_supermarket_parsers`:** `{file_type_lower}_{store_name_lower}.csv` (see `raw_parsing_pipeline.py:48`). So PROMO_FULL_FILE → `promo_full_file_*.csv`. Our `_load_*` functions match by prefix.

---
---



### Frontend (already pushed to `poopkinel/fifth-grape:main`, commit `fd9d496`)

**Progressive radius expansion in the compare screen.** Before: hardcoded 5 km filter dropped all stores when the user had no in-stock matches nearby, leaving the list empty. Now: `rankStores` tries `[5, 10, 25, 50]` km in order and stops at the first tier that yields any matched store. The actual radius used is surfaced back to the UI subtitle so the user sees `רדיוס 25 ק״מ` (or whatever the resolved tier was) instead of a misleading hardcoded "5 ק"מ".

Changes: `app/list/compare.tsx`, `src/domain/recommendation/rankStores.ts`, `src/domain/recommendation/types.ts`, `src/features/compare/selectors.ts`, `src/features/compare/types.ts`. Hot-reloads via Metro — no build needed.

### Backend (already pushed to `poopkinel/another-fifth-grape:main`, four commits)

```
555ef12  Add test suite for canonical grouping (fingerprint + API expansion)
455cd1e  Expand /v1/prices/lookup via canonical groups + comprehensive README
bf7169c  Add canonical_product_id schema + HIGH-tier equivalence-grouping backfill
5db7cc6  Consolidate production state: enrichment, geocoding, Places verification, VPS deploy
```

Headline: **product deduplication** via a `canonical_product_id` column on `products`.

The original symptom: a basket holding `7290102396948` (Tara 3% 1L milk) returned 9 prices from a single Be'er Sheva store. The DB actually had 6 functionally-equivalent SKUs (different barcodes, same product) — the others had 58, 83, 982, 2459 price rows respectively. Coverage was crippled by SKU fragmentation.

Solution had three landings:

1. **Schema**: nullable `canonical_product_id` column + index on `products`. Idempotent migration in `init_db()`. Fully reversible.
2. **Backfill**: `scripts/backfill_canonical.py` runs a fingerprint algorithm `(brand, base_tokens, spec_tokens, size, unit)` over every product row, assigns confidence tiers (HIGH / MEDIUM / LOW), and only writes the **HIGH tier** (size resolved + no orphan variant codes). 1,728 rows updated across 727 groups. DB backup at `data/fifth_grape.db.bak.20260420-130524` before writes.
3. **API**: `POST /v1/prices/lookup` now expands each requested productId to its canonical group, fetches prices for all members, deduplicates by `(store, chain, requested_id)` preferring in-stock-then-cheapest, and **relabels prices back to the requested id** so the frontend's basket-keyed lookup keeps working with no client changes. Gated by `EXPAND_CANONICAL` env var (default `true`).

Result for the milk case: 1 store/9 prices → 11 stores/123 prices, all within 5 km of Tel Aviv.

Also lots of incidental cleanup (commit `5db7cc6`): the prod working tree had drifted ~6 months from `origin/main` — Places verification, geocoding, OFF enrichment, deploy/, etc. were never committed. All consolidated.

Test suite: 26 green tests over fingerprint, `get_canonical_groups`, and the `/v1/prices/lookup` API behavior. Run with `.venv/bin/pytest`.

---

## Current state

- **Service**: `fifth-grape-api` running, PID restarted at 2026-04-20 13:18:55 UTC, code matches `455cd1e`.
- **Frontend**: poopkinel/fifth-grape:main = `fd9d496`. Metro hot-reload picks up changes.
- **Backend**: poopkinel/another-fifth-grape:main = `555ef12`. Working tree clean.
- **DB backup**: `data/fifth_grape.db.bak.20260420-130524` (2.5 GB) — keep at least until the dedup work has soaked in prod a few days.
- **Feature flags**: `EXPAND_CANONICAL=true` (default). To disable: add `EXPAND_CANONICAL=false` to `/home/fifth-grape/backend/.env` and `sudo systemctl restart fifth-grape-api`.

---

## Outstanding items (in rough priority order)

### 1. Empty-city geocoding bug — surfaced today, not fixed

**Scope:** 739 stores have `city = ''` in the DB; ~377 of those pass the Places filter and may show up at wrong coordinates. Root cause: when `city` is empty, the geocoder picks whichever same-named street it finds first — frequently Tel Aviv (which has a lot of common street names like Rothschild, Begin, etc.).

**User-visible examples found today:**
- `yeinot_bitan_2750` / `hazi_hinam_2750` — branch labelled "קרפור עמישב פ"ת" (Petah Tikva), `city = ''`, geocoded to Tel Aviv's Begin 96 (`32.07, 34.79`), shown at 2.4 km from a Tel Aviv user instead of ~15 km.
- `hazi_hinam_719` — branch "רוטשילד כפר סבא", `city = ''`, geocoded to Rothschild Tel Aviv.

**Proposed fix:** infer city from `branch_name` (Hebrew abbreviations: `פ"ת` → פתח תקוה, `כ"ס` → כפר סבא, `רא"ל` → ראשון לציון, `ב"ש` → באר שבע, plus full city names embedded as suffixes), backfill the `city` column, then re-geocode the 377 affected rows with city as a region bias to disambiguate same-named streets. Stub of an audit query is in the README under "Geocoding → Known limitation".

### 2. MEDIUM-tier dedup rollout

The HIGH tier is in. MEDIUM (size resolved + 1 variant code, OR size unresolved + no variant codes) holds 2,935 groups / 4,681 rows that would re-point — significant additional coverage. Needs either:
- Tighter rules in `backfill_canonical.py` to promote some MEDIUM cases to HIGH (e.g. better size extraction from unit columns), or
- A small human-review queue (export top N candidates, eyeball, keep/skip).

LOW tier (207 groups) is mostly noise; probably not worth pursuing.

### 3. Garbage `nan`-named products

1,126 products have `name = 'nan'` (scraper failures). All of them have prices in the `prices` table. Two options to discuss:
- Soft-hide from search (mark as inactive) so they don't pollute results, but their prices remain queryable by barcode.
- Try to backfill names by re-querying Open Food Facts via `enrich_off.py` (those rows would be processed since they have barcodes).

### 4. Alternative-product feature (deferred from today)

The dedup work made today's milk case findable at all. The natural follow-up: when a basket item has zero nearby in-stock stores, surface 1–3 same-category products that DO. The `category` column (e.g. `"milk 3%"`, `"uht-milks"`) is a decent grouping primitive but rough — the same `milks` bucket mixes 1L and 2L, 1% and 3%. We agreed to scope by category + same `unit` + similar size to avoid noisy swaps. This is a UX feature, not a data fix.

### 5. Map clustering glitches

User flagged briefly during testing today; details deferred.

### 6. FastAPI deprecation cleanup (low priority)

Tests emit `on_event("startup") is deprecated` warnings. FastAPI wants the lifespan-event handler pattern instead. One-line refactor in `app/main.py`.

---

## How to verify nothing's broken next session

```bash
# 1. Service is up and serving expanded responses
curl -s http://127.0.0.1:8000/v1/prices/lookup \
  -H "Content-Type: application/json" \
  -d '{"productIds":["7290102396948"]}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'stores={len(d[\"stores\"])} prices={len(d[\"prices\"])}')"
# Expected: stores=11 prices=123

# 2. Tests still pass
cd /home/fifth-grape/backend && .venv/bin/pytest

# 3. Git is aligned
cd /home/fifth-grape/backend && git status   # clean, on main, up to date with origin/main
cd /home/fifth-grape/frontend && git status  # frontend has untracked files (.claude/, SESSION_HANDOFF.md, src/theme/, modified package.json/package-lock.json/StoreMapScene.tsx) — pre-existing, not from today
```

---

## Working with this codebase — quick context

- **Repos:** backend in `/home/fifth-grape/backend` (this VPS, runs the live service). Frontend in `/home/fifth-grape/frontend` (Expo / React Native). Frontend git remote is `poopkinel/fifth-grape`; backend is `poopkinel/another-fifth-grape`. Different repos, easy to confuse.
- **Backend git history:** until today, the live working tree had drifted from `origin/main` by months of uncommitted work. Don't be surprised if pre-today commits feel terse — most prior development never made it into git. The README (now committed in `455cd1e`) is the up-to-date map of features.
- **Service control:** restart needs sudo. From this shell user (`fifth-grape`), it'll fail with "Interactive authentication required" — ask the user to run `sudo systemctl restart fifth-grape-api` themselves (the `! sudo …` prompt prefix works).
- **DB backups:** the backfill script writes `data/fifth_grape.db.bak.<timestamp>` before destructive operations. They're gitignored (`*.db.bak.*`) and 2.5 GB each. Clean up old ones occasionally.
- **The `.claude/` and `data/` and `.env` and `*.log` and `*cookies*` paths are all gitignored** as of `5db7cc6` — safe to leave untracked junk in the working tree without polluting commits.
- **User preference:** confirmed today — never delete or do destructive ops without explicit confirmation, and keep them updated mid-task. They like dry-run + commit-flag patterns for write operations.
