# Fifth Grape — Backend

Scrapes public price and store data from Israeli supermarket chains, enriches it (emoji, brand, geocoding, Google Places verification, Open Food Facts), deduplicates equivalent SKUs, and exposes a read-only JSON API used by the mobile app.

---

## Data flow at a glance

```
  Chain XML feeds
        │
        ▼
   scrape.py ───► app/scraper/runner.py ───► SQLite (stores, products, prices)
                                              │
                                              ▼
                                  ┌────────────────────────┐
                                  │ Offline enrichment     │
                                  │ (run independently)    │
                                  ├────────────────────────┤
                                  │ enrich.py              │  emoji + brand + name cleanup
                                  │ enrich_off.py          │  Open Food Facts (barcode lookup)
                                  │ geocode.py             │  Google Geocoding (lat/lng)
                                  │ verify_stores.py       │  Google Places (ground-truth chain)
                                  │ scripts/backfill_      │  canonical product grouping
                                  │   canonical.py         │
                                  └────────────────────────┘
                                              │
                                              ▼
   serve.py ───► app/main.py (FastAPI) ───► mobile app
                  /v1/products/search
                  /v1/prices/lookup   (canonical-expanded if EXPAND_CANONICAL=true)
```

Everything except the API is offline/one-shot. The API only reads.

---

## Supported chains

See [`app/scraper/chains.py`](app/scraper/chains.py). Currently: Shufersal, Rami Levy, Victory, Yohananof, Osher Ad, Tiv Taam, Yeinot Bitan (also serves Carrefour stores), Hazi Hinam, Mahsanei HaShuk, Super-Pharm.

---

## Project layout

```
app/
  main.py             FastAPI app; /v1/products/search and /v1/prices/lookup
  db.py               SQLite schema + upsert/read helpers + canonical-group expansion
  models.py           Pydantic request/response models
  scraper/
    chains.py         Chain registry
    runner.py         scrape → parse → load pipeline
scrape.py             CLI: run the scraper
serve.py              CLI: run the API server (dev only)
geocode.py            CLI: backfill lat/lng via Google Geocoding
verify_stores.py      CLI: verify which chains really exist at each address via Google Places
enrich.py             CLI: assign emojis, extract brands from names, clean names
enrich_off.py         CLI: enrich products from Open Food Facts by barcode
scripts/
  backfill_canonical.py   One-shot equivalence-grouping backfill (HIGH-tier only)
deploy/
  vps-setup.sh            One-shot Hetzner/Ubuntu VPS bootstrap
  fifth-grape-api.service systemd unit for uvicorn
  fifth-grape-tunnel.service  systemd unit for the Cloudflare tunnel
data/
  fifth_grape.db          SQLite database (gitignored)
requirements.txt
.env                      (gitignored)  GOOGLE_MAPS_API_KEY, etc.
```

---

## Features

### 1. Scraping

`scrape.py` drives [`app/scraper/runner.py`](app/scraper/runner.py) to:

- Download `PriceFull` and `StoresFull` XML feeds for each chain via [`il-supermarket-scraper`](https://pypi.org/project/il-supermarket-scraper/).
- Parse them to CSVs with [`il-supermarket-parser`](https://pypi.org/project/il-supermarket-parser/).
- Upsert into `stores`, `products`, and `prices`.
- Record each run in `scrape_runs` for audit / debugging.

```bash
python scrape.py                              # all chains
python scrape.py --chains shufersal rami_levy # subset
python scrape.py --limit 5                    # cap files per chain per type
python scrape.py -v                           # debug logging
```

The store upsert keeps existing `lat`/`lng`/`geocode_status` when the address hasn't changed, so a re-scrape doesn't wipe geocoding results.

### 2. Product enrichment — emoji, brand, name cleanup

`enrich.py` applies rules over the `products` table:

- **Emoji**: ordered regex table (specific → generic) assigns a category emoji (`🥛`, `🍞`, `🧴`, …).
- **Brand extraction**: lifts the brand out of noisy name strings when the `brand` column is empty.
- **Name cleanup**: strips leading/trailing noise, whitespace normalisation.

```bash
python enrich.py              # full pass
python enrich.py --emoji      # just one step
python enrich.py --dry-run    # preview without writing
```

Idempotent; safe to re-run.

### 3. Product enrichment — Open Food Facts

`enrich_off.py` fetches brand, categories, and image URL per product from the [Open Food Facts API](https://world.openfoodfacts.org) by barcode. Resumable: each processed barcode is tracked so re-runs skip already-checked products.

```bash
python enrich_off.py              # process all unchecked products
python enrich_off.py --limit 500  # cap for testing
python enrich_off.py --stats      # coverage summary
```

### 4. Geocoding

`geocode.py` backfills `lat`/`lng` for stores with no coordinates, using the Google Geocoding API. Stores a `geocode_status` (`ok`, `no_results`, `api_error`) so failed rows can be retried.

```bash
python geocode.py                 # all unresolved rows
python geocode.py --retry-failed  # also retry rows previously marked no_results
python geocode.py --dry-run
```

Requires `GOOGLE_MAPS_API_KEY` in `.env`.

**Known limitation:** stores whose `city` column is empty get geocoded to whichever city's street-name match the geocoder prefers first — frequently Tel Aviv, which has a lot of the common street names. ~377 stores are currently affected. Slated for a follow-up fix that infers city from `branch_name` before the Geocoding call.

### 5. Store verification via Google Places

`verify_stores.py` sanity-checks the scraped stores against ground truth:

1. Finds clusters of stores sharing the same `(address, city)` — often a symptom of chains publishing under a shared feed (e.g. Carrefour stores appearing under the `yeinot_bitan` feed).
2. For each cluster, queries Places Text Search for supermarkets at that address.
3. Matches returned business names against our known chain list.
4. Writes `verified_by_places` per store: `verified` (this chain is at the address), `not_at_address` (Places found supermarkets there but not this chain), or `unknown` (inconclusive).

The API then filters out `not_at_address` rows in `get_stores_by_keys`, so Places verification silently improves result quality.

Also stores the raw Places business name in `places_name`. The API uses that to display Carrefour stores correctly even though they're scraped from `yeinot_bitan`'s feed (see `_display_chain_name` in `app/main.py`).

```bash
python verify_stores.py              # full run
python verify_stores.py --limit 20   # smoke test
python verify_stores.py --dry-run
```

Requires `GOOGLE_MAPS_API_KEY` in `.env`.

### 6. Product deduplication (canonical grouping)

**Problem.** The same consumer-facing product often exists as multiple `products` rows with different barcodes — different production batches, pack revisions, or retailer-assigned codes. Example: "חלב בקרטון 3% 1 ליטר" (Tara) has 6 distinct barcoded rows in the DB. A basket holding one variant would only see that variant's ~9 store prices, even though its siblings together have ~150 — crippling coverage for Tel Aviv users.

**Solution.** A `products.canonical_product_id` column points each row at the "winner" of its equivalence group. Canonical rows point at themselves.

**Grouping algorithm** (implemented in `scripts/backfill_canonical.py`):

1. **Fingerprint** each product: `(brand, base_tokens, spec_tokens, size, unit)` where
   - `base_tokens` = name tokens after stripping brand, stopwords, and size/unit literals.
   - `spec_tokens` = differentiating attributes: fat %, packaging (בקרטון, בשקית, פחית…), diet (אורגני, ללא לקטוז…), color, multipack.
   - `size` = numeric quantity in canonical units (L, kg, u) after unit scaling (ml→L ÷ 1000, g→kg ÷ 1000).
   - `unit` = canonical unit (`L`, `kg`, `u`, or the unit-column string).
2. Hebrew-aware normalisation: strips RTL marks, expands known abbreviations (`מהד`→`מהדורה`), strips single-letter prefixes (`בקרטון` ≡ `קרטון`), canonicalises Latin/Hebrew unit words.
3. **Orphan-numeric as variant code**: any leftover pure number (not matched as size or %) joins `spec_tokens` as `#N`. This keeps `שפתון 20` ≠ `שפתון 60` (lipstick shades) while allowing `חלב 1L` ≡ `חלב 1L`.
4. **Confidence tiers**:
   - **HIGH** — size resolved + no variant codes. Safest merges.
   - **MEDIUM** — either size unresolved OR one variant code.
   - **LOW** — multiple variant codes or other ambiguity.
5. **Winner** = group member with the most `prices` rows (tie-break: freshest `updated_at`, then lowest id).

Garbage rows (name = `'nan'` or empty) are never merged — they stay as singletons.

**Backfill.** The script runs in dry-run mode by default. The committed backfill only applied **HIGH-tier** merges:

```bash
python scripts/backfill_canonical.py               # dry run
python scripts/backfill_canonical.py --commit      # apply HIGH-tier writes
python scripts/backfill_canonical.py --show-samples 20  # more samples
```

`--commit` makes a full DB backup (`data/fifth_grape.db.bak.<timestamp>`) before writing, and applies all writes in a single transaction (rolls back on any error). It also skips products whose `canonical_product_id` is already set, so re-runs are idempotent.

MEDIUM and LOW tiers are **not** written by default — those need either tighter rules or a human-review queue.

**API-side expansion.** At request time, the API expands each requested `productId` to its canonical group and fetches prices for all members, then relabels the returned prices back to the requested id (so the frontend's basket-keyed lookup still works) and de-duplicates by `(store, chain, requested_id)`, preferring in-stock and lowest price.

Gated behind the `EXPAND_CANONICAL` env var (default `true`). Flip to `false` in `.env` and restart the service to disable the expansion instantly without rolling back DB state.

---

## API

### `GET /v1/products/search`

Free-text product search against `name`, `brand`, or exact `barcode`.

```
GET /v1/products/search?q=חלב&limit=5
→ 200 [ { productId, name, brand, unit, barcode, emoji, category }, ... ]
```

### `POST /v1/prices/lookup`

Given a list of product ids, returns stores, product metadata, and prices.

```
POST /v1/prices/lookup
{ "productIds": ["7290102396948"] }

→ 200 {
    "stores":   [ { storeId, chainId, chainName, branchName, address, city, lat, lng, geocodeStatus } ],
    "products": [ { productId, name, brand, unit, barcode, emoji, category } ],
    "prices":   [ { storeId, productId, price, inStock, updatedAt } ],
    "generatedAt": "<ISO-8601 timestamp of the last successful scrape>"
  }
```

When `EXPAND_CANONICAL=true` (default):

- Each requested `productId` is expanded to its full canonical group.
- All prices across the group are returned, relabeled to the requested `productId` so the frontend lookup key (`storeId + productId`) stays valid.
- Duplicates at the same `(store, chain)` collapse to the best row — prefer `in_stock=true`, then lower price.

Stores with `verified_by_places = 'not_at_address'` are always filtered out.

See [`app/models.py`](app/models.py) for exact types.

---

## Data model

Tables (see [`app/db.py`](app/db.py)):

| Table | Key | Notable columns |
|---|---|---|
| `stores` | `(store_id, chain_id)` | `lat`, `lng`, `geocode_status`, `verified_by_places`, `places_name` |
| `products` | `product_id` | `name`, `raw_name` (pre-enrichment), `brand`, `unit`, `barcode`, `emoji`, `category`, `canonical_product_id` |
| `prices` | `(store_id, chain_id, product_id)` | `price`, `in_stock`, `updated_at` |
| `scrape_runs` | `id` | Audit of each scrape run (`status`, `error`, timestamps) |

Migrations live in `init_db()` and run on every API startup; they check `PRAGMA table_info` and `ALTER TABLE` only when a column is missing, so they're idempotent and safe on re-runs.

---

## Environment variables

| Name | Default | Purpose |
|---|---|---|
| `FIFTH_GRAPE_DB` | `data/fifth_grape.db` | SQLite file path |
| `GOOGLE_MAPS_API_KEY` | *(required for geocode.py, verify_stores.py)* | Google Maps Platform key |
| `EXPAND_CANONICAL` | `true` | Expand `/v1/prices/lookup` by canonical group. Flip to `false` for instant rollback. |

---

## Setup (local)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # add your Google Maps key if you'll run geocode/verify_stores
```

Python 3.10+ is required.

Run the scraper, then start the server:

```bash
python scrape.py
python serve.py
# → http://localhost:8000
```

---

## Deployment

Production runs on a Hetzner Ubuntu VPS with two systemd units:

- `fifth-grape-api` — uvicorn serving FastAPI on `127.0.0.1:8000`.
- `fifth-grape-tunnel` — Cloudflare tunnel exposing the service as `https://fifth-api.grapesfarm.com`.

`deploy/vps-setup.sh` is a one-shot bootstrap: creates the `fifth-grape` system user, installs packages + Python venv, copies the systemd units, and starts both services. See the script for the full rsync-based deploy instructions.

**Restarting after a code change:**

```bash
sudo systemctl restart fifth-grape-api
journalctl -u fifth-grape-api -n 30 --no-pager
```

Schema migrations run automatically on start via `init_db()`.
