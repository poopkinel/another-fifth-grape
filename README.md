# Fifth Grape

Scrapes public price and store data from Israeli supermarket chains, normalizes it into SQLite, and exposes a small read-only JSON API.

## What it does

- Downloads the government-mandated `PriceFull` and `StoresFull` XML feeds from ~10 major Israeli chains via [`il-supermarket-scraper`](https://pypi.org/project/il-supermarket-scraper/).
- Parses them into CSVs via [`il-supermarket-parser`](https://pypi.org/project/il-supermarket-parser/).
- Upserts stores, products, and prices into a local SQLite database.
- Serves a single snapshot endpoint over FastAPI.

Supported chains (see [app/scraper/chains.py](app/scraper/chains.py)): Shufersal, Rami Levy, Victory, Yohananof, Osher Ad, Tiv Taam, Yeinot Bitan, Hazi Hinam, Mahsanei HaShuk, Super-Pharm.

## Project layout

```
app/
  main.py          FastAPI app and /v1/market/snapshot endpoint
  db.py            SQLite schema + upsert/read helpers
  models.py        Pydantic response models
  scraper/
    chains.py      Chain registry
    runner.py      scrape → parse → load pipeline
scrape.py          CLI entry point for the scraper
serve.py           CLI entry point for the API server
data/              SQLite database lives here (data/fifth_grape.db)
requirements.txt
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Python 3.10+ is required (the code uses `X | None` type syntax).

## Usage

**Run a scrape** (all chains, all available files):

```bash
python scrape.py
```

Optional flags:

```bash
python scrape.py --chains shufersal rami_levy   # subset
python scrape.py --limit 5                      # cap files per chain per type
python scrape.py -v                             # debug logging
```

**Run the API server:**

```bash
python serve.py
```

Then:

```bash
curl http://localhost:8000/v1/market/snapshot
```

The DB path can be overridden with the `FIFTH_GRAPE_DB` environment variable (default: `data/fifth_grape.db`).

## API

### `GET /v1/market/snapshot`

Returns the full current market snapshot — every store, product, and latest price in the database — in one payload. Shape:

```json
{
  "stores":   [{ "storeId", "chainId", "chainName", "branchName", "address", "city", "lat", "lng" }],
  "products": [{ "productId", "name", "brand", "unit", "barcode", "emoji", "category" }],
  "prices":   [{ "storeId", "productId", "price", "inStock", "updatedAt" }],
  "generatedAt": "ISO-8601 timestamp of the last successful scrape"
}
```

See [app/models.py](app/models.py) for exact field types.

## Data model

Four SQLite tables (see [app/db.py](app/db.py)):

- `stores` — keyed by `(store_id, chain_id)`
- `products` — keyed by `product_id` (typically the barcode)
- `prices` — keyed by `(store_id, chain_id, product_id)`
- `scrape_runs` — audit log of scraper runs with status/error
