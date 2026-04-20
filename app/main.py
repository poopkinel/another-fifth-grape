"""FastAPI application for Fifth Grape backend."""

import os
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from app.db import (
    get_conn,
    init_db,
    search_products,
    get_prices_for_products,
    get_products_by_ids,
    get_stores_by_keys,
    get_last_scrape_time,
    get_canonical_groups,
)
from app.models import PriceLookupRequest, PriceLookupResponse, Store, Product, Price

EXPAND_CANONICAL = os.environ.get("EXPAND_CANONICAL", "true").lower() in ("true", "1", "yes")

app = FastAPI(title="Fifth Grape API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()


def _display_chain_name(store_row) -> str:
    """Override chain_name with the Places-derived brand for known sub-brands
    that share a single feed (e.g., Carrefour publishes under yeinot_bitan).
    """
    places_name = (store_row["places_name"] or "").lower()
    if store_row["chain_id"] == "yeinot_bitan":
        if "קרפור" in places_name or "carrefour" in places_name:
            return "קרפור"
    return store_row["chain_name"]


@app.get("/v1/products/search", response_model=list[Product])
def products_search(
    q: str = Query(..., min_length=1, description="Search text (name, brand, or exact barcode)"),
    limit: int = Query(50, ge=1, le=200),
):
    with get_conn() as conn:
        rows = search_products(conn, q, limit)

    return [
        Product(
            productId=p["product_id"],
            name=p["name"],
            brand=p["brand"],
            unit=p["unit"],
            barcode=p["barcode"],
            emoji=p["emoji"],
            category=p["category"],
        )
        for p in rows
    ]


@app.post("/v1/prices/lookup", response_model=PriceLookupResponse)
def lookup_prices(req: PriceLookupRequest):
    requested_ids = list(dict.fromkeys(req.productIds))

    with get_conn() as conn:
        if EXPAND_CANONICAL:
            groups = get_canonical_groups(conn, requested_ids)
        else:
            groups = {pid: [pid] for pid in requested_ids}

        all_lookup_ids = list({m for members in groups.values() for m in members})
        raw_prices_underlying = (
            get_prices_for_products(conn, all_lookup_ids) if all_lookup_ids else []
        )

        # underlying product_id → which requested id(s) it serves
        underlying_to_requested: dict[str, list[str]] = {}
        for req_id, members in groups.items():
            for m in members:
                underlying_to_requested.setdefault(m, []).append(req_id)

        # Dedupe by (store, chain, requested_id): prefer in_stock, then lowest price.
        best: dict[tuple[str, str, str], dict] = {}
        for price in raw_prices_underlying:
            for req_id in underlying_to_requested.get(price["product_id"], []):
                key = (price["store_id"], price["chain_id"], req_id)
                cur = best.get(key)
                if cur is None:
                    best[key] = price
                    continue
                cur_in, new_in = bool(cur["in_stock"]), bool(price["in_stock"])
                if new_in and not cur_in:
                    best[key] = price
                elif new_in == cur_in and price["price"] < cur["price"]:
                    best[key] = price

        raw_prices = []
        for (_s, _c, req_id), price in best.items():
            relabelled = dict(price)
            relabelled["product_id"] = req_id
            raw_prices.append(relabelled)

        matched_product_ids = list({p["product_id"] for p in raw_prices})
        raw_products = get_products_by_ids(conn, matched_product_ids)
        store_keys = list({(p["store_id"], p["chain_id"]) for p in raw_prices})
        raw_stores = get_stores_by_keys(conn, store_keys)
        last_scrape = get_last_scrape_time(conn)

    stores = [
        Store(
            storeId=s["store_id"],
            chainId=s["chain_id"],
            chainName=_display_chain_name(s),
            branchName=s["branch_name"],
            address=s["address"],
            city=s["city"],
            lat=s["lat"],
            lng=s["lng"],
            geocodeStatus=s["geocode_status"],
        )
        for s in raw_stores
    ]

    products = [
        Product(
            productId=p["product_id"],
            name=p["name"],
            brand=p["brand"],
            unit=p["unit"],
            barcode=p["barcode"],
            emoji=p["emoji"],
            category=p["category"],
        )
        for p in raw_products
    ]

    prices = [
        Price(
            storeId=p["store_id"],
            productId=p["product_id"],
            price=p["price"],
            inStock=bool(p["in_stock"]),
            updatedAt=p["updated_at"],
        )
        for p in raw_prices
    ]

    return PriceLookupResponse(
        stores=stores,
        products=products,
        prices=prices,
        generatedAt=last_scrape or datetime.now(timezone.utc).isoformat(),
    )
