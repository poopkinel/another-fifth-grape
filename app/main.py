"""FastAPI application for Fifth Grape backend."""

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db import get_conn, init_db, get_all_stores, get_all_products, get_all_prices, get_last_scrape_time
from app.models import SnapshotResponse, Store, Product, Price

app = FastAPI(title="Fifth Grape API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()


@app.get("/v1/market/snapshot", response_model=SnapshotResponse)
def market_snapshot():
    with get_conn() as conn:
        raw_stores = get_all_stores(conn)
        raw_products = get_all_products(conn)
        raw_prices = get_all_prices(conn)
        last_scrape = get_last_scrape_time(conn)

    stores = [
        Store(
            storeId=s["store_id"],
            chainId=s["chain_id"],
            chainName=s["chain_name"],
            branchName=s["branch_name"],
            address=s["address"],
            city=s["city"],
            lat=s["lat"],
            lng=s["lng"],
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

    return SnapshotResponse(
        stores=stores,
        products=products,
        prices=prices,
        generatedAt=last_scrape or datetime.now(timezone.utc).isoformat(),
    )
