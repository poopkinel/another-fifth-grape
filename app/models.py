"""Pydantic models matching the API contract exactly."""

from pydantic import BaseModel, Field


class Store(BaseModel):
    storeId: str
    chainId: str
    chainName: str
    branchName: str
    # chainId is per legal entity (corporate parent); subChainId carries the
    # consumer brand (e.g. yeinot_bitan publishes Sheli, Carrefour, Be'er,
    # Quik all under one chainId — they're distinguishable only by subChain*).
    subChainId: str | None = None
    subChainName: str | None = None
    address: str
    city: str
    lat: float | None
    lng: float | None
    geocodeStatus: str | None  # None = not yet attempted; 'ok' = lat/lng present; 'no_results' = tried, no match


class Product(BaseModel):
    productId: str
    name: str
    brand: str | None
    unit: str | None
    barcode: str | None
    emoji: str | None
    category: str | None
    imageUrl: str | None = None


class Price(BaseModel):
    storeId: str
    productId: str
    price: float
    inStock: bool
    updatedAt: str


class PriceLookupRequest(BaseModel):
    productIds: list[str]


class PriceLookupResponse(BaseModel):
    stores: list[Store]
    products: list[Product]
    prices: list[Price]
    generatedAt: str


class EventIn(BaseModel):
    event: str = Field(..., min_length=1, max_length=64)
    client_ts: int = Field(..., ge=0)
    properties: dict = Field(default_factory=dict)


class EventBatch(BaseModel):
    distinct_id: str = Field(..., min_length=1, max_length=64)
    app_version: str | None = Field(default=None, max_length=32)
    platform: str | None = Field(default=None, max_length=16)
    events: list[EventIn] = Field(..., min_length=1, max_length=100)


class EventBatchResponse(BaseModel):
    ingested: int
