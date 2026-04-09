"""Pydantic models matching the API contract exactly."""

from pydantic import BaseModel


class Store(BaseModel):
    storeId: str
    chainId: str
    chainName: str
    branchName: str
    address: str
    city: str
    lat: float | None
    lng: float | None


class Product(BaseModel):
    productId: str
    name: str
    brand: str | None
    unit: str | None
    barcode: str | None
    emoji: str | None
    category: str | None


class Price(BaseModel):
    storeId: str
    productId: str
    price: float
    inStock: bool
    updatedAt: str


class SnapshotResponse(BaseModel):
    stores: list[Store]
    products: list[Product]
    prices: list[Price]
    generatedAt: str
