import uuid
from pydantic import BaseModel, Field
from typing import Optional, Literal


class Offer(BaseModel):
    OfferID: str
    RegionID: int
    NumberDays: int
    StartTimestamp: str
    EndTimestamp: str
    NumberSeats: int
    Price: int
    CarType: str
    HasVollkasko: bool
    FreeKilometers: int


class Offers(BaseModel):
    ID: str
    Offers: list[Offer]


class OfferRequest(BaseModel):
    RegionID: int
    StartRange: int
    EndRange: int
    NumberDays: int
    SortOrder: Literal["price-asc", "price-desc"]
    Page: int
    PageSize: int
    PriceRangeWidth: int
    MinFreeKilometerWidth: int
    MinNumberSeats: Optional[int] = None
    MinPrice: Optional[float] = None
    MaxPrice: Optional[float] = None
    CarType: Optional[Literal["small", "sports", "luxury", "family"]] = None
    OnlyVollkasko: Optional[bool] = None
    MinFreeKilometer: Optional[int] = None
