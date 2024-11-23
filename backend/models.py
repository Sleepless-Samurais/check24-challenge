import uuid
from pydantic import BaseModel, Field
from typing import Optional, Literal


class Offer(BaseModel):
    OfferID: str
    Data: str = Field(max_length=255)
    RegionID: int
    NumberDays: int
    StartTimestamp: int
    EndTimestamp: int
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
