import uuid
from pydantic import BaseModel, Field
from typing import Optional, Literal


class Offer(BaseModel):
    ID: str
    data: str = Field(max_length=255)
    mostSpecificRegionID: int
    startDate: int
    endDate: int
    numberSeats: int
    price: int
    carType: str
    hasVollkasko: bool
    freeKilometers: int


class Offers(BaseModel):
    offers: list[Offer]


class OfferRequest(BaseModel):
    regionID: int
    timeRangeStart: int
    timeRangeEnd: int
    numberDays: int
    sortOrder: Literal["price-asc", "price-desc"]
    page: int
    pageSize: int
    priceRangeWidth: int
    minFreeKilometerWidth: int
    minNumberSeats: Optional[int] = None
    minPrice: Optional[float] = None
    maxPrice: Optional[float] = None
    carType: Optional[Literal["small", "sports", "luxury", "family"]] = None
    onlyVollkasko: Optional[bool] = None
    minFreeKilometer: Optional[int] = None
