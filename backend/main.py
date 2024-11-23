import os

import asyncpg  # type: ignore
from fastapi import FastAPI, HTTPException

from models import Offer, OfferRequest, Offers
from region import region_range

DATABASE_URL = os.environ.get("DATABASE_URL")


async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)


app = FastAPI()


@app.get("/api/offers")
async def get_offers(query: OfferRequest) -> dict:

    filters = []
    filter_params = []

    # Region ID
    if query.regionID < 21:
        min_r, max_r = region_range[query.regionID]
        filters.append(
            "most_specific_region_id >= $? AND most_specific_region_id <= $?"
        )
        filter_params.append(str(min_r))
        filter_params.append(str(max_r))
    else:
        filters.append("most_specific_region_id = $?")
        filter_params.append(str(query.regionID))

    # Time
    filters.append("start_date >= TO_TIMESTAMP($?)")
    filters.append("end_date <= TO_TIMESTAMP($?)")
    filter_params.append(str(query.timeRangeStart / 1000))
    filter_params.append(str(query.timeRangeEnd / 1000))

    # Days
    filters.append("(end_date - start_date) >= INTERVAL '$? days'")
    filter_params.append(str(query.numberDays))

    # Num of seats
    if query.minNumberSeats:
        filters.append("number_seats >= $?")
        filter_params.append(str(query.minNumberSeats))

    # price
    if query.minPrice:
        filters.append("price >= $?")
        filter_params.append(str(query.minPrice))
    if query.maxPrice:
        filters.append("price <= $?")
        filter_params.append(str(query.maxPrice))

    # car type
    if query.carType:
        filters.append("car_type = $?")
        filter_params.append(str(query.carType))

    filter_query = " WHERE " + " AND ".join(filters)

    # Order
    if query.sortOrder == "price-asc":
        order = "ORDER BY price"
    else:
        order = "ORDER BY price DESC"

    # Page size
    paging = "LIMIT $? OFFSET $?"
    paging_params = []
    paging_params.append(query.pageSize)
    paging_params.append(query.page)

    def 

    conn = await get_db_connection()
    try:
        # Offers
        offer_query = " ".join(
            ("SELECT id AS ID, data FROM rental_data", filter_query, order)
        )
        params = 
        offers = await conn.fetch(query, params)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()

    return {}


@app.post("/api/offers")
async def create_offers(offers: Offers) -> None:
    query = """
        INSERT INTO rental_data (
            ID,
            data,
            most_specific_region_id,
            start_date,
            end_date,
            number_seats,
            price,
            car_type,
            has_vollkasko,
            free_kilometers
        ) VALUES (
            $1, $2, $3, TO_TIMESTAMP($4), TO_TIMESTAMP($5), $6,
            $7, $8, $9, $10
        )
    """

    # Connect to the database
    conn = await get_db_connection()
    try:
        for offer in offers.offers:
            # Execute query for each offer
            await conn.execute(
                query,
                offer.ID,
                offer.data,
                offer.mostSpecificRegionID,
                offer.startDate / 1000,
                offer.endDate / 1000,
                offer.numberSeats,
                offer.price,
                offer.carType,
                offer.hasVollkasko,
                offer.freeKilometers,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()


@app.delete("/api/offers")
async def cleanup() -> None:
    query = "DELETE FROM rental_data"

    conn = await get_db_connection()
    try:
        for offer in offers.offers:
            await conn.execute(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()
