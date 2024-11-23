import os

import asyncpg
from fastapi import FastAPI, HTTPException, Response

from models import Offer, OfferRequest, Offers
from region import region_range

DATABASE_URL = os.environ.get("DATABASE_URL")


async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)


app = FastAPI()


@app.get("/api/offers")
async def get_offers(query: OfferRequest) -> dict:

    ## Doing filters
    filters = []
    filter_params = []

    # Region ID
    if query.RegionID < 21:
        min_r, max_r = region_range[query.RegionID]
        filters.append(
            "most_specific_region_id >= $? AND most_specific_region_id <= $?"
        )
        filter_params.append(str(min_r))
        filter_params.append(str(max_r))
    else:
        filters.append("most_specific_region_id = $?")
        filter_params.append(str(query.RegionID))

    # Time
    filters.append("start_date >= TO_TIMESTAMP($?)")
    filters.append("end_date <= TO_TIMESTAMP($?)")
    filter_params.append(str(query.StartRange / 1000))
    filter_params.append(str(query.EndRange / 1000))

    # Days
    filters.append("(end_date - start_date) >= INTERVAL '$? days'")
    filter_params.append(str(query.NumberDays))

    # Num of seats
    if query.MinNumberSeats:
        filters.append("number_seats >= $?")
        filter_params.append(str(query.MinNumberSeats))

    # price
    if query.MinPrice:
        filters.append("price >= $?")
        filter_params.append(str(query.MinPrice))
    if query.MaxPrice:
        filters.append("price <= $?")
        filter_params.append(str(query.MaxPrice))

    # car type
    if query.CarType:
        filters.append("car_type = $?")
        filter_params.append(str(query.CarType))

    filter_query = " WHERE " + " AND ".join(filters)


    ## Doing Order
    if query.SortOrder == "price-asc":
        order = "ORDER BY price"
    else:
        order = "ORDER BY price DESC"

    ## Doing paging
    paging = "LIMIT $? OFFSET $?"
    paging_params = []
    paging_params.append(query.PageSize)
    paging_params.append(query.Page)

    conn = await get_db_connection()
    try:
        # Offers
        query_string = " ".join(
            ("SELECT id AS ID, data FROM rental_data", filter_query, order, paging)
        )
        params = filter_params + paging_params
        offers = await conn.fetch(query_string, params)
        print(offers)
        return offers

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()


@app.post("/api/offers")
async def create_offers(offers: Offers):
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
        # TODO: optimization point
        for offer in offers.Offers:
            # Execute query for each offer
            await conn.execute(
                query,
                [
                    offer.ID,
                    offer.Data,
                    offer.MostSpecificRegionID,
                    offer.StartDate / 1000,
                    offer.EndDate / 1000,
                    offer.NumberSeats,
                    offer.Price,
                    offer.CarType,
                    offer.HasVollkasko,
                    offer.FreeKilometers,
                ]
            )
        return Response(status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()


@app.delete("/api/offers")
async def cleanup():
    query = "DELETE FROM rental_data"

    conn = await get_db_connection()
    try:
        await conn.execute(query)
        return Response(status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()
