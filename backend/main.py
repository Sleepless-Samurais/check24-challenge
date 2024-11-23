import json
import os

import asyncpg  # type: ignore
from fastapi import FastAPI, HTTPException, Query, Response

from models import Offer, OfferRequest, Offers

DATABASE_URL = os.environ.get("DATABASE_URL")

with open("region_array.json", "rt") as fin:
    region_dict = json.load(fin)


async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)


app = FastAPI()


@app.get("/api/offers")
async def get_offers(query: OfferRequest = Query()) -> dict:

    filters: list[str] = []

    # Region ID
    region_filter = []
    for r in region_dict[str(query.regionID)]:
        min_r, max_r = map(int, r)
        if min_r != max_r:
            region_filter.append(
                f"(most_specific_region_id >= {min_r} \
                        AND most_specific_region_id <= {max_r})"
            )
        else:
            region_filter.append(f"(most_specific_region_id = {min_r})")
    filters.append("(" + " OR ".join(region_filter) + ")")

    # Time
    filters.append(
        f"EXTRACT(EPOCH FROM start_date)\
                   >= {query.timeRangeStart // 1000}"
    )
    filters.append(
        f"EXTRACT(EPOCH FROM end_date)\
            <= {query.timeRangeEnd // 1000}"
    )

    # Days
    filters.append(
        f"(end_date - start_date)\
            >= INTERVAL '{query.numberDays} days'"
    )

    # Num of seats
    if query.minNumberSeats:
        filters.append(f"number_seats >= {query.minNumberSeats}")

    # price
    if query.minPrice:
        filters.append(f"price >= {query.minPrice}")
    if query.maxPrice:
        filters.append(f"price <= {query.maxPrice}")

    # car type
    if query.carType:
        filters.append(f"car_type = {query.carType}")

    # vollkasko
    if query.onlyVollkasko:
        filters.append("has_vollkasko = true")

    # min free km
    if query.minFreeKilometer:
        filters.append(f"free_kilometers >= {query.minFreeKilometer}")

    filter_query = " WHERE " + " AND ".join(filters)

    # Page size
    paging_query = f"LIMIT {query.pageSize} OFFSET {query.page}"

    # Order
    if query.sortOrder == "price-asc":
        order_clause = "ORDER BY price, id"
    else:
        order_clause = "ORDER BY price DESC, id DESC"

    conn = await get_db_connection()

    pg_query = f"""
    WITH Page AS (
        SELECT * FROM rental_data
        {filter_query}
        {paging_query}
    ),

    Offers AS (
        SELECT
            id as ID,
            data
        FROM Page
        {order_clause}
    ),

    PriceBuckets AS (
        SELECT
            rangeStart AS start,
            rangeEnd AS end,
            COUNT(*) AS count
        FROM (
            SELECT
                CAST(FLOOR(price / {query.priceRangeWidth}) AS INTEGER)
                        * {query.priceRangeWidth} AS rangeStart,
                CAST(FLOOR(price / {query.priceRangeWidth}) AS INTEGER)
                        * {query.priceRangeWidth} + {query.priceRangeWidth}
                        AS rangeEnd
            FROM
                Page
        )
        GROUP BY rangeStart, rangeEnd
        ORDER BY rangeStart
    ),

    PredefinedCarTypes AS (
        SELECT 'small' AS car_type
        UNION ALL
        SELECT 'sports'
        UNION ALL
        SELECT 'luxury'
        UNION ALL
        SELECT 'family'
    ),

    CarTypeCounts AS (
        SELECT
            pct.car_type as car_type,
            COALESCE(COUNT(p.car_type), 0) AS count
        FROM PredefinedCarTypes pct
        LEFT JOIN Page p ON pct.car_type = p.car_type
        GROUP BY pct.car_type
    ),

    KilometerBuckets AS (
        SELECT
            rangeStart AS start,
            rangeEnd AS end,
            COUNT(*) AS count
        FROM (
            SELECT
                CAST(FLOOR(free_kilometers / {query.minFreeKilometerWidth})
                        AS INTEGER) * {query.minFreeKilometerWidth}
                        AS rangeStart,
                CAST(FLOOR(free_kilometers / {query.minFreeKilometerWidth})
                        AS INTEGER) * {query.minFreeKilometerWidth}
                        + {query.minFreeKilometerWidth}
                    AS rangeEnd
            FROM Page
        )
        GROUP BY rangeStart, rangeEnd
        ORDER BY rangeStart
    ),

    SeatsCount AS (
        SELECT
            number_seats AS numberSeats,
            COUNT(*) AS count
        FROM Page
        GROUP BY number_seats
    ),

    VollkaskoCount AS (
        SELECT
            COUNT(*) FILTER (WHERE has_vollkasko = true) AS trueCount,
            COUNT(*) FILTER (WHERE has_vollkasko = false) AS falseCount
        FROM Page
    )

    SELECT
        json_build_object(
        'offers', COALESCE((SELECT json_agg(json_build_object('id', id, 'data', data)) FROM Page), '[]'::json),
        'priceRanges', COALESCE((SELECT json_agg(PriceBuckets) FROM PriceBuckets), '[]'::json),
        'carTypeCounts', COALESCE((SELECT json_object_agg(car_type, count) FROM CarTypeCounts), '[]'::json),
        'seatsCount', COALESCE((SELECT json_agg(SeatsCount) FROM SeatsCount), '[]'::json),
        'freeKilometerRange', COALESCE((SELECT json_agg(KilometerBuckets) FROM KilometerBuckets), '[]'::json),
        'vollkaskoCount', COALESCE(json_build_object(
            'trueCount', (SELECT trueCount from VollkaskoCount),
            'falseCount', (SELECT falseCount from VollkaskoCount)
        ), '[]'::json)
    ) AS result
    """

    try:
        row = await conn.fetchrow(pg_query)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()

    return Response(content=row["result"], media_type="application/json")


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
        await conn.execute(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()
