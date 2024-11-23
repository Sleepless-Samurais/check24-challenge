import json
import os

import asyncpg  # type: ignore
from fastapi import FastAPI, HTTPException, Query

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
    min_r, max_r = map(int, region_dict[str(query.regionID)])
    if min_r != max_r:
        filters.append(
            f"most_specific_region_id >= {min_r} \
                    AND most_specific_region_id <= {max_r}"
        )
    else:
        filters.append(f"most_specific_region_id = {min_r}")

    # Time
    filters.append(f"start_date >= TO_TIMESTAMP({query.timeRangeStart // 1000})")
    filters.append(f"end_date <= TO_TIMESTAMP({query.timeRangeEnd // 1000})")

    # Days
    filters.append(f"(end_date - start_date) >= INTERVAL '{query.numberDays} days'")

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

    conn = await get_db_connection()
    try:
        # Offers
        page_query = f"""
        WITH Page AS (
            SELECT * FROM rental_data
            {filter_query}
            {paging_query}
        )
        """

        # Order
        if query.sortOrder == "price-asc":
            order = "ORDER BY price"
        else:
            order = "ORDER BY price DESC"

        offer_query = f"{page_query} SELECT id AS ID, data FROM page {order}"

        rows = await conn.fetch(offer_query)
        offers = [dict(row) for row in rows]

        # Price range
        price_query = f"""
        {page_query},
        PriceBuckets AS (
            SELECT
                CAST(FLOOR(price / {query.priceRangeWidth}) AS INTEGER)
                        * {query.priceRangeWidth} AS rangeStart,
                CAST(FLOOR(price / {query.priceRangeWidth}) AS INTEGER)
                        * {query.priceRangeWidth} + {query.priceRangeWidth}
                        AS rangeEnd
            FROM
                Page
        )
        SELECT
            rangeStart AS start,
            rangeEnd AS end,
            COUNT(*) AS count
        FROM
            PriceBuckets
        GROUP BY
            rangeStart, rangeEnd
        ORDER BY
            rangeStart
        """
        rows = await conn.fetch(price_query)
        price_buckets = [dict(row) for row in rows]

        # car type counts
        car_type_query = f"""
        {page_query}
        SELECT
            car_type,
            COUNT(*) AS count
        FROM
            Page
        GROUP BY
            car_type
        """
        rows = await conn.fetch(car_type_query)
        car_type_buckets = {row["car_type"]: row["count"] for row in rows}

        # number seats
        num_seats_query = f"""
        {page_query}
        SELECT
            number_seats,
            COUNT(*) AS count
        FROM
            Page
        GROUP BY
            number_seats
        """
        rows = await conn.fetch(num_seats_query)
        num_seats = [
            {"numberSeats": row["number_seats"], "count": row["count"]} for row in rows
        ]

        # free km range
        free_km_query = f"""
        {page_query},
        KilometerBuckets AS (
            SELECT
                CAST(FLOOR(free_kilometers / {query.minFreeKilometerWidth})
                        AS INTEGER) * {query.minFreeKilometerWidth}
                        AS rangeStart,
                CAST(FLOOR(free_kilometers / {query.minFreeKilometerWidth})
                        AS INTEGER) * {query.minFreeKilometerWidth}
                        + {query.minFreeKilometerWidth}
                    AS rangeEnd
            FROM
                Page
        )
        SELECT
            rangeStart AS start,
            rangeEnd AS end,
            COUNT(*) AS count
        FROM
            KilometerBuckets
        GROUP BY
            rangeStart, rangeEnd
        ORDER BY
            rangeStart
        """
        rows = await conn.fetch(free_km_query)
        free_km = [dict(row) for row in rows]

        vollkasko_query = f"""
        SELECT COUNT(*) FROM (
            SELECT * FROM {page_query}
            WHERE has_vollkasko = true
        ) src;
        """
        print(vollkasko_query)
        true_count = await conn.fetchval(vollkasko_query)
        print(true_count)
        vollkasko = {"trueCount": true_count, "falseCount": len(offers) - true_count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()

    return {
        "offers": offers,
        "priceRanges": price_buckets,
        "carTypeCounts": car_type_buckets,
        "seatsCount": num_seats,
        "freeKilometerRange": free_km,
        "vollkaskoCounts": vollkasko,
    }


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
