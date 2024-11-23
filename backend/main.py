import json
import os

import asyncio
import asyncpg  # type: ignore
from fastapi import FastAPI, HTTPException, Query

from models import Offer, OfferRequest, Offers

DATABASE_URL = os.environ.get("DATABASE_URL")

with open("region_array.json", "rt") as fin:
    region_dict = json.load(fin)


async def get_db_connection():
    return await asyncpg.create_pool(DATABASE_URL)


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
    filters.append(f"EXTRACT(EPOCH FROM start_date) >= {query.timeRangeStart // 1000}")
    filters.append(f"EXTRACT(EPOCH FROM end_date) <= {query.timeRangeEnd // 1000}")

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

        async def get_offers() -> list:
            if query.sortOrder == "price-asc":
                order = "ORDER BY price, id"
            else:
                order = "ORDER BY price DESC, id DESC"

            offer_query = f"""{page_query} SELECT id AS ID, data FROM page {order}"""

            rows = await (await conn.acquire()).fetch(offer_query)
            return [dict(row) for row in rows]
        offers = get_offers()

        async def get_price_range() -> list:
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
            rows = await (await conn.acquire()).fetch(price_query)
            return [dict(row) for row in rows]
        price_buckets = get_price_range()

        async def get_car_type() -> dict:
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
            rows = await (await conn.acquire()).fetch(car_type_query)
            return {row["car_type"]: row["count"] for row in rows}
        car_type_buckets = get_car_type()

        async def get_number_seats() -> list:
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
            rows = await (await conn.acquire()).fetch(num_seats_query)
            return [
                {"numberSeats": row["number_seats"], "count": row["count"]} for row in rows
            ]
        num_seats = get_number_seats()

        async def get_free_km() -> list:
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
            rows = await (await conn.acquire()).fetch(free_km_query)
            return [dict(row) for row in rows]
        free_km = get_free_km()

        # vollkasko count
        async def get_vollkasko() -> dict:
            vollkasko_query = f"""
            {page_query}
            SELECT COUNT(*) FROM (
                SELECT * FROM Page
                WHERE has_vollkasko = true
            ) src;
            """
            true_count = await (await conn.acquire()).fetchval(vollkasko_query)
            return {"trueCount": true_count, "falseCount": len(await offers) - true_count}
        vollkasko = get_vollkasko()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await conn.close()

    return {
        "offers": await offers,
        "priceRanges": await price_buckets,
        "carTypeCounts": await car_type_buckets,
        "seatsCount": await num_seats,
        "freeKilometerRange": await free_km,
        "vollkaskoCount": await vollkasko,
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
