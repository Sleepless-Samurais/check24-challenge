import io
import os
import asyncio
from asyncio import Condition, Lock

import asyncpg  # type: ignore
import orjson as json
from fastapi import FastAPI, HTTPException, Query, Request, Response

from models import Offer, OfferRequest, Offers

DATABASE_URL = os.environ.get("DATABASE_URL")

with open("region_array.json", "rt") as fin:
    region_dict = json.loads(fin.read())

app = FastAPI()
lock = Lock()
condition = Condition()

pool: asyncpg.Pool | None = None


@app.on_event("startup")
async def startup():
    global pool
    # Initialize the connection pool during the application startup
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=20, max_size=50)


@app.on_event("shutdown")
async def shutdown():
    global pool
    # Close the connection pool during the application shutdown
    if pool:
        await pool.close()


@app.get("/api/offers")
async def get_offers(query: OfferRequest = Query()):

    async with condition:
        await condition.wait_for(lambda: not lock.locked())

    filters: list[str] = []
    optional_filters: dict[str, str | None] = {
        "number_seats": None,
        "price": None,
        "car_type": None,
        "has_vollkasko": None,
        "free_kilometers": None,
    }

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
                   >= {query.timeRangeStart / 1000}"
    )
    filters.append(
        f"EXTRACT(EPOCH FROM end_date)\
            <= {query.timeRangeEnd / 1000}"
    )

    # Days
    filters.append(f"(end_date - start_date) = INTERVAL '{query.numberDays} days'")

    # Num of seats
    if query.minNumberSeats:
        optional_filters["number_seats"] = f"number_seats >= {query.minNumberSeats}"

    # price
    if query.minPrice or query.maxPrice:
        tmp = []
        if query.minPrice:
            tmp.append(f"price >= {query.minPrice}")
        if query.maxPrice:
            tmp.append(f"price < {query.maxPrice}")
        optional_filters["price"] = " AND ".join(tmp)

    # car type
    if query.carType:
        optional_filters["car_type"] = f"car_type = '{query.carType}'"

    # vollkasko
    if query.onlyVollkasko:
        optional_filters["has_vollkasko"] = "has_vollkasko = TRUE"

    # min free km
    if query.minFreeKilometer:
        optional_filters["free_kilometers"] = (
            f"free_kilometers >= {query.minFreeKilometer}"
        )

    def where_clause(column: str | None = None) -> str:
        res = [v for k, v in optional_filters.items() if k != column and v]
        if len(res) == 0:
            return ""
        return "WHERE " + " AND ".join(res)

    # Page size
    filter_query = "WHERE " + " AND ".join(filters)
    paging_query = f"LIMIT {query.pageSize} OFFSET {query.page * query.pageSize}"

    # Order
    if query.sortOrder == "price-asc":
        order_clause = "ORDER BY price, id"
    else:
        order_clause = "ORDER BY price DESC, id"

    pg_query = f"""
    WITH Page AS (
        SELECT * FROM rental_data
        {filter_query}
    ),

    Offers AS (
        SELECT
            id as ID,
            data
        FROM Page
        {where_clause()}
        {order_clause}
        {paging_query}
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
            {where_clause("price")}
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
            COALESCE(SUM(CASE WHEN p.car_type IS NOT NULL THEN 1 ELSE 0 END),
                     0) AS count
        FROM PredefinedCarTypes pct
        LEFT JOIN Page p ON pct.car_type = p.car_type
        {where_clause("car_type")}
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
            {where_clause("free_kilometers")}
        )
        GROUP BY rangeStart, rangeEnd
        ORDER BY rangeStart
    ),

    SeatsCount AS (
        SELECT
            number_seats AS numberSeats,
            COUNT(*) AS count
        FROM Page
        {where_clause("number_seats")}
        GROUP BY number_seats
    ),

    VollkaskoCount AS (
        SELECT
            COUNT(*) FILTER (WHERE has_vollkasko = true) AS trueCount,
            COUNT(*) FILTER (WHERE has_vollkasko = false) AS falseCount
        FROM Page
        {where_clause("has_vollkasko")}
    )

    SELECT
        json_build_object(
        'offers', COALESCE((SELECT json_agg(json_build_object('id', id, 'data', data)) FROM Offers), '[]'::json),
        'priceRanges', COALESCE((SELECT json_agg(PriceBuckets) FROM PriceBuckets), '[]'::json),
        'carTypeCounts', (SELECT json_object_agg(car_type, count) FROM CarTypeCounts),
        'seatsCount', COALESCE((SELECT json_agg(SeatsCount) FROM SeatsCount), '[]'::json),
        'freeKilometerRange', COALESCE((SELECT json_agg(KilometerBuckets) FROM KilometerBuckets), '[]'::json),
        'vollkaskoCount', COALESCE(json_build_object(
            'trueCount', (SELECT trueCount from VollkaskoCount),
            'falseCount', (SELECT falseCount from VollkaskoCount)
        ), '[]'::json)
    ) AS result
    """

    global pool
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(pg_query)
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=f"Database error: {e}")

    return Response(content=row["result"], media_type="application/json")


@app.post("/api/offers")
async def create_offers(req: Request) -> None:

    async with lock:

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

        async def write_on_db(offer):
            global pool
            async with pool.acquire() as conn:
                try:
                    await conn.execute(query,
                        offer["ID"],
                        offer["data"],
                        offer["mostSpecificRegionID"],
                        offer["startDate"] / 1000,
                        offer["endDate"] / 1000,
                        offer["numberSeats"],
                        offer["price"],
                        offer["carType"],
                        offer["hasVollkasko"],
                        offer["freeKilometers"]
                    )
                except Exception as e:
                    print(e)
                    raise HTTPException(status_code=500, detail=f"Database error: {e}")

        # starting
        found_body = False
        found_start = False
        found_end = False
        buffer = ""
        async with asyncio.TaskGroup() as tg:
            async for chunk in req.stream():
                buffer += chunk.decode()
                has_to_run = True
                while has_to_run:
                    if not found_body:
                        if not "{" in buffer:
                            has_to_run = False
                            continue
                        found_body = True
                        idx = buffer.index("{")
                        buffer = buffer[idx+1:]
                    else:
                        if not found_start:
                            if not "{" in buffer:
                                has_to_run = False
                                continue
                            found_start = True
                            idx = buffer.index("{")
                            buffer = buffer[idx:]
                        elif not found_end:
                            if not "}" in buffer:
                                has_to_run = False
                                continue
                            found_end = True
                            idx = buffer.index("}")
                            tg.create_task(
                                write_on_db(json.loads(buffer[:idx+1]))
                            )
                            buffer = buffer[idx+1:]
                            found_start = False
                            found_end = False


@app.delete("/api/offers")
async def cleanup() -> None:
    query = "DELETE FROM rental_data"

    global pool
    async with pool.acquire() as conn:
        try:
            await conn.execute(query)
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
