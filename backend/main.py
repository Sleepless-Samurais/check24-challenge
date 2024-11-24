import os
import time
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

startup_lock = Lock()
started = False
stopped = False

time_get = 0
time_post = 0
time_delete = 0
time_sql = 0
count = 0

def print_stats():
    if count and count % 500 == 0:
        print(f"Stats: {time_get=}, {time_post=}, {time_delete=}, {time_sql=}, {count=}")

@app.on_event("startup")
async def startup():
    print("Starting lifespan...")

    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)

    async with startup_lock:
        global started
        if not started:
            started = True
            async with pool.acquire() as conn:
                # CREATE EXTENSION pg_stat_statements if not exists
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    
    print("Lifespan started")


@app.on_event("shutdown")
async def shutdown():
    print("Closing lifespan...")

    async with startup_lock:
        global stopped
        if not stopped:
            stopped = True
            async with pool.acquire() as conn:
                rows = await conn.fetchrow("SELECT * FROM pg_stat_statements")
                print(rows)

    if pool:
        await pool.close()
    print("Lifespan closed")


@app.get("/api/offers")
async def get_offers(query: OfferRequest = Query()):
    global time_get, time_sql, count
    start = time.time()

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

    start_sql = time.time()

    global pool
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(pg_query)
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
    
    time_get += time.time() - start
    time_sql += time.time() - start_sql
    count += 1
    print_stats()

    return Response(content=row["result"], media_type="application/json")


@app.post("/api/offers")
async def create_offers(req: Request) -> None:
    global time_post, time_sql, count

    start = time.time()

    async with lock:

        offers = json.loads(await req.body())

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

        entries = (
            (
                offer["ID"],
                offer["data"],
                offer["mostSpecificRegionID"],
                offer["startDate"] / 1000,
                offer["endDate"] / 1000,
                offer["numberSeats"],
                offer["price"],
                offer["carType"],
                offer["hasVollkasko"],
                offer["freeKilometers"],
            )
            for offer in offers["offers"]
        )
        
        start_sql = time.time()

        global pool
        async with pool.acquire() as conn:
            try:
                await conn.executemany(query, entries)
            except Exception as e:
                print(e)
                raise HTTPException(status_code=500, detail=f"Database error: {e}")
        
        time_post += time.time() - start
        time_sql += time.time() - start_sql
        count += 1
        print_stats()


@app.delete("/api/offers")
async def cleanup() -> None:
    global time_delete, time_sql, count
    start = time.time()

    query = "DELETE FROM rental_data"

    start_sql = time.time()

    global pool
    async with pool.acquire() as conn:
        try:
            await conn.execute(query)
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
    
    time_sql += time.time() - start_sql
    time_delete += time.time() - start
    count += 1
    print_stats()
