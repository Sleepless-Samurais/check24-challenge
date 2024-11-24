import os
import time
from asyncio import Condition, Lock

import psycopg
import psycopg_pool

import orjson as json
from fastapi import FastAPI, HTTPException, Query, Request, Response

from models import Offer, OfferRequest, Offers

DATABASE_URL = os.environ.get("DATABASE_URL")

with open("region_array.json", "rt") as fin:
    region_dict = json.loads(fin.read())

app = FastAPI()

conn_pool = None

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
    global conn_pool
    assert DATABASE_URL
    conn_pool = psycopg_pool.AsyncConnectionPool(DATABASE_URL, min_size=20, max_size=40)
    await conn_pool.open()
    print("Lifespan started")

@app.on_event("shutdown")
async def shutdown():
    global conn_pool
    if conn_pool:
        await conn_pool.close()
    print("Lifespan closed")

@app.get("/api/offers")
async def get_offers(query: OfferRequest = Query()):
    global time_get, time_sql, count
    start = time.time()

    filters: list[str] = []
    optional_filters: dict[str, str | None] = {
        "number_seats": None,
        "price": None,
        "car_type": None,
        "has_vollkasko": None,
        "free_kilometers": None,
    }

    region_filter = []
    for r in region_dict[str(query.regionID)]:
        min_r, max_r = map(int, r)
        if min_r != max_r:
            region_filter.append(
                f"(most_specific_region_id >= {min_r} AND most_specific_region_id <= {max_r})"
            )
        else:
            region_filter.append(f"(most_specific_region_id = {min_r})")
    filters.append("(" + " OR ".join(region_filter) + ")")

    filters.append(f"EXTRACT(EPOCH FROM start_date) >= {query.timeRangeStart / 1000}")
    filters.append(f"EXTRACT(EPOCH FROM end_date) <= {query.timeRangeEnd / 1000}")
    filters.append(f"(end_date - start_date) = INTERVAL '{query.numberDays} days'")

    if query.minNumberSeats:
        optional_filters["number_seats"] = f"number_seats >= {query.minNumberSeats}"

    if query.minPrice or query.maxPrice:
        tmp = []
        if query.minPrice:
            tmp.append(f"price >= {query.minPrice}")
        if query.maxPrice:
            tmp.append(f"price < {query.maxPrice}")
        optional_filters["price"] = " AND ".join(tmp)

    if query.carType:
        optional_filters["car_type"] = f"car_type = '{query.carType}'"

    if query.onlyVollkasko:
        optional_filters["has_vollkasko"] = "has_vollkasko = TRUE"

    if query.minFreeKilometer:
        optional_filters["free_kilometers"] = f"free_kilometers >= {query.minFreeKilometer}"

    def where_clause(column=None):
        res = [v for k, v in optional_filters.items() if k != column and v]
        if len(res) == 0:
            return ""
        return "WHERE " + " AND ".join(res)

    filter_query = "WHERE " + " AND ".join(filters)
    paging_query = f"LIMIT {query.pageSize} OFFSET {query.page * query.pageSize}"

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

    global conn_pool
    async with conn_pool.connection() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(pg_query)
                row = await cur.fetchall()
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=f"Database error: {e}")

    time_get += time.time() - start
    time_sql += time.time() - start_sql
    count += 1
    print_stats()
    # print(row)

    return Response(content=json.dumps(row[0]), media_type="application/json")

@app.post("/api/offers")
async def create_offers(req: Request) -> None:
    global time_post, time_sql, count

    start = time.time()

    offers = json.loads(req.body())

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
            %s, %s, %s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), %s,
            %s, %s, %s, %s
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

    global conn_pool
    assert conn_pool
    async with conn_pool.connection() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.executemany(query, entries)
                await conn.commit()
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

    global conn_pool
    assert conn_pool
    async with conn_pool.connection() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(query)
                await conn.commit()
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
    
    time_sql += time.time() - start_sql
    time_delete += time.time() - start
    count += 1
    print_stats()