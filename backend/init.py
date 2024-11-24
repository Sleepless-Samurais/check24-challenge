import asyncio
import asyncpg  # type: ignore
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

connection = asyncio.run(asyncpg.connect(DATABASE_URL))
# asyncio.run(connection.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))