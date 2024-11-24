import asyncio
import asyncpg  # type: ignore
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

async def main():
	connection = await asyncpg.connect(DATABASE_URL)
	result = await connection.execute("ALTER SYSTEM SET shared_buffers = '2GB';")
	print(result)

asyncio.run(main())