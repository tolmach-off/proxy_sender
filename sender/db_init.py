import asyncio

from aiopg import create_pool

from sender.settings import DATABASE_SETTINGS


async def create_tables():
    pool = await create_pool(**DATABASE_SETTINGS)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                '''CREATE TABLE IF NOT EXISTS messages (
                    id serial PRIMARY KEY,
                    data varchar,
                    status varchar(16),
                    created_at timestamp default CURRENT_TIMESTAMP,
                    updated_at timestamp default CURRENT_TIMESTAMP
                )'''
            )
            await cur.execute(
                '''CREATE TABLE IF NOT EXISTS errors (
                    id serial PRIMARY KEY,
                    error varchar,
                    message_id int references messages(id),
                    created_at timestamp default CURRENT_TIMESTAMP
                )'''
            )


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_tables())
