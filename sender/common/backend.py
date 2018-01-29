import json
import logging
import datetime
import traceback

from aiopg import create_pool

from .exceptions import TaskError


TASK_NEW = 'NEW'
TASK_RUNNING = 'RUNNING'
TASK_SUCCESS = 'SUCCESS'
TASK_FAILURE = 'FAILURE'
TASK_FATAL = 'FATAL'


class Task:

    def __init__(self, task_id, data, broker_backend, handlers=None):
        self.data = data
        self.handlers = list(handlers) if handlers is not None else []
        self._id = task_id
        self._backend = broker_backend

    @property
    def id(self):
        return self._id

    def add_handler(self, handler):
        self.handlers.append(handler)

    async def run(self):
        try:
            self.data = json.loads(self.data)
            for handler in self.handlers:
                self.data = await handler(self.data)
        except TaskError:
            logging.error('Failed task: {id}'.format(id=self.id))
            await self.on_failure()
        except Exception as exc:
            logging.exception(exc)
            await self.on_fatal(traceback.format_exc())
        else:
            await self.on_success()

    async def update_state(self, new_state):
        await self._backend.update_state(self._id, new_state)

    async def on_success(self):
        await self.update_state(TASK_SUCCESS)

    async def on_failure(self):
        await self.update_state(TASK_FAILURE)

    async def on_fatal(self, exc):
        await self.update_state(TASK_FATAL)
        await self._backend.save_error(self._id, exc)


class BDBackend:

    def __init__(self, connection_pool=None):
        self.connection_pool = connection_pool

    async def connect(self, *args, **kwargs):
        if self.connection_pool is None:
            self.connection_pool = await create_pool(*args, **kwargs)

    async def get_messages(self, count=10):
        async with self.connection_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """BEGIN ISOLATION LEVEL SERIALIZABLE;

                        SELECT id, data FROM messages
                        WHERE status IN {statuses}
                        LIMIT %s
                        FOR UPDATE;
                    """.format(statuses=(TASK_NEW, TASK_FAILURE)),
                    (count, )
                )
                messages = [message for message in cur]
                if messages:
                    await cur.execute(
                        """UPDATE messages SET status = 'RUNNING'
                        WHERE id IN ({0});""".format(
                            *[message[0] for message in messages]
                        )
                    )
                await cur.execute('COMMIT;')
                return messages

    async def create_message(self, data):
        async with self.connection_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """INSERT INTO messages (data, status)
                    VALUES (%s, %s)
                    """,
                    (data, TASK_NEW)
                )

    async def update_state(self, message_id, new_state):
        async with self.connection_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """UPDATE messages SET status = %s,
                    updated_at = %s
                    WHERE id = %s
                    """,
                    (new_state, datetime.datetime.now(), message_id)
                )

    async def save_error(self, message_id, error):
        async with self.connection_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """INSERT INTO errors (message_id, error)
                    VALUES (%s, %s)
                    """,
                    (message_id, error)
                )
