import asyncio
import functools
import logging

import aioamqp

from sender.settings import DATABASE_SETTINGS, BROKER_SETTINGS, QUEUE_SETTINGS
from sender.common.backend import BDBackend


class ChannelConsumer:

    def __init__(self, channel, handler, concurrency=10, loop=None):
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._stopped = False
        self._channel = channel
        self._message_handler = handler
        self._semaphore = asyncio.Semaphore(concurrency)
        self._tasks = {}
        self._on_closed_task = self._loop.create_task(self.on_close_channel())

    async def _handle_message(self, delivery_tag, message):
        """Метод обрабатывает входящее сообщение передавая его
        обработчику (self._message_handler).
        В случае возникновения ошибки сообщение считается не обработанным
        и возвращается в очередь.
        """
        try:
            await self._message_handler(message)
            await self._channel.basic_client_ack(delivery_tag)
        except Exception as exc:
            logging.exception(exc)
            await self._channel.basic_client_nack(delivery_tag)
        finally:
            self._semaphore.release()

    def start(self):
        logging.info('Starting subscribe on channel: {}')
        self._stopped = False

    def stop(self):
        logging.info('Stopping subscribe on channel: {}')
        self._stopped = True

    async def create_queue(self, queue_name, **kwargs):
        await self._channel.queue_declare(queue_name, **kwargs)

    async def subscribe(self, queue_name, timeout=1, **kwargs):
        if self._stopped:
            self.start()
        logging.info('Subscribe on channel: {} was started')
        await self.create_queue(queue_name, **kwargs)
        while not self._stopped:
            try:
                await self._semaphore.acquire()
                msg = await self._channel.basic_get(queue_name)
                delivery_tag = msg['delivery_tag']
                message = msg['message'].decode('utf-8')
                task = self._loop.create_task(
                    self._handle_message(delivery_tag, message)
                )
                self._tasks[delivery_tag] = task
                task.add_done_callback(
                    functools.partial(self.on_task_done, delivery_tag)
                )
            except aioamqp.EmptyQueue:
                self._semaphore.release()
                await asyncio.sleep(timeout)

    async def on_close_channel(self):
        """Метод подписывается на событие закрытия канала
        (вызов метода close, обрыв соединения, etc).
        При возникновении данного события останавливается цикл
        подписки на новые сообщения и отменяются все запущенные задачи.
        """
        await self._channel.close_event.wait()
        logging.error('Channel {} was closed')
        if not self._stopped:
            self.stop()
        self._cancel_tasks()

    def on_task_done(self, task_tag, task):
        del self._tasks[task_tag]

    def _cancel_tasks(self):
        for task in self._tasks:
            if not task.cancelled():
                task.cancel()

    async def close(self):
        self.stop()
        self._on_closed_task.cancel()
        self._cancel_tasks()
        self._channel.close()


class IncomingMessageHandler:

    async def __call__(self, message):
        await self.backend.create_message(message)

    def __init__(self, backend):
        self.backend = backend


async def main():
    db_backend = BDBackend()
    await db_backend.connect(**DATABASE_SETTINGS)
    transport, protocol = await aioamqp.connect(**BROKER_SETTINGS)
    channel = await protocol.channel()
    handler = IncomingMessageHandler(db_backend)

    consumer = ChannelConsumer(channel, handler)
    await consumer.subscribe(**QUEUE_SETTINGS)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())