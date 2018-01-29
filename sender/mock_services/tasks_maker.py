import asyncio
import json

import aioamqp

from sender.settings import BROKER_SETTINGS, QUEUE_SETTINGS


async def publish_message(channel):
    await channel.basic_publish(
        json.dumps(
            {
                'user_id': 1,
                'message': {},
                'attachments': [
                    {
                        'name': 'python1.png',
                        'url': 'https://www.python.org/static/opengraph-icon-200x200.png'
                    },
                    {
                        'name': 'python2.png',
                        'url': 'https://www.python.org/static/opengraph-icon-200x200.png'
                    }
                ]
            }
        ), '', QUEUE_SETTINGS['queue_name']
    )


async def main():
    transport, protocol = await aioamqp.connect(**BROKER_SETTINGS)
    channel = await protocol.channel()
    await channel.queue_declare(**QUEUE_SETTINGS)
    await asyncio.wait([publish_message(channel) for _ in range(1000)])


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())