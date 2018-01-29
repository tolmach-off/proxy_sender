import asyncio
import base64
import logging
import json

import aiohttp

from sender.settings import (
    DATABASE_SETTINGS, CHECK_AUTH_KEY_URL, GET_AUTH_KEY_URL, SEND_MESSAGE_URL
)
from sender.common.backend import BDBackend, Task
from sender.common.exceptions import (
    AuthenticationError, DownloadError, SendMessageError
)


async def check_auth_key_valid(auth_key):
    """
    Метод проверяет валидность ключа авторизации.
    :param str auth_key: ключ авторизации
    :return: True - если ключ валиден, False - если не валиден
    """
    params = {'auth_key': auth_key}
    try:
        async with aiohttp.ClientSession() as client:
            # Обращаемся к стороннему сервису проверки авторизации
            async with client.get(CHECK_AUTH_KEY_URL, params=params) as r:
                if r.status == 200:
                    return True
                else:
                    return False
    except aiohttp.ClientConnectionError:
        logging.error('Could not check auth key.')
        raise AuthenticationError


async def get_auth_key(user_id):
    """Метод возвращает ключ авторизации, сгенерированный для пользователя
    на основе его user_id. В случае ошибки - поднимает исключение
    AuthenticationError.
    """
    data = {'user_id': user_id}
    try:
        async with aiohttp.ClientSession() as client:
            async with client.post(GET_AUTH_KEY_URL, data=data) as r:
                if r.status == 200:
                    response = await r.json()
                    return response['auth_key']
                else:
                    raise AuthenticationError(
                        'Bad response: {status} from auth server.'
                    )
    except aiohttp.ClientConnectionError:
        logging.error('Could not get auth key.')
        raise AuthenticationError


async def refresh_auth_key(data):
    """Метод проверяет валидность ключа авторизации и генерирует новый,
    используя user_id, если текущий не валиден или отсутствует.

    :param dict data: словарь, содержащий ключи:
    - auth_key - ключ авторизации для проверки/обновления. Может отсутствовать.
    - user_id - id пользователя для генерации нового ключа. Обязателен.
    :return: data, с валидным auth_key.
    """
    auth_key = data.get('auth_key')
    if auth_key is None or not await check_auth_key_valid(auth_key):
        data['auth_key'] = await get_auth_key(data['user_id'])
    return data


async def load_file(file_name, url):
    try:
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as r:
                if r.status == 200:
                    return file_name, await r.read()
                else:
                    raise DownloadError(
                        'Could not download file from url: {url}'.format(
                            url=url
                        )
                    )
    except aiohttp.ClientConnectionError:
        logging.error('Could not download file: {name}.'.format(
            name=file_name)
        )
        raise DownloadError


async def load_attachments(data):
    attachments = data.get('attachments')
    data['attachments'] = {}
    if attachments:
        download_futures = [
            load_file(attachment['name'], attachment['url'])
            for attachment in attachments
        ]
        for result in asyncio.as_completed(download_futures):
            name, body = await result
            data['attachments'][name] = base64.b64encode(body).decode('utf-8')
        return data


async def send_message(data):
    async with aiohttp.ClientSession() as client:
        data = json.dumps(data)
        try:
            async with client.post(SEND_MESSAGE_URL, data=data) as r:
                response = await r.json()
                if response['status'] != 'ok':
                    raise SendMessageError(
                        (
                            'Send mail server returned '
                            'wrong status: {status}'
                        ).format(
                            status=response['status']
                        )
                    )
        except (
            KeyError, aiohttp.ClientConnectionError, aiohttp.ContentTypeError,
        ):
            logging.error('Could not send mail.', exc_info=True)
            raise SendMessageError


async def main():
    handlers = (refresh_auth_key, load_attachments, send_message)
    while True:
        db_backend = BDBackend()
        await  db_backend.connect(**DATABASE_SETTINGS)
        tasks = [
            Task(
                message[0], message[1], broker_backend=db_backend,
                handlers=handlers
            ).run()
            for message in await db_backend.get_messages()
        ]
        if tasks:
            await asyncio.wait(tasks)
        else:
            await asyncio.sleep(5)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())


