import json
import os
import sys
from unittest import mock

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

import pytest
import asynctest

from sender.common.backend import (
    Task, TASK_FATAL
)


def init_task():
    task_id = 1
    backend = mock.MagicMock()
    return Task(
        task_id, json.dumps({}), broker_backend=backend
    )


async def handle_with_success(msg):
    return msg


async def handle_with_error(msg):
    raise Exception


@pytest.mark.asyncio
async def test_task_handle_success():
    """Проверка на вызов метода on_success после успешного
    завершения задачи
    """
    task = init_task()
    task.add_handler(handle_with_success)
    task.on_success = asynctest.CoroutineMock()
    await task.run()
    task.on_success.called_once()


@pytest.mark.asyncio
async def test_task_handle_fatal():
    task = init_task()
    task.add_handler(handle_with_error)
    task.on_fatal = asynctest.CoroutineMock()
    await task.run()
    task.on_fatal.called_once()
    
    
@pytest.mark.asyncio
async def test_task_on_fatal_state():
    task = init_task()
    task.add_handler(handle_with_error)
    task.update_state = asynctest.CoroutineMock()
    await task.run()
    task.update_state.called_once_with(task.id, TASK_FATAL)