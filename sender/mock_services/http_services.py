import random
import uuid

from sanic import Sanic
from sanic.response import json


app = Sanic()


@app.route('/check_auth_key', methods=['GET'])
async def check_auth(request):
    is_valid = random.choice([True, False])
    if is_valid:
        return json(
            {'status': 'ok'},
            status=200
        )
    else:
        return json(
            {'status': 'fail'},
            status=401
        )


@app.route('/get_auth_key', methods=['POST'])
async def get_auth(request):
    return json(
        {'status': 'ok', 'auth_key': uuid.uuid4().hex},
        status=200
    )


@app.route('/send_message', methods=['POST'])
async def send_message(request):
    fail_statuses = (500, 404, 502)
    success_statuses = (200, 201)
    status = random.choice(fail_statuses + success_statuses)
    if status in fail_statuses:
        msg = {'status': 'fail'}
    else:
        msg = {'status': 'ok'}
    return json(msg, status=status)


if __name__ == '__main__':
    app.run(host='localhost', port=9090)