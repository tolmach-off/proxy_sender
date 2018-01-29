

DATABASE_SETTINGS = {
    'user': 'sender',
    'password': 'sender',
    'host': '127.0.0.1',
    'port': '5532',
    'database': 'test_db'
}


CHECK_AUTH_KEY_URL = 'http://127.0.0.1:9090/check_auth_key'
GET_AUTH_KEY_URL = 'http://127.0.0.1:9090/get_auth_key'
SEND_MESSAGE_URL = 'http://127.0.0.1:9090/send_message'


BROKER_SETTINGS = {
    'user': 'guest',
    'password': 'guest',
    'host': '127.0.0.1',
    'port': 6672,
    'virtual_host': '/'
}
QUEUE_SETTINGS = {
    'queue_name': 'messages'
}