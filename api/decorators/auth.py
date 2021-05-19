from functools import wraps
from flask import request
from extensions import cache
import psycopg2
from config import config

@cache.cached(key_prefix='all_api_tokens')
def get_api_tokens():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM api_tokens')
        return cursor.fetchall()

class AuthenticationError(Exception):
    pass

def bearer(header='Authorization'):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            def get_api_token(auth_token):
                for row in get_api_tokens():
                    if row[2] == auth_token:
                        return row
                return None

            auth_header = request.headers.get(header)

            auth_token = ''
            if auth_header:
                auth_token = auth_header.split(" ")[1]

            api_token = get_api_token(auth_token)
            if api_token is None:
                raise AuthenticationError('Invalid bearer token')

            return f(api_token=api_token, *args, **kwargs)

        return decorated_function

    return decorator