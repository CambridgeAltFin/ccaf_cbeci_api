from functools import wraps
from flask import Response

from os import environ as env


def cache_control():
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            response = f(*args, **kwargs)  # type: Response
            if type(response) != str:
                response.headers.add('Cache-Control', 'public,max-age=%d' % int(env.get('CACHE_CONTROL', 300)))
            return response
        return decorated_function
    return decorator
