from functools import wraps
from flask import request

def validate(schema):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            schema.validate(request.json)

            return f(*args, **kwargs)

        return decorated_function

    return decorator