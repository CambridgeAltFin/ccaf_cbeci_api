from functools import wraps
from flask import request, jsonify


def validate(schema):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                schema.validate(request.json)
            except Exception as error:
                if len(error.autos) > 1:
                    message = str(error.autos[-1])
                    if message.startswith('Missing key:'):
                        return jsonify(error=message)
                raise error

            return f(*args, **kwargs)

        return decorated_function

    return decorator