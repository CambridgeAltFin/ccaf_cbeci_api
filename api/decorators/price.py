
from functools import wraps
from flask import request


def get_price():
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            price = kwargs.pop('value', None) or request.args.get('p')
            if price is None:
                return (
                    "Welcome to the CBECI API data endpoint. "
                    "To get bitcoin electricity consumption estimate timeseries, "
                    "specify electricity price parameter 'p' (in USD), for example {endpoint}?p=0.05"
                ).format(endpoint=request.path)
            return f(value=price, *args, **kwargs)
        return decorated_function
    return decorator

