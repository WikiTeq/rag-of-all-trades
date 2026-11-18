from functools import wraps

from fastapi import Request

from utils.config import settings


def limit(rate: str):
    def decorator(func):
        if not settings.env.ENABLE_RATE_LIMIT:
            return func

        limited_func = None

        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal limited_func
            request: Request = kwargs.get("request") or args[0]
            if limited_func is None:
                limited_func = request.app.state.limiter.limit(rate)(func)
            return await limited_func(*args, **kwargs)

        return wrapper

    return decorator
