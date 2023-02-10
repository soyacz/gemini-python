import pytest

from gemini_python import GeminiConfiguration
from gemini_python.middleware.concurrency_limiter import (
    ConcurrencyLimiter,
    ConcurrencyLimiterMiddleware,
)


def test_concurrency_limiter_waits_for_value_below_limit():
    limiter = ConcurrencyLimiter(limit=2)
    limiter.acquire(timeout=0)
    limiter.acquire(timeout=0)
    limiter.release()
    limiter.acquire(timeout=0)

    with pytest.raises(TimeoutError):
        limiter.acquire(timeout=0)


def test_concurrency_limiter_middleware_is_working():
    config = GeminiConfiguration()
    limiter = ConcurrencyLimiterMiddleware(config=config)
    release_func, func_on_error = limiter.run()
    release_func([])
    limiter.teardown()
    assert release_func.__name__ == func_on_error.__name__
