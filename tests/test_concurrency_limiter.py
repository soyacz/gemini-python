import pytest

from gemini_python.middleware.concurrency_limiter import ConcurrencyLimiter


def test_concurrency_limiter_waits_for_value_below_limit():
    limiter = ConcurrencyLimiter(limit=2)
    limiter.acquire(timeout=0)
    limiter.acquire(timeout=0)
    limiter.release()
    limiter.acquire(timeout=0)

    with pytest.raises(TimeoutError):
        limiter.acquire(timeout=0)
