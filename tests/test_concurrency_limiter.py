import pytest

from gemini_python.limiter import ConcurrencyLimiter


def test_concurrency_limiter_waits_for_value_below_limit():
    limiter = ConcurrencyLimiter(limit=2)
    limiter.increment(timeout=0)
    limiter.increment(timeout=0)
    limiter.decrement()
    limiter.increment(timeout=0)

    with pytest.raises(TimeoutError):
        limiter.increment(timeout=0)
