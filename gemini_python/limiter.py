import threading
from abc import ABC
from typing import Iterable


class Limiter(ABC):
    """Limiter class serves for limiting actions by waiting
    for limited value to go below specified limit"""

    def increment(self) -> None:
        """Increments value. In case limit was reached,
        waits until value drops below limit."""

    def decrement(self, _: Iterable | Exception | None = None) -> None:
        """Decreases value.
        This lets increment method to proceed in case of waiting for value drop."""


class ConcurrencyLimiter(Limiter):
    """Limits number of concurrent async calls"""

    def __init__(self, limit: int = 10) -> None:
        self._semaphore: threading.Semaphore = threading.Semaphore(limit)

    def increment(self, timeout: int = 5) -> None:
        """Increments running async executions counter.

        If limit is reached, waits for decrement."""
        # pylint: disable=consider-using-with
        not_blocked = self._semaphore.acquire(blocking=True, timeout=timeout)
        if not not_blocked:
            raise TimeoutError("Timeout during waiting for async executor.")

    def decrement(self, _: Iterable | Exception | None = None) -> None:
        """Decrements running async executions counter."""
        self._semaphore.release()
