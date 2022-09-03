import queue
from abc import ABC
from typing import Iterable


class Limiter(ABC):
    """Limiter class serves for limiting actions by waiting
    for limited value to go below specified limit"""

    def increment(self) -> None:
        """Increments value. In case limit was reached,
        waits until value drops below limit."""

    def decrement(self, _: Iterable | None = None) -> None:
        """Decreases value.
        This lets increment method to proceed in case of waiting for value drop."""

    # noinspection PyPropertyDefinition
    @property
    def value(self) -> int:
        """Shows current value."""


class ConcurrencyLimiter(Limiter):
    """Limits number of concurrent async calls"""

    def __init__(self, limit: int = 10) -> None:
        self._queue: queue.Queue[str] = queue.Queue(maxsize=limit)

    def increment(self, timeout: int = 5) -> None:
        """Increments running async executions counter.

        If limit is reached, waits for decrement."""
        try:
            self._queue.put("", timeout=timeout)
        except queue.Full as exc:
            raise TimeoutError("Timeout during waiting for async executor.") from exc

    def decrement(self, _: Iterable | None = None) -> None:
        """Decrements running async executions counter."""
        self._queue.get(block=False)

    @property
    def value(self) -> int:
        return self._queue.qsize()
