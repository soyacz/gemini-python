import threading
from typing import Iterable, Tuple, Any

from gemini_python import OnSuccessClb, OnErrorClb, GeminiConfiguration
from gemini_python.middleware import Middleware


class ConcurrencyLimiter:
    """Limits number of concurrent async calls"""

    def __init__(self, limit: int = 10) -> None:
        self._semaphore: threading.Semaphore = threading.Semaphore(limit)

    def acquire(self, timeout: int = 5) -> None:
        """Acquires one semaphore if available and continues execution.
        Otherwise, waits for semaphore to be released."""
        # pylint: disable=consider-using-with
        not_blocked = self._semaphore.acquire(blocking=True, timeout=timeout)
        if not not_blocked:
            raise TimeoutError("Timeout during waiting for async executor.")

    def release(self, _: Iterable | Exception | None = None) -> None:
        """Releases semaphore for further acquisition."""
        self._semaphore.release()


class ConcurrencyLimiterMiddleware(Middleware):
    """Middleware used for limiting concurrency of async callbacks."""

    def __init__(self, config: GeminiConfiguration) -> None:
        super().__init__(config=config)
        self._limiter = ConcurrencyLimiter(limit=200)
        self._timeout: int = 5

    # pylint: disable=unused-argument
    def run(self, *args: Any, **kwargs: Any) -> Tuple[OnSuccessClb, OnErrorClb]:
        self._limiter.acquire(timeout=self._timeout)
        return self._limiter.release, self._limiter.release
