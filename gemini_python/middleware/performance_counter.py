import logging
import time
from typing import Any, Tuple

from gemini_python import GeminiConfiguration, OnSuccessClb, OnErrorClb
from gemini_python.middleware import Middleware

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PerformanceCounterMiddleware(Middleware):
    """Middleware used for counting number of executed queries per second."""

    def __init__(self, config: GeminiConfiguration) -> None:
        super().__init__(config=config)
        self._start_time: float = time.time()
        self._executed_queries: int = 0

    # pylint: disable=unused-argument
    def run(self, *args: Any, **kwargs: Any) -> Tuple[OnSuccessClb, OnErrorClb]:
        self._executed_queries += 1
        return self._on_success, self._on_error

    # pylint: disable=unused-argument
    def _on_success(self, *args: Any, **kwargs: Any) -> None:
        pass

    # pylint: disable=unused-argument
    def _on_error(self, *args: Any, **kwargs: Any) -> None:
        pass

    def teardown(self) -> None:
        queries_per_second = self._executed_queries / (time.time() - self._start_time)
        logger.info(
            "Executed %d queries in %d seconds. Queries per second: %d",
            self._executed_queries,
            time.time() - self._start_time,
            queries_per_second,
        )
