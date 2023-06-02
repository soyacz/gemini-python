import threading
import time
from collections import deque
from typing import Tuple, Deque, Optional

from gemini_python import CqlDto, Operation


class RetriesGenerator:
    """Class to generate retries for failed mutations after defined backoff time."""

    def __init__(self, max_mutation_retries_backoff: float) -> None:
        self._dto_in_list: Deque[Tuple[Operation, CqlDto, int, float]] = deque()
        self._dto_out_list: Deque[Tuple[Operation, CqlDto, int]] = deque()
        self._max_mutation_retries_backoff = max_mutation_retries_backoff
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None

    def add_retry(self, operation: Operation, cql_dto: CqlDto, attempt: int) -> None:
        """Add cql_dto to retry list, so it's later returned after backoff time"""
        self._dto_in_list.append((operation, cql_dto, attempt, time.time()))
        if self._timer is None:
            self._start_timer()

    def retry_available(self) -> bool:
        """Return True if there is a retry available, False otherwise"""
        return bool(self._dto_out_list)

    def get_retry(self) -> Tuple[Operation, CqlDto, int]:
        """Return a tuple with the operation, cql_dto and attempt of the next retry"""
        return self._dto_out_list.popleft()

    def _append_to_dto_out_list(self, operation: Operation, cql_dto: CqlDto, attempt: int) -> None:
        self._dto_out_list.append((operation, cql_dto, attempt))
        self._timer = None
        if self._dto_in_list:
            self._start_timer()

    def _start_timer(self) -> None:
        with self._lock:
            if self._timer is not None:
                return
            operation, cql_dto, attempt, add_time = self._dto_in_list.popleft()
            timeout = max(0.0, add_time - time.time() + self._max_mutation_retries_backoff)
            timer = threading.Timer(
                timeout, self._append_to_dto_out_list, args=[operation, cql_dto, attempt]
            )
            timer.start()
            self._timer = timer
