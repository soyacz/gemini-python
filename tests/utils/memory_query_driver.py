from typing import Any, List, Iterable

from gemini_python.query_driver import QueryDriver
from gemini_python import CqlDto, OnSuccessClb, OnErrorClb


class MemoryQueryDriver(QueryDriver):
    """In memory database for unit tests purposes"""

    def __init__(self) -> None:
        super().__init__()
        self._data: List[Any] = []

    def execute_async(
        self,
        cql_dto: CqlDto,
        on_success: List[OnSuccessClb],
        on_error: List[OnErrorClb],
    ) -> None:
        if cql_dto.statement == "insert":
            self._data.append(cql_dto.values)
            result = None
        else:
            result = self._data
        for callback in on_success:
            callback(result)

    def execute(self, cql_dto: CqlDto) -> Iterable | None:
        if cql_dto.statement == "insert":
            self._data.append(cql_dto.values)
            return None
        return self._data
