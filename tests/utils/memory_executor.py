from typing import Any, List

from gemini_python.executor import QueryExecutor
from gemini_python import CqlDto, OnSuccessClb, OnErrorClb


class MemoryExecutor(QueryExecutor):
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
