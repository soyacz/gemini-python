from typing import Callable, Iterable

from gemini_python.executor import QueryExecutor
from gemini_python import CqlDto


class RecordingExecutor(QueryExecutor):
    """Records all CQL's executed for unit tests purposes.

    All queries return None."""

    def __init__(self) -> None:
        super().__init__()
        self.executed_queries: list[CqlDto] = []

    def execute_async(
        self,
        cql_dto: CqlDto,
        on_success: list[Callable[[Iterable | None], None]],
        on_error: list[Callable[[Exception], None]],
    ) -> None:
        self.executed_queries.append(cql_dto)
        for callback in on_success:
            callback(None)

    def execute(self, cql_dto: CqlDto) -> None:
        self.executed_queries.append(cql_dto)
