from typing import Callable, Iterable

from gemini_python.executor import QueryExecutor


class RecordingExecutor(QueryExecutor):
    """Records all CQL's executed for unit tests purposes.

    All queries return None."""

    def __init__(self) -> None:
        super().__init__()
        self.executed_cqls: list[tuple[str, tuple]] = []

    def execute_async(
        self,
        statement: str,
        query_values: tuple,
        on_success: list[Callable[[Iterable | None], None]],
        on_error: list[Callable[[Exception], None]],
    ) -> None:
        self.executed_cqls.append(
            (statement, query_values),
        )
        for callback in on_success:
            callback(None)

    def execute(self, statement: str, query_values: tuple = ()) -> None:
        self.executed_cqls.append(
            (statement, query_values),
        )
