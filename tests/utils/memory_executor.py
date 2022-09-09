from typing import Any, Callable, Iterable

from gemini_python.executor import QueryExecutor


class MemoryExecutor(QueryExecutor):
    def __init__(self) -> None:
        super().__init__()
        self._data: list[Any] = []

    def execute_async(
        self,
        statement: str,
        query_values: tuple,
        on_success: list[Callable[[Iterable | None], None]],
        on_error: list[Callable[[Any], None]],
    ) -> None:
        if statement == "insert":
            self._data.append(query_values)
            result = None
        else:
            result = self._data
        for callback in on_success:
            callback(result)
