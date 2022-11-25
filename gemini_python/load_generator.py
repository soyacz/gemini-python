from gemini_python.query import InsertQueryGenerator, SelectQueryGenerator
from gemini_python.schema import Keyspace

# todo replace mode str with enum


class LoadGenerator:
    """Query generator selector according to schema and mode."""

    def __init__(self, schema: Keyspace, mode: str = "write"):
        match mode:
            case "write":
                self._query_generator = InsertQueryGenerator(table=schema.tables[0])
            case "read":
                self._query_generator = SelectQueryGenerator(table=schema.tables[0])  # type: ignore
            case _:
                raise ValueError("Not supported query operation mode")

    def get_query(self) -> tuple[str, tuple]:
        return next(self._query_generator)
