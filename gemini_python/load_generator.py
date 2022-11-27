from enum import Enum, unique

from gemini_python import CqlDto
from gemini_python.query import InsertQueryGenerator, SelectQueryGenerator
from gemini_python.schema import Keyspace


@unique
class QueryMode(Enum):
    """Query operation mode available options"""

    WRITE = "write"
    READ = "read"


class LoadGenerator:
    """Query generator selector according to schema and mode."""

    def __init__(self, schema: Keyspace, mode: QueryMode = QueryMode.WRITE):
        match mode:
            case QueryMode.WRITE:
                self._query_generator = InsertQueryGenerator(table=schema.tables[0])
            case QueryMode.READ:
                self._query_generator = SelectQueryGenerator(table=schema.tables[0])  # type: ignore
            case _:
                raise ValueError("Not supported query operation mode")

    def get_query(self) -> CqlDto:
        return next(self._query_generator)
