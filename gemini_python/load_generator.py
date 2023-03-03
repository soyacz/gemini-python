from gemini_python import CqlDto, QueryMode
from gemini_python.query import (
    InsertQueryGenerator,
    SelectQueryGenerator,
    MixedQueryGenerator,
    QueryGenerator,
)
from gemini_python.schema import Keyspace


class LoadGenerator:
    """Query generator selector according to schema and mode."""

    def __init__(
        self, schema: Keyspace, partitions: list[list[tuple]], mode: QueryMode = QueryMode.WRITE
    ):
        self._query_generator: QueryGenerator
        match mode:
            case QueryMode.WRITE:
                self._query_generator = InsertQueryGenerator(
                    table=schema.tables[0], partitions=partitions[0]
                )
            case QueryMode.READ:
                self._query_generator = SelectQueryGenerator(
                    table=schema.tables[0], partitions=partitions[0]
                )
            case QueryMode.MIXED:
                self._query_generator = MixedQueryGenerator(
                    table=schema.tables[0], partitions=partitions[0]
                )
            case _:
                raise ValueError(f"Unsupported query mode: {mode}")

    def get_query(self) -> CqlDto:
        return next(self._query_generator)
