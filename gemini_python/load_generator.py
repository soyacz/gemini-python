from itertools import cycle

from gemini_python import CqlDto, QueryMode
from gemini_python.query import (
    InsertQueryGenerator,
    SelectQueryGenerator,
    QueryGenerator,
)
from gemini_python.schema import Keyspace


class LoadGenerator:
    """Query generator selector according to schema and mode."""

    def __init__(
        self, schema: Keyspace, partitions: list[list[tuple]], mode: QueryMode = QueryMode.WRITE
    ):
        generators: list[QueryGenerator] = []
        assert len(schema.tables) == len(
            partitions
        ), "partitions were not generated for all tables. Should not happen."
        for table, partition_list in zip(schema.tables, partitions):
            match mode:
                case QueryMode.WRITE:
                    generators.append(InsertQueryGenerator(table=table, partitions=partition_list))
                case QueryMode.READ:
                    generators.append(SelectQueryGenerator(table=table, partitions=partition_list))
                case QueryMode.MIXED:
                    generators.append(InsertQueryGenerator(table=table, partitions=partition_list))
                    generators.append(SelectQueryGenerator(table=table, partitions=partition_list))
                case _:
                    raise ValueError(f"Unsupported query mode: {mode}")
        self._query_generator = cycle(generators)

    def get_query(self) -> CqlDto:
        return next(next(self._query_generator))
