from itertools import cycle
from typing import Tuple

from gemini_python import CqlDto, QueryMode, Operation
from gemini_python.history_store import HistoryStore
from gemini_python.query import (
    InsertQueryGenerator,
    SelectQueryGenerator,
    QueryGenerator,
)
from gemini_python.schema import Schema


class LoadGenerator:
    """Query generator selector according to schema and mode."""

    def __init__(
        self,
        schema: Schema,
        partitions: list[list[tuple]],
        history_store: HistoryStore,
        mode: QueryMode = QueryMode.WRITE,
    ):

        generators: list[QueryGenerator] = []
        assert len(schema.tables) == len(
            partitions
        ), "partitions were not generated for all tables. Should not happen."
        for table, partition_list in zip(schema.tables, partitions):
            if mode == QueryMode.WRITE:
                generators.append(InsertQueryGenerator(table=table, partitions=partition_list))
            elif mode == QueryMode.READ:
                generators.append(
                    SelectQueryGenerator(
                        table=table, partitions=partition_list, history_store=history_store
                    )
                )
            elif mode == QueryMode.MIXED:
                generators.append(InsertQueryGenerator(table=table, partitions=partition_list))
                generators.append(
                    SelectQueryGenerator(
                        table=table, partitions=partition_list, history_store=history_store
                    )
                )
            else:
                raise ValueError(f"Unsupported query mode: {mode}")
        self._query_generator = cycle(generators)

    def get_query(self) -> Tuple[Operation, CqlDto]:
        return next(next(self._query_generator))
