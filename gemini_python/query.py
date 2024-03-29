from abc import ABC
from itertools import cycle
from typing import Tuple

from gemini_python import CqlDto, Operation
from gemini_python.history_store import HistoryStore
from gemini_python.schema import Table


class QueryGenerator(ABC):
    """Base class for CQL queries generators."""

    def __init__(self, table: Table) -> None:
        self._table = table

    def __iter__(self) -> "QueryGenerator":
        return self

    def __next__(self) -> Tuple[Operation, CqlDto]:
        pass


class InsertQueryGenerator(QueryGenerator):
    """Basic insert query with all table columns."""

    def __init__(self, table: Table, partitions: list[tuple]) -> None:
        super().__init__(table)
        self._partitions = cycle(partitions)
        self._stmt = (
            f"insert into {table.keyspace_name}.{table.name} "
            f"({', '.join([col.name for col in self._table.all_columns])}) "
            f"VALUES ({','.join('?'*len(self._table.all_columns))})"
        )

    def __iter__(self) -> "QueryGenerator":
        return self

    def __next__(self) -> Tuple[Operation, CqlDto]:
        return Operation.WRITE, CqlDto(
            self._stmt,
            next(self._partitions)
            + tuple(
                column.generate_random_value()
                for column in self._table.clustering_keys + self._table.columns
            ),
        )


class SelectQueryGenerator(QueryGenerator):
    """Basic select query with all table columns."""

    def __init__(self, table: Table, partitions: list[tuple], history_store: HistoryStore) -> None:
        super().__init__(table)
        # todo: we may want to use random here instead of cycling
        self.history_store = history_store
        self._partitions = cycle(partitions)
        self._stmt = (
            f"select {', '.join(col.name for col in self._table.all_columns)}"
            f" from {table.keyspace_name}.{table.name} "
            f"where {' AND '.join([col.name + '=?' for col in self._table.partition_keys + self._table.clustering_keys])}"
        )

    def __iter__(self) -> "QueryGenerator":
        return self

    def __next__(self) -> Tuple[Operation, CqlDto]:
        return Operation.READ, CqlDto(
            self._stmt,
            self.history_store.get_random_row(),
        )
