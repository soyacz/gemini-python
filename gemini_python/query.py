from abc import ABC

from gemini_python.schema import Table


class QueryGenerator(ABC):
    """Base class for CQL queries generators."""

    def __init__(self, table: Table) -> None:
        self._table = table

    def __iter__(self) -> "QueryGenerator":
        return self

    def __next__(self) -> tuple[str, tuple]:
        pass


class InsertQueryGenerator(QueryGenerator):
    """Basic insert query with all table columns."""

    def __init__(self, table: Table) -> None:
        super().__init__(table)
        self._stmt = (
            f"insert into {table.keyspace_name}.{table.name} "
            f"({', '.join([col.name for col in self._table.all_columns])}) "
            f"VALUES ({','.join('?'*len(self._table.all_columns))})"
        )

    def __iter__(self) -> "QueryGenerator":
        return self

    def __next__(self) -> tuple[str, tuple]:
        return self._stmt, tuple(
            column.generate_sequence_value() for column in self._table.all_columns
        )


class SelectQueryGenerator(QueryGenerator):
    """Basic select query with all table columns."""

    def __init__(self, table: Table) -> None:
        super().__init__(table)
        self._stmt = (
            f"select {', '.join(col.name for col in self._table.all_columns)}"
            f" from {table.keyspace_name}.{table.name} "
            f"where {' AND '.join([col.name + '=?' for col in self._table.primary_keys])}"
        )

    def __iter__(self) -> "QueryGenerator":
        return self

    def __next__(self) -> tuple[str, tuple]:
        return self._stmt, tuple(
            column.generate_sequence_value() for column in self._table.primary_keys
        )
