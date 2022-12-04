from dataclasses import dataclass, field
from typing import List

from gemini_python.column_types import Column, AsciiColumn, BigIntColumn
from gemini_python.executor import QueryExecutor
from gemini_python import CqlDto


@dataclass
class Table:
    """Represents Scylla table"""

    name: str
    keyspace_name: str
    primary_keys: List[Column]
    clustering_keys: List[Column] = field(default_factory=list)
    columns: List[Column] = field(default_factory=list)

    @property
    def all_columns(self) -> List[Column]:
        return self.primary_keys + self.clustering_keys + self.columns

    def as_query(self) -> CqlDto:
        primary_key = (
            f"({','.join([column.name for column in self.primary_keys])})" + ")"
            if not self.clustering_keys
            else f", {','.join([column.name for column in self.clustering_keys])}"
        )
        return CqlDto(
            f"CREATE TABLE IF NOT EXISTS {self.keyspace_name}.{self.name} "
            f"({', '.join([str(col) for col in self.all_columns])},"
            f" PRIMARY KEY ({primary_key});"
        )


@dataclass
class Keyspace:
    """Represents Scylla keyspace with tables"""

    name: str
    replication_strategy: str
    tables: List[Table]

    def as_queries(self) -> List[CqlDto]:
        queries = [
            CqlDto(
                f"CREATE KEYSPACE IF NOT EXISTS {self.name} "
                f"with replication = {self.replication_strategy};"
            )
        ]
        for table in self.tables:
            queries.append(table.as_query())
        return queries

    def create(self, query_executor: QueryExecutor) -> None:
        """Creates keyspace with tables in database."""
        for statement in self.as_queries():
            query_executor.execute(statement)

    def drop(self, query_executor: QueryExecutor) -> None:
        """Drops whole keyspace"""
        query_executor.execute(CqlDto(f"drop keyspace if exists {self.name}"))


def generate_schema() -> Keyspace:
    """Generates schema: Keyspace with tables."""
    ks_name = "gemini"
    return Keyspace(
        name=ks_name,
        replication_strategy="{'class': 'SimpleStrategy', 'replication_factor' : 3}",
        tables=[
            Table(
                name="table1",
                keyspace_name=ks_name,
                primary_keys=[BigIntColumn(name="pk")],
                columns=[AsciiColumn(name="col1")],
            )
        ],
    )
