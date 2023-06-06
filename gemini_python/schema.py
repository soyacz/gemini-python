from dataclasses import dataclass, field
import random
from typing import List, Type, Optional

from gemini_python.column_types import Column, ALL_COLUMN_TYPES
from gemini_python.query_driver import QueryDriver
from gemini_python import CqlDto, GeminiConfiguration
from gemini_python.replication_strategy import ReplicationStrategy


@dataclass
class Table:
    """Represents Scylla table"""

    name: str
    keyspace_name: str
    partition_keys: List[Column]
    clustering_keys: List[Column] = field(default_factory=list)
    columns: List[Column] = field(default_factory=list)

    @property
    def all_columns(self) -> List[Column]:
        return self.partition_keys + self.clustering_keys + self.columns

    def as_query(self) -> CqlDto:
        partition_key = f"{', '.join([column.name for column in self.partition_keys])}"
        partition_key = f"({partition_key})" if len(self.partition_keys) > 1 else partition_key
        clustering_key = (
            f", {', '.join([column.name for column in self.clustering_keys])}"
            if self.clustering_keys
            else ""
        )
        return CqlDto(
            f"CREATE TABLE IF NOT EXISTS {self.keyspace_name}.{self.name} "
            f"({', '.join([str(col) for col in self.all_columns])},"
            f" PRIMARY KEY ({partition_key}{clustering_key}));"
        )

    def as_sql(self) -> CqlDto:
        return CqlDto(
            f"CREATE TABLE IF NOT EXISTS '{self.keyspace_name}.{self.name}' ("
            f"d_time INTEGER , "
            f"{', '.join([column.name + ' ' + column.sql_type for column in self.partition_keys])}, "
            f"{', '.join([column.name + ' ' + column.sql_type for column in self.clustering_keys])},"
            f" PRIMARY KEY ({', '.join([column.name for column in self.partition_keys + self.clustering_keys])}));"
        )


@dataclass
class Keyspace:
    """Represents Scylla keyspace with tables"""

    name: str
    tables: List[Table]

    def as_queries(self, replication_strategy: ReplicationStrategy) -> List[CqlDto]:
        queries = [
            CqlDto(
                f"CREATE KEYSPACE IF NOT EXISTS {self.name} "
                f"with replication = {replication_strategy};"
            )
        ]
        for table in self.tables:
            queries.append(table.as_query())
        return queries

    def as_sql(self) -> List[CqlDto]:
        queries = []
        for table in self.tables:
            queries.append(table.as_sql())
        return queries

    def create(self, query_driver: QueryDriver, replication_strategy: ReplicationStrategy) -> None:
        """Creates keyspace with tables in database."""
        for statement in self.as_queries(replication_strategy):
            query_driver.execute(statement)

    def drop(self, query_driver: QueryDriver) -> None:
        """Drops whole keyspace"""
        query_driver.execute(CqlDto(f"drop keyspace if exists {self.name}"))


def _generate_random_column(
    rand: random.Random,
    seed: int,
    prefix: str,
    index: int,
    column_types: List[Type[Column]],
    max_size: Optional[int] = None,
) -> Column:
    params = {"name": f"{prefix}{index}", "seed": seed}
    if max_size:
        params["max_size"] = max_size
    return rand.choice(column_types)(**params)  # type: ignore


def generate_schema(  # pylint: disable=dangerous-default-value
    config: GeminiConfiguration,
    pk_types: List[Type[Column]] = ALL_COLUMN_TYPES,
    ck_types: List[Type[Column]] = ALL_COLUMN_TYPES,
    c_types: List[Type[Column]] = ALL_COLUMN_TYPES,
) -> Keyspace:
    """Generates schema: Keyspace with tables according to configuration."""
    ks_name = "gemini"
    rand = random.Random(config.seed)
    tables = []
    for idx in range(config.max_tables):
        num_partition_keys = rand.randint(config.min_partition_keys, config.max_partition_keys)
        num_clustering_keys = rand.randint(config.min_clustering_keys, config.max_clustering_keys)
        num_columns = rand.randint(config.min_columns, config.max_columns)
        partition_keys = [
            _generate_random_column(rand, config.seed, "pk", idx, pk_types)
            for idx in range(num_partition_keys)
        ]
        clustering_keys = [
            _generate_random_column(rand, config.seed, "ck", idx, ck_types)
            for idx in range(num_clustering_keys)
        ]
        columns = [
            _generate_random_column(rand, config.seed, "col", idx, c_types)
            for idx in range(num_columns)
        ]
        tables.append(
            Table(
                name=f"table{idx}",
                keyspace_name=ks_name,
                partition_keys=partition_keys,
                clustering_keys=clustering_keys,
                columns=columns,
            )
        )
    return Keyspace(name=ks_name, tables=tables)
