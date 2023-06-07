import sqlite3
from typing import List

from gemini_python import CqlDto
from gemini_python.schema import Keyspace


class HistoryStore:
    """HistoryStore is a simple sqlite3 database that stores the history of all writes (only pk and ck values)."""

    def __init__(self, index: int, schema: Keyspace, drop_schema: bool = False) -> None:
        self._schema = schema
        table = schema.tables[0]
        self._bindings = len(table.partition_keys + table.clustering_keys) + 1
        self._insert_query = (
            f"INSERT OR REPLACE INTO '{table.keyspace_name}.{table.name}' "
            f"(d_time, {', '.join([col.name for col in table.partition_keys + table.clustering_keys])}) "
            f"VALUES ({','.join('?'*self._bindings)})"
        )
        self.conn = sqlite3.connect(f"gemini_{index}.db")
        self.cursor = self.conn.cursor()
        if drop_schema:
            self.drop_schema()
        for cql_dto in schema.as_sql():
            self.cursor.execute(cql_dto.statement)

    def drop_schema(self) -> None:
        for table in self._schema.tables:
            self.cursor.execute(f"drop table if exists '{table.keyspace_name}.{table.name}'")
        self.conn.commit()

    def insert(self, cql_dto: CqlDto) -> None:
        deletion_time = (None,)
        self.cursor.execute(
            self._insert_query, deletion_time + cql_dto.values[: self._bindings - 1]
        )

    def commit(self) -> None:
        self.conn.commit()

    def get_random_row(self) -> tuple:
        self.cursor.execute("SELECT * FROM 'gemini.table0' ORDER BY RANDOM() LIMIT 1;")
        row: List[tuple] = self.cursor.fetchall()
        return row[0][1:]  # drop d_time column
