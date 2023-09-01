import logging
import random
import sqlite3
from pathlib import Path
from typing import List

from gemini_python import CqlDto
from gemini_python.schema import Schema

logger = logging.getLogger(__name__)


class HistoryStore:
    """HistoryStore is a simple sqlite3 database that stores the history of all writes (only pk and ck values)."""

    def __init__(
        self,
        index: int,
        schema: Schema,
        drop_schema: bool = False,
        history_file_dir: Path = Path.cwd() / ".gemini",
    ) -> None:
        self._schema = schema
        table = schema.tables[0]
        self._no_of_columns = (
            len(table.partition_keys + table.clustering_keys) + 1
        )  # +1 for d_time (deletion time)
        self._insert_query = (
            f"INSERT OR REPLACE INTO '{table.keyspace_name}.{table.name}' "
            f"(d_time, {', '.join([col.name for col in table.partition_keys + table.clustering_keys])}) "
            f"VALUES ({','.join('?' * self._no_of_columns)})"
        )
        self.conn = sqlite3.connect(f"{history_file_dir}/gemini_{index}.db")
        self.cursor = self.conn.cursor()
        if drop_schema:
            self.drop_schema()
        for cql_dto in schema.as_sql():
            self.cursor.execute(cql_dto.statement)
        self.cursor.execute("select max(id) from 'gemini.table0';")
        self.rows_count = self.cursor.fetchall()[0][0] or 0

    def drop_schema(self) -> None:
        for table in self._schema.tables:
            self.cursor.execute(f"drop table if exists '{table.keyspace_name}.{table.name}'")
        self.conn.commit()

    def insert(self, cql_dto: CqlDto) -> None:
        deletion_time = (None,)
        self.cursor.execute(
            self._insert_query, deletion_time + cql_dto.values[: self._no_of_columns - 1]
        )
        self.rows_count += 1

    def commit(self) -> None:
        self.conn.commit()

    def get_random_row(self) -> tuple:
        idx = random.randint(1, self.rows_count)
        self.cursor.execute(f"SELECT * FROM 'gemini.table0' where id={idx}")
        row: List[tuple] = self.cursor.fetchall()
        return row[0][2:]  # drop id and d_time column
