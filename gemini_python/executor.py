#  pylint: disable=no-name-in-module
import logging
from abc import ABC
from functools import lru_cache
from typing import Callable, Iterable

from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy  # type: ignore
from cassandra.query import PreparedStatement  # type: ignore

logger = logging.getLogger(__name__)


class QueryExecutor(ABC):
    """Responsible for communication with DB and running queries."""

    def execute_async(
        self,
        statement: str,
        query_values: tuple,
        on_success: list[Callable[[Iterable | None], None]],
        on_error: list[Callable[[Exception], None]],
    ) -> None:
        """Execute statement asynchronously."""

    def execute(self, statement: str, query_values: tuple = ()) -> Iterable | None:
        """Executes statement synchronously."""

    def prepare(self, statement: str) -> None:
        """Preparation before running statement."""


class CqlQueryExecutor(QueryExecutor):
    """Communicates with and queries Scylla/Cassandra databases."""

    def __init__(self, hosts: list[str], port: int = 9042) -> None:
        self.cluster = Cluster(
            hosts,
            port=port,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            protocol_version=4,
        )
        self.session = self.cluster.connect()

    def prepare(self, statement: str) -> None:
        self._prepare_statement(statement)

    @lru_cache(maxsize=1000)
    def _prepare_statement(self, statement: str) -> PreparedStatement:
        """Prepare statement before running it. Cached so it is done only once."""
        return self.session.prepare(statement)

    def execute_async(
        self,
        statement: str,
        query_values: tuple,
        on_success: list[Callable[[Iterable | None], None]],
        on_error: list[Callable[[Exception], None]],
    ) -> None:
        """Executes query statement with given values asynchronously
        and run callbacks on success/failure"""
        prepared_statement = self._prepare_statement(statement)
        # todo: execute_async of python driver somehow does not work
        #  - there's almost no benefit of concurrency
        future = self.session.execute_async(query=prepared_statement, parameters=query_values)
        for callback in on_success:
            future.add_callback(callback)
        future.add_errback(on_error)

    def execute(self, statement: str, query_values: tuple = ()) -> Iterable | None:
        """Executes statement synchronously."""
        return self.session.execute(statement, parameters=query_values)  # type: ignore

    def __del__(self) -> None:
        logger.info("Closing connection to %s", self.cluster.metadata.all_hosts())
        self.cluster.shutdown()


class NoOpQueryExecutor(QueryExecutor):
    """Does nothing. Used when no oracle is configured."""
