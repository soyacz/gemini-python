#  pylint: disable=no-name-in-module
import logging
from abc import ABC
from functools import lru_cache
from typing import Iterable, List

from cassandra.cluster import Cluster  # type: ignore
from cassandra.policies import RoundRobinPolicy  # type: ignore
from cassandra.query import PreparedStatement  # type: ignore
from cassandra.io.libevreactor import LibevConnection  # type: ignore

from gemini_python import CqlDto, OnSuccessClb, OnErrorClb

logger = logging.getLogger(__name__)


class QueryExecutor(ABC):
    """Responsible for communication with DB and running queries."""

    def execute_async(
        self,
        cql_dto: CqlDto,
        on_success: List[OnSuccessClb],
        on_error: List[OnErrorClb],
    ) -> None:
        """Execute statement asynchronously."""

    def execute(self, cql_dto: CqlDto) -> Iterable | None:
        """Executes statement synchronously."""

    def prepare(self, statement: str) -> None:
        """Preparation before running statement."""

    def teardown(self) -> None:
        """Proper query executor shutdown, closing all connections, feeing resources."""


class CqlQueryExecutor(QueryExecutor):
    """Communicates with and queries Scylla/Cassandra databases."""

    def __init__(self, hosts: List[str], port: int = 9042) -> None:
        kwargs = {"metrics_enabled": False, "connection_class": LibevConnection}
        self.cluster = Cluster(
            hosts, port=port, load_balancing_policy=RoundRobinPolicy(), protocol_version=4, **kwargs
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
        cql_dto: CqlDto,
        on_success: List[OnSuccessClb],
        on_error: List[OnErrorClb],
    ) -> None:
        """Executes cql statement with given values asynchronously
        and run callbacks on success/failure"""
        prepared_statement = self._prepare_statement(cql_dto.statement)
        future = self.session.execute_async(query=prepared_statement, parameters=cql_dto.values)
        for callback in on_success:
            future.add_callback(callback)
        for err_callback in on_error:
            future.add_errback(err_callback)

    def execute(self, cql_dto: CqlDto) -> Iterable | None:
        """Executes statement synchronously."""
        return self.session.execute(cql_dto.statement, parameters=cql_dto.values)  # type: ignore

    def teardown(self) -> None:
        logger.debug("Closing connection with %s", self.cluster.metadata.all_hosts())
        self.cluster.shutdown()

    def __del__(self) -> None:
        self.teardown()


class NoOpQueryExecutor(QueryExecutor):
    """Does nothing. Used when no oracle is configured."""

    def execute_async(
        self,
        cql_dto: CqlDto,
        on_success: List[OnSuccessClb],
        on_error: List[OnErrorClb],
    ) -> None:
        for callback in on_success:
            callback(None)


class QueryExecutorFactory:
    """Creates QueryExecutor objects according to cluster parameters."""

    @classmethod
    def create_executor(cls, cluster_ips: List[str] | None = None) -> QueryExecutor:
        if cluster_ips:
            return CqlQueryExecutor(cluster_ips)
        return NoOpQueryExecutor()
