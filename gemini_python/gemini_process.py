import logging
from multiprocessing import Process
from multiprocessing.synchronize import Event as EventClass
from typing import Type

from gemini_python import GeminiConfiguration
from gemini_python.error_handlers import base_error_handler
from gemini_python.query_driver import (
    QueryDriverFactory,
    QueryDriverException,
)
from gemini_python.load_generator import LoadGenerator
from gemini_python.middleware import init_middlewares, run_middlewares, Middleware
from gemini_python.middleware.performance_counter import PerformanceCounterMiddleware
from gemini_python.middleware.validator import Validator
from gemini_python.schema import Keyspace


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handle_exception(exception: Exception) -> None:
    logger.error(exception)


class GeminiProcess(Process):
    """
    Main Gemini process - creates connections, queries and validates results in accordance to config.

    queries_count param is temporary for limiting time gemini is executed.
    """

    def __init__(
        self, config: GeminiConfiguration, schema: Keyspace, termination_event: EventClass
    ):
        super().__init__()
        self._gemini_config = config
        self._schema = schema
        self._partitions = self._generate_partitions()
        self._termination_event: EventClass = termination_event
        assert config.duration > 0, "duration should be greater than 0 seconds"

    def _generate_partitions(self) -> list[list[tuple]]:
        tables_partitions: list[list[tuple]] = []
        for table in self._schema.tables:
            tables_partitions.append(
                [
                    tuple(column.generate_random_value() for column in table.partition_keys)
                    for _ in range(
                        self._gemini_config.token_range_slices // self._gemini_config.concurrency
                    )
                ]
            )
        return tables_partitions

    def run(self) -> None:
        # query drivers must be created in run() method and not in __init__, otherwise cassandra driver hangs
        sut_query_driver = QueryDriverFactory.create_query_driver(self._gemini_config.test_cluster)
        generator = LoadGenerator(
            schema=self._schema, mode=self._gemini_config.mode, partitions=self._partitions
        )
        active_middlewares: list[Type[Middleware]] = [PerformanceCounterMiddleware]
        if self._gemini_config.oracle_cluster:
            active_middlewares.append(Validator)
        middlewares = init_middlewares(self._gemini_config, active_middlewares)
        error_handler = base_error_handler(
            config=self._gemini_config, termination_event=self._termination_event
        )
        while not self._termination_event.is_set():
            cql_dto = generator.get_query()
            on_success_callbacks, on_error_callbacks = run_middlewares(
                cql_dto, middlewares, error_handler
            )
            try:
                result = sut_query_driver.execute(cql_dto)
            except QueryDriverException as exc:
                for err_clb in on_error_callbacks:
                    err_clb(exc)
                continue
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Unhandled exception when querying SUT.: %s", exc, exc_info=True)
                self._termination_event.set()
                continue
            for clb in on_success_callbacks:
                clb(result)
        for middleware in middlewares:
            middleware.teardown()
        sut_query_driver.teardown()
