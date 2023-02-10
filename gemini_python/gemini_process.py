import logging
import time
from multiprocessing import Process

from gemini_python import GeminiConfiguration
from gemini_python.executor import (
    QueryExecutorFactory,
)
from gemini_python.load_generator import LoadGenerator
from gemini_python.middleware import init_middlewares, run_middlewares
from gemini_python.middleware.concurrency_limiter import ConcurrencyLimiterMiddleware
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

    def __init__(self, config: GeminiConfiguration, schema: Keyspace):
        super().__init__()
        self._config = config
        self._schema = schema
        self._partitions = self._generate_partitions()
        assert config.duration > 0, "duration should be greater than 0 seconds"

    def _generate_partitions(self) -> list[list[tuple]]:
        tables_partitions: list[list[tuple]] = []
        for table in self._schema.tables:
            tables_partitions.append(
                [
                    tuple(column.generate_random_value() for column in table.primary_keys)
                    for _ in range(self._config.token_range_slices // self._config.concurrency)
                ]
            )
        return tables_partitions

    def run(self) -> None:
        executed_queries_count = 0
        start_time = time.time()
        # executors must be created in run() method, otherwise cassandra driver hangs
        sut_query_executor = QueryExecutorFactory.create_executor(self._config.test_cluster)
        generator = LoadGenerator(
            schema=self._schema, mode=self._config.mode, partitions=self._partitions
        )
        middlewares = init_middlewares(self._config, [ConcurrencyLimiterMiddleware, Validator])
        while time.time() - start_time < self._config.duration:
            cql_dto = generator.get_query()
            on_success_callbacks, on_error_callbacks = run_middlewares(cql_dto, middlewares)
            sut_query_executor.execute_async(
                cql_dto,
                on_success=on_success_callbacks,
                on_error=on_error_callbacks + [handle_exception],
            )
            executed_queries_count += 1
        logger.info("%s statements/s", round(executed_queries_count / self._config.duration))
        for middleware in middlewares:
            middleware.teardown()
        sut_query_executor.teardown()
