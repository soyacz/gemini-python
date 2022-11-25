import logging
import time
from multiprocessing import Process

from gemini_python.executor import (
    QueryExecutorFactory,
)
from gemini_python.limiter import ConcurrencyLimiter
from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import Keyspace
from gemini_python.validator import GeminiValidator

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
        self,
        schema: Keyspace,
        mode: str,
        test_cluster: list[str] | None,
        oracle_cluster: list[str] | None,
        duration: int = 10000,
    ):
        super().__init__()
        self._mode = mode
        self._schema = schema
        self._test_cluster = test_cluster
        self._oracle_cluster = oracle_cluster
        self._duration = duration
        assert duration > 0, "duration should be greater than 0 seconds"

    def run(self) -> None:
        executed_queries_count = 0
        start_time = time.time()
        # executors must be created in run() method, otherwise cassandra driver hangs
        sut_query_executor = QueryExecutorFactory.create_executor(self._test_cluster)
        oracle_query_executor = QueryExecutorFactory.create_executor(self._oracle_cluster)
        validator = GeminiValidator(oracle=oracle_query_executor)
        generator = LoadGenerator(schema=self._schema, mode=self._mode)
        concurrency = ConcurrencyLimiter(limit=100)
        while time.time() - start_time < self._duration:
            concurrency.increment()
            cql_dto = generator.get_query()
            validate_results = validator.prepare_validation_method(cql_dto)
            sut_query_executor.execute_async(
                cql_dto,
                on_success=[concurrency.decrement, validate_results],
                on_error=[concurrency.decrement, handle_exception],
            )
            executed_queries_count += 1
        logger.info("inserted %s rows in %s seconds", executed_queries_count, self._duration)
        logger.info("%s statements/s", round(executed_queries_count / self._duration))
