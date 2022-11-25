import time
from multiprocessing import Process

from gemini_python.executor import (
    logger,
    QueryExecutorFactory,
)
from gemini_python.limiter import ConcurrencyLimiter
from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import Keyspace
from gemini_python.validator import GeminiValidator


def handle_exception(exception: Exception) -> None:
    logger.error(exception)


class GeminiProcess(Process):
    """
    Main Gemini process - creates connections, queries and validates results in accordance to config.

    queries_count param is temporary for limiting time gemini is executed.
    # todo replace queries_count with duration param
    """

    def __init__(
        self,
        schema: Keyspace,
        mode: str,
        test_cluster: list[str] | None,
        oracle_cluster: list[str] | None,
        queries_count: int = 10000,
    ):
        super().__init__()
        self._mode = mode
        self._schema = schema
        self._test_cluster = test_cluster
        self._oracle_cluster = oracle_cluster
        self._queries_count = queries_count

    def run(self) -> None:
        i = 0
        start_time = time.time()
        # executors must be created in run() method, otherwise cassandra driver hangs
        sut_query_executor = QueryExecutorFactory.create_executor(self._test_cluster)
        oracle_query_executor = QueryExecutorFactory.create_executor(self._oracle_cluster)
        validator = GeminiValidator(oracle=oracle_query_executor)
        generator = LoadGenerator(schema=self._schema, mode=self._mode)
        concurrency = ConcurrencyLimiter(limit=100)
        while True:
            concurrency.increment()
            cql_dto = generator.get_query()
            validate_results = validator.prepare_validation_method(cql_dto)
            sut_query_executor.execute_async(
                cql_dto,
                on_success=[concurrency.decrement, validate_results],
                on_error=[concurrency.decrement, handle_exception],
            )
            i += 1
            if not i % self._queries_count:
                secs = time.time() - start_time
                print(f"inserted next {self._queries_count} rows in {secs:.2f} seconds")
                print(self._queries_count / secs)
                break
