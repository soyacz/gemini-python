import logging
from multiprocessing import Process
from multiprocessing.synchronize import Event as EventClass
from queue import Queue

from gemini_python import GeminiConfiguration, ProcessResult, ValidationError
from gemini_python.query_driver import (
    QueryDriverFactory,
    QueryDriverException,
)
from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import Keyspace
from gemini_python.validator import validate_result

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class GeminiProcess(Process):
    """
    Main Gemini process - creates connections, queries and validates results in accordance to config.

    queries_count param is temporary for limiting time gemini is executed.
    """

    def __init__(
        self,
        config: GeminiConfiguration,
        schema: Keyspace,
        termination_event: EventClass,
        results_queue: Queue[ProcessResult],
    ):
        super().__init__()
        self._gemini_config = config
        self._schema = schema
        self._partitions = self._generate_partitions()
        self._termination_event: EventClass = termination_event
        self._results_queue: Queue[ProcessResult] = results_queue
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
        oracle_query_driver = QueryDriverFactory.create_query_driver(
            self._gemini_config.oracle_cluster
        )
        process_result = ProcessResult()
        generator = LoadGenerator(
            schema=self._schema, mode=self._gemini_config.mode, partitions=self._partitions
        )
        while not self._termination_event.is_set():
            operation, cql_dto = generator.get_query()
            try:
                sut_result = sut_query_driver.execute(cql_dto)
                oracle_result = oracle_query_driver.execute(cql_dto)
                validate_result(oracle_result, sut_result)
            except (QueryDriverException, ValidationError) as exc:
                logger.error(exc)
                process_result.increment_errors(operation)
                if self._gemini_config.fail_fast:
                    self._termination_event.set()
                continue
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Unhandled exception when querying SUT.: %s", exc, exc_info=True)
                self._termination_event.set()
                continue
            process_result.increment_ops(operation)
        self._results_queue.put(process_result)
        sut_query_driver.teardown()
        oracle_query_driver.teardown()
