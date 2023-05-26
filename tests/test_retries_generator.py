import time

from gemini_python import CqlDto, Operation
from gemini_python.retries_generator import RetriesGenerator


def test_can_add_retry_to_retries_generator_and_get_it_after_backoff_time():
    gen = RetriesGenerator(max_mutation_retries_backoff=0.005)
    cql_dto = CqlDto(statement="select * from test", values=())
    operation = Operation.READ
    for _ in range(1):  # set to 1000 to verify test stability
        gen.add_retry(operation, cql_dto, attempt=1)
        assert gen.retry_available() is False, "Retry should not be available before backoff time"
        time.sleep(0.006)
        assert gen.retry_available() is True, "Retry should be available after backoff time"
        assert gen.get_retry() == (operation, cql_dto, 1)


def test_can_add_multiple_retry_to_retries_generator_and_get_it_after_backoff_time():
    gen = RetriesGenerator(max_mutation_retries_backoff=0.01)
    cql_dto = CqlDto(statement="select * from test", values=())
    operation = Operation.READ
    for _ in range(1):  # set to 1000 to verify test stability
        gen.add_retry(operation, cql_dto, attempt=1)
        time.sleep(0.007)
        gen.add_retry(Operation.WRITE, cql_dto, attempt=1)
        assert gen.retry_available() is False, "Retry should not be available before backoff time"
        time.sleep(0.005)
        assert gen.retry_available() is True, "Retry should be available after backoff time"
        assert gen.get_retry() == (operation, cql_dto, 1)
        assert (
            gen.retry_available() is False
        ), "Second Retry should not be available before backoff time"
        time.sleep(0.007)
        assert gen.retry_available() is True, "Retry should be available after backoff time"
        assert gen.get_retry() == (Operation.WRITE, cql_dto, 1)
