from functools import partial
from itertools import zip_longest
from typing import Callable, Iterable

from gemini_python.executor import QueryExecutor, logger
from gemini_python import CqlDto


class GeminiValidator:
    """Class responsible for querying oracle and verifying returned results."""

    def __init__(self, oracle: QueryExecutor):
        self.oracle = oracle

    @staticmethod
    def _validate_fun(expected_it: Iterable | None, actual_it: Iterable | None) -> None:
        error_msg = "error while validation: %s != %s"
        if expected_it is None or actual_it is None:
            if expected_it is actual_it:
                return
            logger.error(error_msg, actual_it, expected_it)
            return
        for (
            actual,
            expected,
        ) in zip_longest(actual_it, expected_it):
            if actual != expected:
                logger.error(error_msg, actual, expected)

    def prepare_validation_method(self, cql_dto: CqlDto) -> Callable:
        # prepare statement upfront, otherwise it hangs when running inside sut executor callback
        self.oracle.prepare(cql_dto.statement)
        return partial(self.validate, cql_dto)

    def validate(self, cql_dto: CqlDto, expected_result: Iterable | None) -> None:
        validate_fun = partial(self._validate_fun, expected_result)
        self.oracle.execute_async(
            cql_dto,
            on_success=[validate_fun],
            on_error=[logger.exception],
        )
