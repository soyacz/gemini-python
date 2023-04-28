import logging
from functools import partial
from itertools import zip_longest
from typing import Iterable, Callable, Tuple

from gemini_python import (
    GeminiConfiguration,
    CqlDto,
    OnSuccessClb,
    OnErrorClb,
    log_error,
    ValidationError,
)
from gemini_python.query_driver import QueryDriver, QueryDriverFactory
from gemini_python.middleware import Middleware

logger = logging.getLogger(__name__)


class GeminiValidator:
    """Class responsible for querying oracle and verifying returned results."""

    def __init__(self, oracle: QueryDriver):
        self._oracle = oracle

    @staticmethod
    def _validate_fun(expected_it: Iterable, actual_it: Iterable) -> None:
        for (
            actual,
            expected,
        ) in zip_longest(actual_it, expected_it):
            if actual != expected:
                raise ValidationError(expected, actual)

    def prepare_validation_method(self, cql_dto: CqlDto) -> Callable:
        return partial(self.validate, cql_dto)

    def validate(self, cql_dto: CqlDto, expected_result: Iterable) -> None:
        result = self._oracle.execute(cql_dto)
        self._validate_fun(expected_result, result)

    def teardown(self) -> None:
        self._oracle.teardown()


class Validator(Middleware):
    """Middleware for SUT query result comparison with oracle's query result."""

    def __init__(self, config: GeminiConfiguration) -> None:
        super().__init__(config)
        self._gemini_validator = GeminiValidator(
            QueryDriverFactory.create_query_driver(config.oracle_cluster)
        )

    def run(self, cql_dto: CqlDto) -> Tuple[OnSuccessClb, OnErrorClb]:
        return self._gemini_validator.prepare_validation_method(cql_dto), log_error

    def teardown(self) -> None:
        self._gemini_validator.teardown()
