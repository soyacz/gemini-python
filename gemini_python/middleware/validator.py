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
from gemini_python.query_driver import NoOpQueryDriver, QueryDriver
from gemini_python.middleware import Middleware
from gemini_python.subprocess_query_driver import SubprocessQueryDriver

logger = logging.getLogger(__name__)


class GeminiValidator:
    """Class responsible for querying oracle and verifying returned results."""

    def __init__(self, oracle: QueryDriver):
        self._oracle = oracle

    @staticmethod
    def _validate_fun(expected_it: Iterable | None, actual_it: Iterable | None) -> None:
        if expected_it is None or actual_it is None:
            if expected_it is actual_it:
                return
            raise ValidationError(expected_it, actual_it)
        for (
            actual,
            expected,
        ) in zip_longest(actual_it, expected_it):
            if actual != expected:
                raise ValidationError(expected, actual)

    def prepare_validation_method(self, cql_dto: CqlDto) -> Callable:
        # prepare statement upfront, otherwise it hangs when running inside sut query driver callback
        if isinstance(self._oracle, NoOpQueryDriver):
            # don't validate when oracle is not configured
            return lambda *args, **kwargs: None
        return partial(self.validate, cql_dto)

    def validate(self, cql_dto: CqlDto, expected_result: Iterable | None) -> None:
        result = self._oracle.execute(cql_dto)
        self._validate_fun(expected_result, result)

    def teardown(self) -> None:
        self._oracle.teardown()


class Validator(Middleware):
    """Middleware for SUT query result comparison with oracle's query result."""

    def __init__(self, config: GeminiConfiguration) -> None:
        super().__init__(config)
        self._gemini_validator = GeminiValidator(SubprocessQueryDriver(config.oracle_cluster))

    def run(self, cql_dto: CqlDto) -> Tuple[OnSuccessClb, OnErrorClb]:
        return self._gemini_validator.prepare_validation_method(cql_dto), log_error

    def teardown(self) -> None:
        self._gemini_validator.teardown()
