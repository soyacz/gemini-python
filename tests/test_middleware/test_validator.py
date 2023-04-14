import pytest

from gemini_python import CqlDto, ValidationError
from gemini_python.query_driver import NoOpQueryDriver
from gemini_python.middleware.validator import GeminiValidator
from tests.utils.memory_query_driver import MemoryQueryDriver


def test_validator_passes_when_queried_rows_match():
    oracle_query_driver = MemoryQueryDriver()
    oracle_query_driver.execute_async(
        CqlDto(statement="insert", values=("test", "value")), on_success=[], on_error=[]
    )
    oracle_query_driver.execute_async(
        CqlDto(statement="insert", values=("row", "two")), on_success=[], on_error=[]
    )
    validator = GeminiValidator(oracle=oracle_query_driver)
    validator.validate(
        CqlDto(statement="select", values=()), expected_result=[("test", "value"), ("row", "two")]
    )


def test_validator_fails_when_oracle_returns_value_and_is_expected_empty_list():
    oracle_query_driver = MemoryQueryDriver()
    validator = GeminiValidator(oracle=oracle_query_driver)

    with pytest.raises(ValidationError):
        validator.validate(CqlDto(statement="select", values=()), expected_result=[("test",)])


def test_validator_fails_when_queried_rows_mismatch():
    oracle_query_driver = MemoryQueryDriver()
    oracle_query_driver.execute_async(
        CqlDto(statement="insert", values=("value",)), on_success=[], on_error=[]
    )
    validator = GeminiValidator(oracle=oracle_query_driver)
    with pytest.raises(ValidationError):
        validator.validate(CqlDto(statement="select", values=()), expected_result=[("not value",)])


def test_validator_fails_when_queried_rows_number_mismatch():
    oracle_query_driver = MemoryQueryDriver()
    validator = GeminiValidator(oracle=oracle_query_driver)
    oracle_query_driver.execute_async(
        CqlDto(statement="insert", values=("test",)), on_success=[], on_error=[]
    )
    with pytest.raises(ValidationError):
        validator.validate(CqlDto(statement="select", values=()), expected_result=["test", "value"])


def test_validator_passes_when_oracle_returns_empty_list_and_is_expected_empty_list():
    oracle_query_driver = MemoryQueryDriver()
    validator = GeminiValidator(oracle=oracle_query_driver)

    validator.validate(CqlDto(statement="select", values=()), expected_result=[])


def test_validator_fails_when_oracle_returns_empty_list_and_is_expected_value():
    oracle_query_driver = MemoryQueryDriver()
    validator = GeminiValidator(oracle=oracle_query_driver)

    with pytest.raises(ValidationError):
        validator.validate(CqlDto(statement="select", values=()), expected_result=["test"])


def test_validator_does_nothing_when_oracle_is_not_configured():
    oracle_query_driver = NoOpQueryDriver()
    validator = GeminiValidator(oracle=oracle_query_driver)
    validate = validator.prepare_validation_method(CqlDto(statement="insert", values=("", "value")))
    validate(expected_result="I would fail if oracle was configured")
