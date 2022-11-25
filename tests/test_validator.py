from gemini_python import CqlDto
from gemini_python.validator import GeminiValidator
from tests.utils.memory_executor import MemoryExecutor


def test_validator_passes_when_queried_rows_match():
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)
    validator.validate(
        CqlDto(statement="insert", values=("test", "value")),
        expected_result=None,
    )

    validator.validate(CqlDto(statement="select", values=()), expected_result=["test", "value"])


def test_validator_fails_when_oracle_returns_value_and_is_expected_none():
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)

    validator.validate(CqlDto(statement="select", values=()), expected_result=["test"])


def test_validator_fails_when_queried_rows_mismatch():
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)
    validator.validate(
        CqlDto(statement="insert", values=("", "value")),
        expected_result=None,
    )

    validator.validate(CqlDto(statement="select", values=()), expected_result=["test", "value"])


def test_validator_fails_when_queried_rows_number_mismatch():
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)
    validator.validate(
        CqlDto(statement="insert", values=("test",)),
        expected_result=None,
    )

    validator.validate(CqlDto(statement="select", values=()), expected_result=["test", "value"])


def test_validator_passes_when_oracle_returns_none_and_is_expected_none():
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)

    validator.validate(CqlDto(statement="select", values=()), expected_result=None)


def test_validator_fails_when_oracle_returns_none_and_is_expected_value():
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)
    validator.validate(
        CqlDto(statement="insert", values=("", "value")),
        expected_result=None,
    )

    validator.validate(CqlDto(statement="select", values=()), expected_result=None)
