from gemini_python import CqlDto
from gemini_python.executor import NoOpQueryExecutor
from gemini_python.middleware.validator import GeminiValidator
from tests.utils.memory_executor import MemoryExecutor


def test_validator_passes_when_queried_rows_match(caplog):
    oracle_executor = MemoryExecutor()
    oracle_executor.execute_async(
        CqlDto(statement="insert", values=("test", "value")), on_success=[], on_error=[]
    )
    oracle_executor.execute_async(
        CqlDto(statement="insert", values=("row", "two")), on_success=[], on_error=[]
    )
    validator = GeminiValidator(oracle=oracle_executor)

    validator.validate(
        CqlDto(statement="select", values=()), expected_result=[("test", "value"), ("row", "two")]
    )
    assert "validation error" not in caplog.text


def test_validator_fails_when_oracle_returns_value_and_is_expected_empty_list(caplog):
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)

    validator.validate(CqlDto(statement="select", values=()), expected_result=[("test",)])
    assert "validation error" in caplog.text


def test_validator_fails_when_queried_rows_mismatch(caplog):
    oracle_executor = MemoryExecutor()
    oracle_executor.execute_async(
        CqlDto(statement="insert", values=("value",)), on_success=[], on_error=[]
    )
    validator = GeminiValidator(oracle=oracle_executor)

    validator.validate(CqlDto(statement="select", values=()), expected_result=[("not value",)])
    assert "validation error" in caplog.text


def test_validator_fails_when_queried_rows_number_mismatch(caplog):
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)
    oracle_executor.execute_async(
        CqlDto(statement="insert", values=("test",)), on_success=[], on_error=[]
    )

    validator.validate(CqlDto(statement="select", values=()), expected_result=["test", "value"])
    assert "validation error" in caplog.text


def test_validator_passes_when_oracle_returns_empty_list_and_is_expected_empty_list(caplog):
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)

    validator.validate(CqlDto(statement="select", values=()), expected_result=[])
    assert "validation error" not in caplog.text


def test_validator_fails_when_oracle_returns_empty_list_and_is_expected_value(caplog):
    oracle_executor = MemoryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)

    validator.validate(CqlDto(statement="select", values=()), expected_result=["test"])
    assert "validation error" in caplog.text


def test_validator_does_nothing_when_oracle_is_not_configured(caplog):
    oracle_executor = NoOpQueryExecutor()
    validator = GeminiValidator(oracle=oracle_executor)
    validate = validator.prepare_validation_method(CqlDto(statement="insert", values=("", "value")))
    validate(expected_result="I would fail if oracle was configured")
    assert "validation error" not in caplog.text
