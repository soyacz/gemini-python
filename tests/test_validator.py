import pytest

from gemini_python import ValidationError
from gemini_python.validator import validate_result


def test_validate_result_same_passes():
    oracle_result = [1, 2, 3, 4, 5]
    sut_result = [1, 2, 3, 4, 5]
    try:
        validate_result(oracle_result, sut_result)
    except ValidationError:
        pytest.fail("ValidationError was raised unexpectedly!")


def test_validate_result_different_raises_validation_error():
    oracle_result = [1, 2, 3, 4, 5]
    sut_result = [1, 2, 3, 4, 6]  # Different last element
    with pytest.raises(ValidationError) as exc_info:
        validate_result(oracle_result, sut_result)
    assert exc_info.value.actual == 5  # type: ignore
    assert exc_info.value.expected == 6  # type: ignore


def test_validate_result_different_length_raises_validation_error():
    oracle_result = [1, 2, 3, 4, 5]
    sut_result = [1, 2, 3]  # Shorter input
    with pytest.raises(ValidationError) as exc_info:
        validate_result(oracle_result, sut_result)
    assert exc_info.value.actual == 4  # type: ignore
    assert exc_info.value.expected is None
