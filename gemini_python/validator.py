from itertools import zip_longest
from typing import Iterable

from gemini_python import ValidationError


def validate_result(oracle_result: Iterable, sut_result: Iterable) -> None:
    for (
        oracle_value,
        sut_value,
    ) in zip_longest(oracle_result, sut_result):
        if oracle_value != sut_value:
            raise ValidationError(oracle_value, sut_value)
