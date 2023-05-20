from itertools import zip_longest
from typing import Iterable

from gemini_python import ValidationError


def validate_result(oracle_result: Iterable, sut_result: Iterable) -> None:
    for (
        actual,
        expected,
    ) in zip_longest(oracle_result, sut_result):
        if actual != expected:
            raise ValidationError(expected, actual)
