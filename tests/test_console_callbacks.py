import pytest

from gemini_python.console import time_period_str_to_seconds


@pytest.mark.parametrize(
    "duration,seconds",
    (
        ("1h1m20s", 3680),
        ("1m20s", 80),
        ("1h20s", 3620),
        ("25m", 1500),
        ("10h", 36000),
        ("25s", 25),
        ("500ms", 0.5),
    ),
)
def test_duration_str_to_seconds_function(duration, seconds):
    assert time_period_str_to_seconds(duration) == seconds
