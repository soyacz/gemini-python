from dataclasses import dataclass
from enum import unique, Enum
from typing import List, Callable, Iterable, Any


@dataclass
class CqlDto:
    """Data Transfer Object for CQL statements with values"""

    statement: str
    values: tuple = ()


@unique
class QueryMode(Enum):
    """Query operation mode available options"""

    WRITE = "write"
    READ = "read"


@dataclass
class GeminiConfiguration:
    """Configuration parameters for Gemini"""

    mode: QueryMode = QueryMode.WRITE
    test_cluster: List[str] | None = None
    oracle_cluster: List[str] | None = None
    duration: int = 3
    drop_schema: bool = False


# pylint: disable=unused-argument
def do_nothing(*args: Any, **kwargs: Any) -> None:
    """Does nothing. May be sometimes required."""


OnSuccessClb = Callable[[Iterable | None], None]
OnErrorClb = Callable[[Exception], None]
