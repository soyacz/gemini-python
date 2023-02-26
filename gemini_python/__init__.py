import logging
from dataclasses import dataclass
from enum import unique, Enum
from typing import List, Callable, Iterable, Any

logger = logging.getLogger(__name__)


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
class GeminiConfiguration:  # pylint: disable=too-many-instance-attributes
    """Configuration parameters for Gemini"""

    mode: QueryMode = QueryMode.WRITE
    test_cluster: List[str] | None = None
    oracle_cluster: List[str] | None = None
    duration: int = 3
    drop_schema: bool = False
    token_range_slices: int = 10000
    concurrency: int = 4
    seed: int = 0
    max_tables: int = 1
    min_partition_keys: int = 2
    max_partition_keys: int = 6
    min_clustering_keys: int = 2
    max_clustering_keys: int = 4
    min_columns: int = 8
    max_columns: int = 16


# pylint: disable=unused-argument
def do_nothing(*args: Any, **kwargs: Any) -> None:
    """Does nothing. May be sometimes required."""


def log_error(*args: Any, **kwargs: Any) -> None:
    """Logging error callback"""
    logger.error("error: %s, %s", args, kwargs)


OnSuccessClb = Callable[[Iterable | None], None]
OnErrorClb = Callable[[Exception], None]
