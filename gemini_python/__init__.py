import logging
import threading
from dataclasses import dataclass
from enum import unique, Enum
from multiprocessing.synchronize import Event as EventClass
from pathlib import Path
from typing import List, Callable, Iterable, Dict, Optional, Union

logger = logging.getLogger(__name__)


@dataclass
class CqlDto:
    """Data Transfer Object for CQL statements with values"""

    statement: str
    values: tuple = ()


class Operation(Enum):
    """Operation type"""

    WRITE = "write"
    READ = "read"


@unique
class QueryMode(Enum):
    """Query operation mode available options"""

    WRITE = "write"
    READ = "read"
    MIXED = "mixed"


@dataclass
class GeminiConfiguration:  # pylint: disable=too-many-instance-attributes
    """Configuration parameters for Gemini"""

    mode: QueryMode = QueryMode.WRITE
    test_cluster: Optional[List[str]] = None
    oracle_cluster: Optional[List[str]] = None
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
    fail_fast: bool = False
    max_mutation_retries: int = 2
    max_mutation_retries_backoff: float = 0.5
    ttl: int = 0
    history_files_max_size_gb: int = 1
    history_files_dir: Path = Path.cwd() / ".gemini"
    outfile: Optional[Path] = None


OnSuccessClb = Callable[[Optional[Iterable]], None]
OnErrorClb = Callable[[Exception], None]


class ValidationError(Exception):
    """Exception raised when validation fails"""

    def __init__(self, expected: Union[Dict, Iterable, None], actual: Union[Dict, Iterable, None]):
        self.actual = actual
        self.expected = expected
        super().__init__(f"Expected: {expected}, actual: {actual}")


def set_event_after_timeout(event: EventClass, timeout: int) -> threading.Timer:
    timer = threading.Timer(timeout, event.set)
    timer.start()
    return timer
