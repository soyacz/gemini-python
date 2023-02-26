import hashlib
import logging
import random
import string
import sys
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class Column:
    """Base class for Columns types.

    Column is capable of creating valid values for itself."""

    name: str
    cql_type: str
    seed: int = 0
    size: int = 100

    def __post_init__(self) -> None:
        self.seed = (
            self.seed + int(hashlib.sha256(self.name.encode("utf-8")).hexdigest(), 16) % 10**8
        )
        self._random = random.Random(self.seed)
        logger.debug("Column %s (%s) initialized with seed %s", self.name, self.cql_type, self.seed)

    def __str__(self) -> str:
        return f"{self.name} {self.cql_type}"

    def generate_sequence_value(self) -> Any:
        """Generates next sequence value for given Column"""

    def generate_random_value(self) -> Any:
        """Generates random value for given Column"""


@dataclass
class AsciiColumn(Column):
    """Represents 'ascii' column type"""

    name: str
    cql_type: str = field(default="ascii")
    seed: int = 0
    size: int = 100

    def generate_random_value(self) -> Any:
        return "".join(self._random.choices(string.ascii_letters + string.digits, k=self.size))

    def generate_sequence_value(self) -> Any:
        return self.generate_random_value()


@dataclass
class BigIntColumn(Column):
    """Represents 'bigint' column type"""

    cql_type: str = "bigint"
    _seq: int = 0
    seed = 0
    size = sys.maxsize

    def generate_random_value(self) -> Any:
        return self._random.randint(-self.size - 1, self.size)

    def generate_sequence_value(self) -> int:
        self._seq += 1
        return self._seq


ALL_COLUMN_TYPES = [AsciiColumn, BigIntColumn]
