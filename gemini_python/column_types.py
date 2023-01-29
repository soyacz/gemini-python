import random
import string
import sys
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Column:
    """Base class for Columns types.

    Column is capable of creating valid values for itself."""

    name: str
    cql_type: str

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
    length: int = 100

    def generate_random_value(self) -> Any:
        return "".join(random.choices(string.ascii_letters + string.digits, k=self.length))

    def generate_sequence_value(self) -> Any:
        return self.generate_random_value()


@dataclass
class BigIntColumn(Column):
    """Represents 'bigint' column type"""

    cql_type: str = "bigint"
    _seq: int = 0

    def generate_random_value(self) -> Any:
        return random.randint(-sys.maxsize - 1, sys.maxsize)

    def generate_sequence_value(self) -> int:
        self._seq += 1
        return self._seq
