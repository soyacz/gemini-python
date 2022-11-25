from dataclasses import dataclass


@dataclass
class CqlDto:
    """Data Transfer Object for CQL statements with values"""

    statement: str
    values: tuple = ()
