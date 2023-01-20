class ReplicationStrategy:  # pylint: disable=too-few-public-methods
    """Class storing information about data replication strategy"""


class SimpleReplicationStrategy(ReplicationStrategy):
    """Represents SimpleStrategy replication strategy"""

    class_: str = "SimpleStrategy"

    def __init__(self, replication_factor: int):
        self.replication_factor = replication_factor

    def __str__(self) -> str:
        return f"{{'class': '{self.class_}', 'replication_factor': {self.replication_factor}}}"


class NetworkTopologyReplicationStrategy(ReplicationStrategy):
    """Represents NetworkTopologyStrategy replication strategy"""

    class_: str = "NetworkTopologyStrategy"

    def __init__(self, **replication_factors: int):
        """
        Provide datacenter names with corresponding factor as parameters. E.g.:
        NetworkTopologyReplicationStrategy(dc1=3, dc2=2, dc3=1)
        """
        self.replication_factors = replication_factors

    def __str__(self) -> str:
        factors = ", ".join(
            [f"'{key}': {value}" for key, value in self.replication_factors.items()]
        )
        return f"{{'class': '{self.class_}', {factors}}}"
