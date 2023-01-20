from gemini_python.replication_strategy import (
    SimpleReplicationStrategy,
    NetworkTopologyReplicationStrategy,
)


def test_can_create_simple_replication_strategy():
    strategy = SimpleReplicationStrategy(replication_factor=3)
    assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 3}"


def test_can_create_network_topology_replication_strategy():
    strategy = NetworkTopologyReplicationStrategy(dc1=3, dc2=8)
    assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 8}"
