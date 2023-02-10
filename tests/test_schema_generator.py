from gemini_python import CqlDto
from gemini_python.replication_strategy import SimpleReplicationStrategy
from gemini_python.schema import generate_schema
from tests.utils.recording_executor import RecordingExecutor


def test_schema_can_generate_keyspace_and_tables_ddl_queries():
    keyspace = generate_schema(seed=1234)
    assert keyspace.name == "gemini"
    assert keyspace.tables
    queries = keyspace.as_queries(replication_strategy=SimpleReplicationStrategy(3))
    expected_queries = [
        CqlDto(
            "CREATE KEYSPACE IF NOT EXISTS gemini with "
            "replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"
        ),
        CqlDto(
            "CREATE TABLE IF NOT EXISTS gemini.table1 (pk bigint, col1 ascii, PRIMARY KEY ((pk)));"
        ),
    ]
    assert queries == expected_queries


def test_schema_can_be_created_in_database():
    executor = RecordingExecutor()
    keyspace = generate_schema(seed=1234)
    keyspace.create(executor, SimpleReplicationStrategy(3))
    assert len(keyspace.as_queries(SimpleReplicationStrategy(3))) == len(executor.executed_queries)
    for ks_cql, executed_cql in zip(
        keyspace.as_queries(SimpleReplicationStrategy(3)), executor.executed_queries
    ):
        assert ks_cql == executed_cql
