from gemini_python.schema import generate_schema
from tests.utils.recording_executor import RecordingExecutor


def test_schema_can_generate_keyspace_and_tables_ddl_queries():
    keyspace = generate_schema()
    assert keyspace.name == "gemini"
    assert keyspace.tables
    schema_cql_statements = keyspace.as_cqls()
    assert (
        schema_cql_statements[0] == "CREATE KEYSPACE IF NOT EXISTS gemini with "
        "replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};"
    )
    assert (
        schema_cql_statements[1]
        == "CREATE TABLE IF NOT EXISTS gemini.table1 (pk bigint, col1 ascii, PRIMARY KEY ((pk)));"
    )


def test_schema_can_be_created_in_database():
    executor = RecordingExecutor()
    keyspace = generate_schema()
    keyspace.create(executor)
    assert len(keyspace.as_cqls()) == len(executor.executed_cqls)
    for ks_cql, executed_cql in zip(keyspace.as_cqls(), executor.executed_cqls):
        assert ks_cql == executed_cql[0]
