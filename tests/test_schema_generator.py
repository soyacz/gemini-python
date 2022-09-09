from gemini_python.schema import generate_schema


def test_schema_can_generate_keyspace_and_tables_ddl_queries():
    keyspace = generate_schema()
    assert keyspace.name == "gemini"
    assert keyspace.tables
    schema_cql_statements = keyspace.as_cql()
    assert (
        schema_cql_statements[0] == "CREATE KEYSPACE IF NOT EXISTS gemini with "
        "replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};"
    )
    assert (
        schema_cql_statements[1]
        == "CREATE TABLE IF NOT EXISTS gemini.table1 (pk bigint, col1 ascii, PRIMARY KEY ((pk)));"
    )
