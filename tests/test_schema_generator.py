from gemini_python import CqlDto, GeminiConfiguration
from gemini_python.column_types import BigIntColumn, AsciiColumn
from gemini_python.replication_strategy import SimpleReplicationStrategy
from gemini_python.schema import generate_schema
from tests.utils.recording_query_driver import RecordingQueryDriver


def test_schema_can_generate_keyspace_and_tables_ddl_queries():
    config = GeminiConfiguration(
        seed=1234,
        min_columns=2,
        max_columns=2,
        max_clustering_keys=2,
        max_partition_keys=2,
    )
    schema = generate_schema(
        config,
        pk_types=[AsciiColumn, BigIntColumn],
        ck_types=[AsciiColumn, BigIntColumn],
        c_types=[AsciiColumn, BigIntColumn],
    )
    assert schema.name == "gemini"
    assert schema.tables
    queries = schema.as_queries(replication_strategy=SimpleReplicationStrategy(3))
    expected_queries = [
        CqlDto(
            "CREATE KEYSPACE IF NOT EXISTS gemini with "
            "replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"
        ),
        CqlDto(
            "CREATE TABLE IF NOT EXISTS gemini.table0"
            " (pk0 ascii, pk1 ascii, ck0 ascii, ck1 ascii, col0 bigint, col1 ascii,"
            " PRIMARY KEY ((pk0, pk1), ck0, ck1));"
        ),
    ]
    assert queries == expected_queries
    # verify sql queries
    queries = schema.as_sql()
    assert queries == [
        CqlDto(
            "CREATE TABLE IF NOT EXISTS 'gemini.table0'"
            " (id INTEGER PRIMARY KEY AUTOINCREMENT, d_time INTEGER , pk0 TEXT, pk1 TEXT, ck0 TEXT, ck1 TEXT);"
        )
    ]


def test_can_create_table_with_one_pk():
    config = GeminiConfiguration(
        seed=1234,
        min_columns=2,
        max_columns=2,
        max_clustering_keys=2,
        min_partition_keys=1,
        max_partition_keys=1,
    )
    schema = generate_schema(
        config,
        pk_types=[AsciiColumn, BigIntColumn],
        ck_types=[AsciiColumn, BigIntColumn],
        c_types=[AsciiColumn, BigIntColumn],
    )
    assert schema.name == "gemini"
    assert schema.tables
    queries = schema.as_queries(replication_strategy=SimpleReplicationStrategy(3))
    expected_queries = [
        CqlDto(
            "CREATE KEYSPACE IF NOT EXISTS gemini with "
            "replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"
        ),
        CqlDto(
            "CREATE TABLE IF NOT EXISTS gemini.table0"
            " (pk0 ascii, ck0 ascii, ck1 ascii, col0 ascii, col1 bigint,"
            " PRIMARY KEY (pk0, ck0, ck1));"
        ),
    ]
    assert queries == expected_queries
    queries = schema.as_sql()
    assert queries == [
        CqlDto(
            "CREATE TABLE IF NOT EXISTS 'gemini.table0'"
            " (id INTEGER PRIMARY KEY AUTOINCREMENT, d_time INTEGER , pk0 TEXT, ck0 TEXT, ck1 TEXT);"
        )
    ]


def test_schema_can_be_created_in_database(config):
    query_driver = RecordingQueryDriver()
    schema = generate_schema(config)
    schema.create(query_driver, SimpleReplicationStrategy(3))
    assert len(schema.as_queries(SimpleReplicationStrategy(3))) == len(
        query_driver.executed_queries
    )
    for ks_cql, executed_cql in zip(
        schema.as_queries(SimpleReplicationStrategy(3)), query_driver.executed_queries
    ):
        assert ks_cql == executed_cql


def test_can_create_multiple_tables(config):
    query_driver = RecordingQueryDriver()
    config.max_tables = 2
    schema = generate_schema(config)
    assert len(schema.tables) == 2
    schema.create(query_driver, SimpleReplicationStrategy(3))
    assert len(schema.as_queries(SimpleReplicationStrategy(3))) == len(
        query_driver.executed_queries
    )
    for ks_cql, executed_cql in zip(
        schema.as_queries(SimpleReplicationStrategy(3)), query_driver.executed_queries
    ):
        assert ks_cql == executed_cql
