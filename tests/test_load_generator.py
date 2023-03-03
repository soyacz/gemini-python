from gemini_python import QueryMode
from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import generate_schema


def test_can_generate_insert_queries(simple_schema_config, only_big_int_column_types):
    schema = generate_schema(simple_schema_config, **only_big_int_column_types)
    generator = LoadGenerator(schema=schema, mode=QueryMode.WRITE, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower() == "insert into gemini.table0 (pk0, ck0, col0) values (?,?,?)"
    assert isinstance(cql_dto.values, tuple)
    # verify seed is working
    assert cql_dto.values == (1, 97, 67)
    # verify we don't generate the same partitions
    cql_dto_2 = generator.get_query()
    assert cql_dto.values != cql_dto_2.values


def test_can_generate_select_queries(simple_schema_config):
    schema = generate_schema(simple_schema_config)
    generator = LoadGenerator(schema=schema, mode=QueryMode.READ, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower() == "select pk0, ck0, col0 from gemini.table0 where pk0=?"
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (1,)
    # verify we don't generate the same partitions
    cql_dto_2 = generator.get_query()
    assert cql_dto.values != cql_dto_2.values


def test_can_generate_insert_queries_multi_partition(
    simple_schema_config, only_big_int_column_types
):
    simple_schema_config.min_partition_keys = 2
    simple_schema_config.max_partition_keys = 2
    schema = generate_schema(simple_schema_config, **only_big_int_column_types)
    generator = LoadGenerator(
        schema=schema, mode=QueryMode.WRITE, partitions=[[(1, 2), (3, 4), (5, 6), (7, 8)]]
    )
    cql_dto = generator.get_query()
    assert (
        cql_dto.statement.lower()
        == "insert into gemini.table0 (pk0, pk1, ck0, col0) values (?,?,?,?)"
    )
    assert isinstance(cql_dto.values, tuple)
    # verify seed is working
    assert cql_dto.values == (1, 2, 97, 67)


def test_can_generate_select_queries_multi_pk(simple_schema_config):
    simple_schema_config.min_partition_keys = 2
    simple_schema_config.max_partition_keys = 2
    schema = generate_schema(simple_schema_config)
    generator = LoadGenerator(
        schema=schema, mode=QueryMode.READ, partitions=[[(1, 2), (3, 4), (5, 6), (7, 8)]]
    )
    cql_dto = generator.get_query()
    assert (
        cql_dto.statement.lower()
        == "select pk0, pk1, ck0, col0 from gemini.table0 where pk0=? and pk1=?"
    )
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (1, 2)


def test_can_generate_mixed_queries(simple_schema_config, only_big_int_column_types):
    schema = generate_schema(simple_schema_config, **only_big_int_column_types)
    generator = LoadGenerator(schema=schema, mode=QueryMode.MIXED, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower() == "insert into gemini.table0 (pk0, ck0, col0) values (?,?,?)"
    assert isinstance(cql_dto.values, tuple)
    # verify seed is working
    assert cql_dto.values == (1, 97, 67)
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower() == "select pk0, ck0, col0 from gemini.table0 where pk0=?"
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (1,)


def test_can_generate_queries_for_multiple_tables(simple_schema_config):
    simple_schema_config.max_tables = 2
    schema = generate_schema(simple_schema_config)

    assert len(schema.tables) == 2

    generator = LoadGenerator(
        schema=schema, mode=QueryMode.READ, partitions=[[(1,), (2,)], [(1,), (2,)]]
    )
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower() == "select pk0, ck0, col0 from gemini.table0 where pk0=?"
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (1,)

    # verify next query is "select"
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower() == "select pk0, ck0, col0 from gemini.table1 where pk0=?"
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (1,)
