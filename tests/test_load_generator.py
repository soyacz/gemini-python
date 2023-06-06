from gemini_python import QueryMode, Operation
from gemini_python.history_store import HistoryStore
from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import generate_schema


def test_can_generate_insert_queries(simple_schema_config, only_big_int_column_types):
    schema = generate_schema(simple_schema_config, **only_big_int_column_types)
    history_store = HistoryStore(0, schema, drop_schema=True)
    generator = LoadGenerator(
        schema=schema, mode=QueryMode.WRITE, partitions=[[(1,), (2,)]], history_store=history_store
    )
    operation, cql_dto = generator.get_query()
    assert cql_dto.statement.lower() == "insert into gemini.table0 (pk0, ck0, col0) values (?,?,?)"
    assert isinstance(cql_dto.values, tuple)
    assert operation == Operation.WRITE
    # verify seed is working
    assert cql_dto.values == (1, 97, 67)
    # verify we don't generate the same partitions
    operation, cql_dto_2 = generator.get_query()
    assert cql_dto.values != cql_dto_2.values


def test_can_generate_select_queries(simple_schema_config):
    schema = generate_schema(simple_schema_config)
    print(schema)
    history_store = HistoryStore(0, schema, drop_schema=True)
    insert_generator = LoadGenerator(
        schema=schema,
        mode=QueryMode.WRITE,
        partitions=[[("1",), ("2",)]],
        history_store=history_store,
    )
    operation, cql_dto = insert_generator.get_query()
    history_store.insert(cql_dto)
    history_store.commit()
    generator = LoadGenerator(
        schema=schema,
        mode=QueryMode.READ,
        partitions=[[("1",), ("2",)]],
        history_store=history_store,
    )
    operation, cql_dto = generator.get_query()
    assert (
        cql_dto.statement.lower()
        == "select pk0, ck0, col0 from gemini.table0 where pk0=? and ck0=?"
    )
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (
        "1",
        "9gYBr0jzBVRnepzTZUcVaRzlvpUSlNN9s2T9QJtXWXWWgC7eXXg6c2E1XCwmLdH1mpWU1yMcMTc2zfqbn8o3oIvpuWPAKrPUeN1N",
    )
    assert operation == Operation.READ


def test_can_generate_insert_queries_multi_partition(
    simple_schema_config, only_big_int_column_types
):
    simple_schema_config.min_partition_keys = 2
    simple_schema_config.max_partition_keys = 2
    schema = generate_schema(simple_schema_config, **only_big_int_column_types)
    history_store = HistoryStore(0, schema, drop_schema=True)
    generator = LoadGenerator(
        schema=schema,
        mode=QueryMode.WRITE,
        partitions=[[(1, 2), (3, 4), (5, 6), (7, 8)]],
        history_store=history_store,
    )
    operation, cql_dto = generator.get_query()
    assert (
        cql_dto.statement.lower()
        == "insert into gemini.table0 (pk0, pk1, ck0, col0) values (?,?,?,?)"
    )
    assert isinstance(cql_dto.values, tuple)
    # verify seed is working
    assert cql_dto.values == (1, 2, 97, 67)
    assert operation == Operation.WRITE


def test_can_generate_select_queries_multi_pk(simple_schema_config):
    simple_schema_config.min_partition_keys = 2
    simple_schema_config.max_partition_keys = 2
    schema = generate_schema(simple_schema_config)
    history_store = HistoryStore(0, schema, drop_schema=True)
    insert_generator = LoadGenerator(
        schema=schema,
        mode=QueryMode.WRITE,
        partitions=[[(1, 2), (3, 4), (5, 6), (7, 8)]],
        history_store=history_store,
    )
    operation, cql_dto = insert_generator.get_query()
    history_store.insert(cql_dto)
    history_store.commit()
    generator = LoadGenerator(
        schema=schema,
        mode=QueryMode.READ,
        partitions=[[(1, 2), (3, 4), (5, 6), (7, 8)]],
        history_store=history_store,
    )
    operation, cql_dto = generator.get_query()
    assert (
        cql_dto.statement.lower()
        == "select pk0, pk1, ck0, col0 from gemini.table0 where pk0=? and pk1=? and ck0=?"
    )
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (
        "1",
        "2",
        "9gYBr0jzBVRnepzTZUcVaRzlvpUSlNN9s2T9QJtXWXWWgC7eXXg6c2E1XCwmLdH1mpWU1yMcMTc2zfqbn8o3oIvpuWPAKrPUeN1N",
    )
    assert operation == Operation.READ


def test_can_generate_mixed_queries(simple_schema_config, only_big_int_column_types):
    schema = generate_schema(simple_schema_config, **only_big_int_column_types)
    history_store = HistoryStore(0, schema, drop_schema=True)
    generator = LoadGenerator(
        schema=schema, mode=QueryMode.MIXED, partitions=[[(1,), (2,)]], history_store=history_store
    )
    operation, cql_dto = generator.get_query()
    history_store.insert(cql_dto)
    history_store.commit()
    assert cql_dto.statement.lower() == "insert into gemini.table0 (pk0, ck0, col0) values (?,?,?)"
    assert isinstance(cql_dto.values, tuple)
    assert operation == Operation.WRITE
    # verify seed is working
    assert cql_dto.values == (1, 97, 67)
    operation, cql_dto = generator.get_query()
    assert (
        cql_dto.statement.lower()
        == "select pk0, ck0, col0 from gemini.table0 where pk0=? and ck0=?"
    )
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (1, 97)
    assert operation == Operation.READ


def test_can_generate_queries_for_multiple_tables(simple_schema_config):
    simple_schema_config.max_tables = 2
    schema = generate_schema(simple_schema_config)
    assert len(schema.tables) == 2

    history_store = HistoryStore(0, schema, drop_schema=True)
    insert_generator = LoadGenerator(
        schema=schema,
        mode=QueryMode.WRITE,
        partitions=[[(1,), (2,)], [(1,), (2,)]],
        history_store=history_store,
    )
    operation, cql_dto = insert_generator.get_query()
    history_store.insert(cql_dto)
    history_store.commit()
    generator = LoadGenerator(
        schema=schema,
        mode=QueryMode.READ,
        partitions=[[(1,), (2,)], [(1,), (2,)]],
        history_store=history_store,
    )
    operation, cql_dto = generator.get_query()
    assert (
        cql_dto.statement.lower()
        == "select pk0, ck0, col0 from gemini.table0 where pk0=? and ck0=?"
    )
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (
        "1",
        "9gYBr0jzBVRnepzTZUcVaRzlvpUSlNN9s2T9QJtXWXWWgC7eXXg6c2E1XCwmLdH1mpWU1yMcMTc2zfqbn8o3oIvpuWPAKrPUeN1N",
    )
    assert operation == Operation.READ

    # verify next query is "select"
    operation, cql_dto = generator.get_query()
    assert (
        cql_dto.statement.lower()
        == "select pk0, ck0, col0 from gemini.table1 where pk0=? and ck0=?"
    )
    assert isinstance(cql_dto.values, tuple)
    assert operation == Operation.READ
