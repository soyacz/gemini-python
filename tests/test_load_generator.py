from gemini_python import QueryMode
from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import generate_schema


def test_load_generator_can_generate_insert_queries(simple_schema_config):
    schema = generate_schema(simple_schema_config)
    generator = LoadGenerator(schema=schema, mode=QueryMode.WRITE, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower().startswith("insert")
    assert isinstance(cql_dto.values, tuple)
    # verify seed is working
    assert cql_dto.values == (
        1,
        "9gYBr0jzBVRnepzTZUcVaRzlvpUSlNN9s2T9QJtXWXWWgC7eXXg6c2E1XCwmLdH1mpWU1yMcMTc2zfqbn8o3oIvpuWPAKrPUeN1N",
        "Otb9ahtJtTV8ryJD3jfZKOmqxLQbxGRoV2zl7eMizwYazhQKwTtDkacRuKWjmp5wP1vZHpWjLP1Jh6zFKjQhRxzs81cX0kXt9CC1",
    )


def test_load_generator_can_generate_select_queries(config):
    schema = generate_schema(config)
    generator = LoadGenerator(schema=schema, mode=QueryMode.READ, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower().startswith("select")
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (1,)
