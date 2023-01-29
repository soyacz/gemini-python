from gemini_python import QueryMode
from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import generate_schema


def test_load_generator_can_generate_insert_queries():
    schema = generate_schema()
    generator = LoadGenerator(schema=schema, mode=QueryMode.WRITE, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower().startswith("insert")
    assert isinstance(cql_dto.values, tuple)


def test_load_generator_can_generate_select_queries():
    schema = generate_schema()
    generator = LoadGenerator(schema=schema, mode=QueryMode.READ, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower().startswith("select")
    assert isinstance(cql_dto.values, tuple)
