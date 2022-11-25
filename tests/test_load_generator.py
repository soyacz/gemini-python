from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import generate_schema


def test_load_generator_can_generate_insert_queries():
    schema = generate_schema()
    generator = LoadGenerator(schema=schema, mode="write")
    statement, values = generator.get_query()
    assert statement.lower().startswith("insert")
    assert isinstance(values, tuple)


def test_load_generator_can_generate_select_queries():
    schema = generate_schema()
    generator = LoadGenerator(schema=schema, mode="read")
    statement, values = generator.get_query()
    assert statement.lower().startswith("select")
    assert isinstance(values, tuple)
