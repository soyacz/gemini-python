from gemini_python import QueryMode
from gemini_python.load_generator import LoadGenerator
from gemini_python.schema import generate_schema


def test_load_generator_can_generate_insert_queries():
    schema = generate_schema(seed=1234)
    generator = LoadGenerator(schema=schema, mode=QueryMode.WRITE, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower().startswith("insert")
    assert isinstance(cql_dto.values, tuple)
    # verify seed is working
    assert cql_dto.values == (
        1,
        "BL0FerBqopIXEVWhdEEZnSxUh2yG6k6JfKLotC8joPlWACJE81gG4sdUnmbGfxxS4dX1vpQhwCSlMtQ56oww6Kz6NzUzY4nkzJ99",
    )


def test_load_generator_can_generate_select_queries():
    schema = generate_schema(seed=1234)
    generator = LoadGenerator(schema=schema, mode=QueryMode.READ, partitions=[[(1,), (2,)]])
    cql_dto = generator.get_query()
    assert cql_dto.statement.lower().startswith("select")
    assert isinstance(cql_dto.values, tuple)
    assert cql_dto.values == (1,)
