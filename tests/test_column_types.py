from gemini_python.column_types import AsciiColumn, BigIntColumn, ALL_COLUMN_TYPES, Column


def test_ascii_column():
    col = AsciiColumn("col_ascii", size=50)
    assert len(col.generate_sequence_value()) == 50
    assert len(col.generate_random_value()) == 50


def test_bigint_column():
    col = BigIntColumn("col_bigint")
    assert col.generate_sequence_value() == 1
    assert col.generate_sequence_value() == 2
    assert isinstance(col.generate_random_value(), int)


def test_all_column_types_contain_all_column_types():
    """Just making sure that ALL_COLUMN_TYPES contains all Column subclasses"""
    assert set(ALL_COLUMN_TYPES) == set(Column.__subclasses__())
