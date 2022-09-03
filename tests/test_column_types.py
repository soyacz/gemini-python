from gemini_python.column_types import AsciiColumn, BigIntColumn


def test_ascii_column():
    col = AsciiColumn("col_ascii", length=50)
    assert len(col.generate_sequence_value()) == 50
    assert len(col.generate_random_value()) == 50


def test_bigint_column():
    col = BigIntColumn("col_bigint")
    assert col.generate_sequence_value() == 1
    assert col.generate_sequence_value() == 2
    assert type(col.generate_random_value()) == int
